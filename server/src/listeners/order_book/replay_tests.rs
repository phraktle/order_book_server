//! Offline replay of recorded node streams through `OrderBookState`.
//!
//! The node's raw book diffs alone define its book (New / Update / Remove per
//! oid). The server additionally needs the order-status stream to learn each
//! order's details, and the two streams reach it through independent file
//! watchers, so their relative arrival order is arbitrary. This harness feeds
//! one recorded slice through the real apply path under several arrival
//! orders and checks that the resulting book is the one the diffs define -
//! whatever the interleaving.
//!
//! Run against real node files (plain newline-delimited JSON, the lines of
//! `node_raw_book_diffs_streaming` / `node_order_statuses_streaming` hourly
//! files for the same block range):
//!
//! ```bash
//! OBS_REPLAY_DIFFS=/path/diffs.jsonl OBS_REPLAY_STATUSES=/path/statuses.jsonl \
//!     cargo test --release -p server replay_recorded_streams -- --ignored --nocapture
//! ```
use super::*;
use crate::order_book::Sz;
use std::io::{BufRead, BufReader};

#[derive(Clone)]
enum Ev {
    Diff(Batch<NodeDataOrderDiff>),
    Status(Batch<NodeDataOrderStatus>),
}

/// (block, unix ms the node wrote the line, event)
type Rec = (u64, i64, Ev);

fn load(path: &str, statuses: bool) -> Vec<Rec> {
    let file = std::fs::File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    let mut out = Vec::new();
    for line in BufReader::with_capacity(1 << 20, file).lines() {
        let line = line.unwrap();
        if line.is_empty() {
            continue;
        }
        if statuses {
            let b: Batch<NodeDataOrderStatus> = sonic_rs::from_str(&line).unwrap();
            out.push((b.block_number(), b.local_time_ms(), Ev::Status(b)));
        } else {
            let b: Batch<NodeDataOrderDiff> = sonic_rs::from_str(&line).unwrap();
            out.push((b.block_number(), b.local_time_ms(), Ev::Diff(b)));
        }
    }
    out
}

/// The book the diff stream alone defines, for orders created inside the
/// slice: oid -> size. Spot is skipped like the server does (`ignore_spot`).
fn truth(diffs: &[Rec]) -> HashMap<Oid, Sz> {
    let mut book = HashMap::new();
    for (_, _, ev) in diffs {
        let Ev::Diff(batch) = ev else { continue };
        for diff in batch.events_ref() {
            if diff.coin().is_spot() {
                continue;
            }
            let oid = diff.oid();
            match diff.diff().try_into().unwrap() {
                InnerOrderDiff::New { sz, .. } => {
                    book.insert(oid, sz);
                }
                InnerOrderDiff::Update { new_sz, .. } => {
                    if new_sz.value() == 0 {
                        book.remove(&oid);
                    } else if let Some(sz) = book.get_mut(&oid) {
                        *sz = new_sz;
                    }
                }
                InnerOrderDiff::Remove => {
                    book.remove(&oid);
                }
            }
        }
    }
    book
}

fn resting(state: &OrderBookState) -> HashMap<Oid, Sz> {
    let mut out = HashMap::new();
    for book in state.order_book.as_ref().values() {
        let snapshot = book.to_snapshot();
        for side in snapshot.as_ref() {
            for order in side {
                out.insert(order.oid(), order.sz());
            }
        }
    }
    out
}

fn crossed_books(state: &OrderBookState) -> usize {
    state
        .order_book
        .as_ref()
        .values()
        .filter(|book| matches!(book.get_bbo(), (Some((bid, _, _)), Some((ask, _, _))) if bid >= ask))
        .count()
}

fn feed(state: &mut OrderBookState, events: impl IntoIterator<Item = Ev>) {
    for ev in events {
        match ev {
            Ev::Diff(b) => drop(state.apply_order_diffs_hft(b).unwrap()),
            Ev::Status(b) => drop(state.apply_order_statuses_hft(b).unwrap()),
        }
    }
}

/// Merge the two streams: each record gets a (slot, rank) key from `key`, the
/// merge is stable, so the order inside each stream is always preserved.
fn interleave(diffs: &[Rec], statuses: &[Rec], key: impl Fn(&Rec) -> (i64, u8)) -> Vec<Ev> {
    let mut all: Vec<((i64, u8), &Rec)> = diffs.iter().chain(statuses.iter()).map(|r| (key(r), r)).collect();
    all.sort_by_key(|(k, _)| *k);
    all.into_iter().map(|(_, r)| r.2.clone()).collect()
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Mismatch {
    /// resting in the server's book, not in the node's
    phantoms: usize,
    /// resting in the node's book, not in the server's
    holes: usize,
    /// resting in both with different sizes
    sizes: usize,
}

fn compare(state: &OrderBookState, want: &HashMap<Oid, Sz>, label: &str) -> Mismatch {
    let got = resting(state);
    let mut m = Mismatch::default();
    let mut shown = 0;
    for (oid, sz) in &got {
        match want.get(oid) {
            None => {
                m.phantoms += 1;
                if shown < 5 {
                    shown += 1;
                    println!("    phantom {oid:?} sz {}", sz.to_str());
                }
            }
            Some(w) if w != sz => m.sizes += 1,
            Some(_) => {}
        }
    }
    m.holes = want.keys().filter(|oid| !got.contains_key(oid)).count();
    println!(
        "{label}: node {} orders, server {} | phantoms {} holes {} sizes {} | crossed books {} | pending diffs {} statuses {}",
        want.len(),
        got.len(),
        m.phantoms,
        m.holes,
        m.sizes,
        crossed_books(state),
        state.pending_new_diffs_count(),
        state.pending_order_statuses_count(),
    );
    m
}

fn empty_state() -> OrderBookState {
    OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), 0, 0, true, true)
}

#[test]
#[ignore = "needs recorded node files: OBS_REPLAY_DIFFS / OBS_REPLAY_STATUSES"]
fn replay_recorded_streams_any_arrival_order_gives_the_nodes_book() {
    let (Ok(diffs_path), Ok(statuses_path)) = (std::env::var("OBS_REPLAY_DIFFS"), std::env::var("OBS_REPLAY_STATUSES"))
    else {
        println!("OBS_REPLAY_DIFFS / OBS_REPLAY_STATUSES not set; nothing to replay");
        return;
    };
    let diffs = load(&diffs_path, false);
    let statuses = load(&statuses_path, true);
    let first = diffs.first().unwrap().0;
    let last = diffs.last().unwrap().0;
    println!("{} diff lines, {} status lines, blocks {first}..={last}", diffs.len(), statuses.len());
    let want = truth(&diffs);
    let is_diff = |r: &Rec| matches!(r.2, Ev::Diff(_));
    let mut failed = Vec::new();

    // Arrival orders. "chunk N ms": each watcher hands over what the node wrote
    // in the same N ms as one read, the diff watcher's read first.
    type Key = Box<dyn Fn(&Rec) -> (i64, u8)>;
    let block = |r: &Rec| i64::try_from(r.0).unwrap();
    let mut orders: Vec<(String, Key)> = vec![
        ("statuses first, per block".into(), Box::new(move |r| (block(r), u8::from(is_diff(r))))),
        ("diffs first, per block".into(), Box::new(move |r| (block(r), u8::from(!is_diff(r))))),
    ];
    for lag in [1_i64, 5, 50] {
        orders.push((
            format!("statuses {lag} blocks late"),
            Box::new(move |r| if is_diff(r) { (block(r), 0) } else { (block(r) + lag, 1) }),
        ));
    }
    for ms in [5_i64, 20, 100] {
        orders.push((
            format!("chunk {ms} ms, diffs first"),
            Box::new(move |r| (r.1 / ms, u8::from(!is_diff(r)))),
        ));
        orders.push((
            format!("chunk {ms} ms, statuses first"),
            Box::new(move |r| (r.1 / ms, u8::from(is_diff(r)))),
        ));
    }
    for (label, key) in &orders {
        let mut state = empty_state();
        feed(&mut state, interleave(&diffs, &statuses, key));
        if compare(&state, &want, label) != Mismatch::default() {
            failed.push(label.clone());
        }
    }

    // A re-sync replays cached events from a height that is only a lower bound
    // of the snapshot's content, so a stretch of blocks is applied twice. Build
    // the book up to a block D, then replay from an earlier block C to the end.
    let d = first + (last - first) / 2;
    for back in [20_u64, 200] {
        let c = d.saturating_sub(back).max(first);
        let head: Vec<Rec> = diffs.iter().chain(statuses.iter()).filter(|r| r.0 <= d).cloned().collect();
        let (hd, hs): (Vec<Rec>, Vec<Rec>) = head.into_iter().partition(is_diff);
        let tail: Vec<Rec> = diffs.iter().chain(statuses.iter()).filter(|r| r.0 > c).cloned().collect();
        let (td, ts): (Vec<Rec>, Vec<Rec>) = tail.into_iter().partition(is_diff);
        for (name, diffs_first) in [("statuses first", false), ("diffs first", true)] {
            let mut state = empty_state();
            feed(&mut state, interleave(&hd, &hs, |r| (block(r), u8::from(is_diff(r)))));
            // the install starts with empty pairing caches
            state.pending_order_statuses.clear();
            state.pending_new_diffs.clear();
            feed(&mut state, interleave(&td, &ts, |r| (block(r), u8::from(is_diff(r) != diffs_first))));
            let label = format!("re-sync replay overlapping {back} blocks, {name}");
            if compare(&state, &want, &label) != Mismatch::default() {
                failed.push(label);
            }
        }
    }
    assert!(failed.is_empty(), "the server's book differs from the node's for: {failed:?}");
}
