//! Offline replay of recorded node streams through `OrderBookState`.
//!
//! The node's raw book diffs alone define its book (New / Update / Remove per
//! oid). The server also needs the order-status stream for each order's
//! details, and the two streams reach it through independent file watchers,
//! so their relative arrival order is arbitrary. This harness feeds one
//! recorded slice through the real apply path under several arrival orders
//! and compares the resulting book with the one the diffs define: the set of
//! resting orders, and each order's side, price and size (not queue order).
//!
//! Input: plain newline-delimited JSON, the lines of the hourly
//! `node_raw_book_diffs_streaming` / `node_order_statuses_streaming` files
//! for the same block range.
//!
//! ```bash
//! OBS_REPLAY_DIFFS=/path/diffs.jsonl OBS_REPLAY_STATUSES=/path/statuses.jsonl \
//!     cargo test --release -p server replay_recorded_streams -- --ignored --nocapture
//! ```
use super::*;
use crate::order_book::{Px, Side, Sz};
use std::{
    fs::File,
    io::{BufRead, BufReader},
};

#[derive(Clone)]
enum Ev {
    Diff(Batch<NodeDataOrderDiff>),
    Status(Batch<NodeDataOrderStatus>),
}

/// (block, unix ms the node wrote the line, event)
type Rec = (u64, i64, Ev);
/// What the diff stream says about one resting order.
type Resting = HashMap<Oid, (Side, Px, Sz)>;

/// The diff fields the server's own type does not keep.
#[derive(serde::Deserialize)]
struct RawDiff {
    oid: u64,
    side: Side,
    px: String,
}
#[derive(serde::Deserialize)]
struct RawLine {
    events: Vec<RawDiff>,
}

fn lines(path: &str) -> impl Iterator<Item = String> {
    let file = File::open(path).unwrap_or_else(|e| panic!("open {path}: {e}"));
    BufReader::with_capacity(1 << 20, file).lines().map(|l| l.unwrap()).filter(|l| !l.is_empty())
}

fn load_statuses(path: &str) -> Vec<Rec> {
    lines(path)
        .map(|line| {
            let b: Batch<NodeDataOrderStatus> = sonic_rs::from_str(&line).unwrap();
            (b.block_number(), b.local_time_ms(), Ev::Status(b))
        })
        .collect()
}

/// The diff records, and the book they define: `book` (the orders resting at
/// block `above`; empty for a bare slice) with every diff of a later block
/// applied. Spot is skipped like the server does (`ignore_spot`). Also returns
/// the diffs that do not fit the book they are applied to: the oids of a New
/// for an order already resting, of an Update, and of a Remove for one that is
/// not (a Remove right after an Update to zero is not counted).
fn load_diffs(path: &str, mut book: Resting, above: u64) -> (Vec<Rec>, Resting, [Vec<Oid>; 3]) {
    let mut recs = Vec::new();
    let mut misfits = [Vec::new(), Vec::new(), Vec::new()];
    let mut zeroed = HashSet::new();
    for line in lines(path) {
        let b: Batch<NodeDataOrderDiff> = sonic_rs::from_str(&line).unwrap();
        if b.block_number() <= above {
            continue;
        }
        let raw: RawLine = sonic_rs::from_str(&line).unwrap();
        for (diff, raw) in b.events_ref().iter().zip(&raw.events) {
            if diff.coin().is_spot() {
                continue;
            }
            let oid = Oid::new(raw.oid);
            match diff.diff().try_into().unwrap() {
                InnerOrderDiff::New { sz, .. } => {
                    let order = (raw.side, Px::parse_from_str(&raw.px).unwrap(), sz);
                    if book.insert(oid.clone(), order).is_some() {
                        misfits[0].push(oid);
                    }
                }
                InnerOrderDiff::Update { new_sz, .. } if new_sz.value() > 0 => match book.get_mut(&oid) {
                    Some(order) => order.2 = new_sz,
                    None => misfits[1].push(oid),
                },
                InnerOrderDiff::Update { .. } => {
                    if book.remove(&oid).is_none() {
                        misfits[1].push(oid.clone());
                    }
                    zeroed.insert(oid);
                }
                InnerOrderDiff::Remove => {
                    if book.remove(&oid).is_none() && !zeroed.contains(&oid) {
                        misfits[2].push(oid);
                    }
                }
            }
        }
        recs.push((b.block_number(), b.local_time_ms(), Ev::Diff(b)));
    }
    (recs, book, misfits)
}

fn resting(state: &OrderBookState) -> Resting {
    let mut out = Resting::new();
    for book in state.order_book.as_ref().values() {
        for side in book.to_snapshot_in_band(PxBand::default()).as_ref() {
            for order in side {
                out.insert(order.oid(), (order.side(), order.limit_px(), order.sz()));
            }
        }
    }
    out
}

fn crossed_books(state: &OrderBookState) -> usize {
    let crossed = |book: &&crate::order_book::OrderBook<InnerL4Order>| {
        matches!(book.get_bbo(), (Some((bid, _, _)), Some((ask, _, _))) if bid >= ask)
    };
    state.order_book.as_ref().values().filter(crossed).count()
}

fn feed(state: &mut OrderBookState, events: impl IntoIterator<Item = Ev>) {
    for ev in events {
        match ev {
            Ev::Diff(b) => drop(state.apply_order_diffs_hft(b).unwrap()),
            Ev::Status(b) => drop(state.apply_order_statuses_hft(b).unwrap()),
        }
    }
}

/// Merge the two streams: each record gets a (slot, rank) key, the merge is
/// stable, so the order inside each stream is always preserved.
fn interleave(diffs: &[Rec], statuses: &[Rec], key: &dyn Fn(&Rec) -> (i64, u8)) -> Vec<Ev> {
    let mut all: Vec<((i64, u8), &Rec)> = diffs.iter().chain(statuses.iter()).map(|r| (key(r), r)).collect();
    all.sort_by_key(|(k, _)| *k);
    all.into_iter().map(|(_, r)| r.2.clone()).collect()
}

/// True when the server's book is the node's.
fn compare(state: &OrderBookState, want: &Resting, label: &str) -> bool {
    let got = resting(state);
    let phantoms = got.keys().filter(|oid| !want.contains_key(oid)).count();
    let holes = want.keys().filter(|oid| !got.contains_key(oid)).count();
    let differ = |f: fn(&(Side, Px, Sz), &(Side, Px, Sz)) -> bool| {
        got.iter().filter(|(oid, g)| want.get(oid).is_some_and(|w| f(g, w))).count()
    };
    let sizes = differ(|g, w| g.2 != w.2);
    let places = differ(|g, w| (g.0, g.1) != (w.0, w.1));
    println!(
        "{label}: node {} orders, server {} | phantoms {phantoms} holes {holes} sizes {sizes} side/px {places} | crossed books {}",
        want.len(),
        got.len(),
        crossed_books(state),
    );
    phantoms + holes + sizes + places == 0
}

fn perps_state() -> OrderBookState {
    OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), Vec::new(), 0, 0, true, true, true)
}

#[test]
#[ignore = "needs recorded node files: OBS_REPLAY_DIFFS / OBS_REPLAY_STATUSES"]
fn replay_recorded_streams_any_arrival_order_gives_the_nodes_book() {
    let (Ok(diffs_path), Ok(statuses_path)) = (std::env::var("OBS_REPLAY_DIFFS"), std::env::var("OBS_REPLAY_STATUSES"))
    else {
        println!("OBS_REPLAY_DIFFS / OBS_REPLAY_STATUSES not set; nothing to replay");
        return;
    };
    let (diffs, want, _) = load_diffs(&diffs_path, Resting::new(), 0);
    let statuses = load_statuses(&statuses_path);
    let (first, last) = (diffs.first().unwrap().0, diffs.last().unwrap().0);
    println!("{} diff lines, {} status lines, blocks {first}..={last}", diffs.len(), statuses.len());

    // Arrival orders. "chunk N ms": each watcher hands over what the node wrote
    // in the same N ms as one read; "diffs first" = the diff watcher's read first.
    type Key = Box<dyn Fn(&Rec) -> (i64, u8)>;
    let is_diff = |r: &Rec| matches!(r.2, Ev::Diff(_));
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
        orders.push((format!("chunk {ms} ms, diffs first"), Box::new(move |r| (r.1 / ms, u8::from(!is_diff(r))))));
        orders.push((format!("chunk {ms} ms, statuses first"), Box::new(move |r| (r.1 / ms, u8::from(is_diff(r))))));
    }

    let mut failed = Vec::new();
    for (label, key) in &orders {
        let mut state = perps_state();
        feed(&mut state, interleave(&diffs, &statuses, key));
        if !compare(&state, &want, label) {
            failed.push(label.clone());
        }
    }

    // A re-sync replays cached events from a height that is only a lower bound
    // of the snapshot's content, so a stretch of blocks is applied twice: build
    // the book up to a block D, then replay from an earlier block C to the end,
    // under each arrival order.
    let d = first + (last - first) / 2;
    let upto = |recs: &[Rec], h: u64| recs.iter().filter(|r| r.0 <= h).cloned().collect::<Vec<Rec>>();
    let from = |recs: &[Rec], h: u64| recs.iter().filter(|r| r.0 > h).cloned().collect::<Vec<Rec>>();
    for back in [20_u64, 200] {
        let c = d.saturating_sub(back).max(first);
        for (label, key) in &orders {
            let mut state = perps_state();
            feed(&mut state, interleave(&upto(&diffs, d), &upto(&statuses, d), &orders[0].1));
            // the install starts with empty pairing caches
            state.pending_order_statuses.clear();
            state.pending_new_diffs.clear();
            feed(&mut state, interleave(&from(&diffs, c), &from(&statuses, c), key));
            let label = format!("re-sync overlap {back} blocks, then {label}");
            if !compare(&state, &want, &label) {
                failed.push(label);
            }
        }
    }
    assert!(failed.is_empty(), "the server's book differs from the node's for: {failed:?}");
}

/// A snapshot written by the node's info server (`--snapshot-mode info`:
/// `[height, markets]`) loads with its own height into an uncrossed book.
/// With stream files that start before that height (OBS_REPLAY_DIFFS /
/// OBS_REPLAY_STATUSES) it also checks the handoff: every later diff fits the
/// snapshot's book (so the height is the right cutoff), and snapshot + stream
/// through the server gives the book that snapshot + diffs define.
///
/// ```bash
/// OBS_SNAPSHOT_FILE=/path/snapshot.json [OBS_REPLAY_DIFFS=.. OBS_REPLAY_STATUSES=..] \
///     cargo test --release -p server recorded_info_snapshot -- --ignored --nocapture
/// ```
#[tokio::test]
#[ignore = "needs a node info-server snapshot file: OBS_SNAPSHOT_FILE"]
async fn recorded_info_snapshot_loads_and_hands_over_to_the_stream() {
    use crate::order_book::multi_book::load_snapshots_with_height;
    let Ok(path) = std::env::var("OBS_SNAPSHOT_FILE") else {
        println!("OBS_SNAPSHOT_FILE not set; nothing to load");
        return;
    };
    let load = || async {
        let (height, snapshots, untriggered) = load_snapshots_with_height::<
            InnerL4Order,
            (alloy::primitives::Address, crate::types::L4Order),
        >(std::path::Path::new(&path))
        .await
        .unwrap();
        (height, OrderBookState::from_snapshot(snapshots, untriggered, height, 0, true, true, true))
    };
    let (height, state) = load().await;
    let perps = state.order_book.as_ref().keys().filter(|coin| coin.is_perp()).count();
    println!(
        "snapshot at height {height}: {} books ({perps} perps), {} resting orders, {} untriggered, {} crossed books",
        state.coin_count(),
        state.order_count(),
        state.untriggered_count(),
        crossed_books(&state)
    );
    assert!(height > 0 && state.order_count() > 0 && perps > 0);
    assert_eq!(crossed_books(&state), 0);

    let (Ok(diffs_path), Ok(statuses_path)) = (std::env::var("OBS_REPLAY_DIFFS"), std::env::var("OBS_REPLAY_STATUSES"))
    else {
        return;
    };
    let (diffs, want, misfits) = load_diffs(&diffs_path, resting(&state), height);
    let statuses: Vec<Rec> = load_statuses(&statuses_path).into_iter().filter(|r| r.0 > height).collect();
    // Trigger orders never rest, but canceling one still emits a Remove diff:
    // the snapshot's untriggered orders and the trigger statuses of the slice.
    let mut triggers: HashSet<Oid> =
        state.untriggered_orders.values().flat_map(|orders| orders.keys().cloned()).collect();
    for (_, _, ev) in &statuses {
        let Ev::Status(batch) = ev else { continue };
        triggers.extend(batch.events_ref().iter().filter(|s| s.order.is_trigger).map(|s| Oid::new(s.order.oid)));
    }
    let stray_removes: Vec<&Oid> = misfits[2].iter().filter(|oid| !triggers.contains(oid)).collect();
    println!(
        "{} diff lines above the snapshot, blocks {}..={}; not fitting its book: {} new, {} update, {} remove \
         (+ {} removes of trigger orders) {:?}",
        diffs.len(),
        diffs.first().unwrap().0,
        diffs.last().unwrap().0,
        misfits[0].len(),
        misfits[1].len(),
        stray_removes.len(),
        misfits[2].len() - stray_removes.len(),
        stray_removes.iter().take(5).collect::<Vec<_>>()
    );
    assert!(
        misfits[0].is_empty() && misfits[1].is_empty() && stray_removes.is_empty(),
        "the snapshot's height is not the stream cutoff"
    );
    let is_diff = |r: &Rec| matches!(r.2, Ev::Diff(_));
    let block = |r: &Rec| i64::try_from(r.0).unwrap();
    let mut ok = true;
    for (label, diffs_first) in [("statuses first, per block", false), ("diffs first, per block", true)] {
        let (_, mut state) = load().await;
        feed(&mut state, interleave(&diffs, &statuses, &|r| (block(r), u8::from(is_diff(r) != diffs_first))));
        ok &= compare(&state, &want, label);
    }
    assert!(ok, "snapshot + stream differs from snapshot + diffs");
}
