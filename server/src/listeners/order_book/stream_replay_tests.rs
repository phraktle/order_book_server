//! Offline replay of a captured node slice through the watcher channels and
//! the streaming book (diffs drive the book, statuses attach metadata).
//!
//! `OBS_REPLAY_SLICE=<dir>` holds `statuses.jsonl.zst`, `diffs.jsonl.zst` and
//! `fills.jsonl.zst`: the node's `node_order_statuses_streaming`,
//! `node_raw_book_diffs_streaming` and `node_fills_streaming` lines for one
//! block range, verbatim (zstd; read through `zstd -dc`). Run:
//!
//!   OBS_REPLAY_SLICE=/path cargo test --release -p server stream_replay -- --ignored --nocapture --test-threads=1
//!
//! One producer thread per stream feeds a bounded channel with
//! `blocking_send`, like the watchers. Two feeds:
//!   backlog - producers read flat out, the consumer is throttled below their
//!             rate, so the channels stay full (the heavy-block backlog that
//!             made new levels late in production);
//!   paced   - each line is sent at its own node write time (`local_time`,
//!             so the node's real statuses-vs-diffs skew is kept), optionally
//!             with an extra statuses delay, a statuses stall (rotation gap /
//!             stalled stream) or a node restart (both streams down, then the
//!             last blocks written again).
//! Each feed runs through the old single shared channel and through the
//! per-source channels + `MergedEvents`. For every resting order added to the
//! book it records how many blocks the book had already moved past the
//! order's own block when it was added (the bbo/l2 stamp is the book's
//! time/height at that moment, so >0 = a late new level), the same for every
//! update/remove diff, and how long each diff line waited between being
//! written and being applied.
use super::parallel::{FileEvent, MergedEvents};
use super::state::OrderBookState;
use super::utils::EventBatch;
use crate::order_book::multi_book::Snapshots;
use crate::types::OrderDiff;
use crate::types::node_data::EventSource;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::io::BufRead;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

const CAP: usize = 10_000; // = the production per-source / old shared capacity
const BATCH: usize = 64; // = EVENT_RECV_BATCH

fn height_of(line: &str) -> u64 {
    let Some(i) = line.find("\"block_number\":") else { return 0 };
    let rest = &line[i + 15..];
    let end = rest.find(|c: char| !c.is_ascii_digit()).unwrap_or(rest.len());
    rest[..end].parse().unwrap_or(0)
}

/// The node's write time of the line, in ms since the epoch.
fn local_ms(line: &str) -> i64 {
    let i = line.find("\"local_time\":\"").expect("local_time") + 14;
    let s = &line[i..i + line[i..].find('"').expect("local_time end")];
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S%.f")
        .expect("local_time format")
        .and_utc()
        .timestamp_millis()
}

fn lines(path: &Path) -> impl Iterator<Item = String> {
    let mut child = std::process::Command::new("zstd")
        .arg("-dcq")
        .arg(path)
        .stdout(std::process::Stdio::piped())
        .spawn()
        .unwrap_or_else(|e| panic!("zstd {}: {e}", path.display()));
    let out = child.stdout.take().expect("stdout");
    std::io::BufReader::with_capacity(1 << 22, out).lines().map_while(Result::ok).filter(|l| !l.is_empty())
}

#[derive(Clone, Copy, Debug)]
enum Feed {
    Backlog { consumer_bytes_per_ms: u64 },
    Paced,
}

#[derive(Clone, Debug)]
struct Scenario {
    name: &'static str,
    feed: Feed,
    /// Only blocks in this range (inclusive).
    range: (u64, u64),
    /// Statuses written this much later than at the node (legit node lag).
    status_delay: Duration,
    /// Diffs written this much later than at the node (statuses far ahead).
    diff_delay: Duration,
    /// From this block on, the statuses stream is silent for this long.
    status_stall: Option<(u64, Duration)>,
    /// At this block both streams go down for `down`, then the last `rewind`
    /// blocks are written again (node restart replaying), then live again.
    restart: Option<(u64, u64, Duration)>,
}

#[derive(Default, Debug)]
struct Stats {
    lines: [usize; 3],
    adds: usize,
    late_adds: BTreeMap<u64, usize>,
    mods: usize,
    late_mods: BTreeMap<u64, usize>,
    /// Diff lines: write-to-apply wait, ms -> count (paced feeds only).
    diff_wait_ms: BTreeMap<u64, usize>,
    /// Final book: oid -> (side, px, sz, queue index), and oid -> L4 metadata.
    book: HashMap<u64, (u8, u64, u64, usize)>,
    meta: HashMap<u64, String>,
    awaiting: usize,
}

impl Stats {
    fn late_add_blocks(&self) -> usize {
        self.late_adds.values().sum()
    }
    fn late_mod_blocks(&self) -> usize {
        self.late_mods.values().sum()
    }
    fn max_wait_ms(&self) -> u64 {
        self.diff_wait_ms.keys().next_back().copied().unwrap_or(0)
    }
    fn wait_pct(&self, p: f64) -> u64 {
        let n: usize = self.diff_wait_ms.values().sum();
        let mut acc = 0;
        for (ms, c) in &self.diff_wait_ms {
            acc += c;
            if acc as f64 >= p * n as f64 {
                return *ms;
            }
        }
        0
    }
    fn summary(&self) -> String {
        format!(
            "lines s/d/f={:?} adds={} late_adds={} {:?} mods={} late_mods={} {:?} diff_wait_ms p50={} p99={} max={} resting={} awaiting_status={}",
            self.lines,
            self.adds,
            self.late_add_blocks(),
            self.late_adds,
            self.mods,
            self.late_mod_blocks(),
            self.late_mods,
            self.wait_pct(0.5),
            self.wait_pct(0.99),
            self.max_wait_ms(),
            self.book.len(),
            self.awaiting,
        )
    }
}

/// When each diff block's line was handed to its channel (for the wait).
type Sent = Arc<Mutex<HashMap<u64, Instant>>>;

fn producer(
    stream: usize,
    path: PathBuf,
    sc: Scenario,
    t0: Instant,
    lt0: i64,
    tx: tokio::sync::mpsc::Sender<FileEvent>,
    sent: Sent,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        let wrap = |l: String| match stream {
            0 => FileEvent::OrderStatus(l),
            1 => FileEvent::OrderDiff(l),
            _ => FileEvent::Fill(l),
        };
        let paced = matches!(sc.feed, Feed::Paced);
        let mut shift = Duration::ZERO; // added downtime (stall / restart)
        let mut stalled = false;
        let mut restarted = false;
        let mut recent: VecDeque<String> = VecDeque::new();
        let mark = |h: u64| {
            if stream == 1 {
                sent.lock().unwrap().insert(h, Instant::now());
            }
        };
        for l in lines(&path) {
            let h = height_of(&l);
            if h < sc.range.0 {
                continue;
            }
            if h > sc.range.1 {
                break;
            }
            if stream == 0
                && let Some((at, d)) = sc.status_stall
                && h >= at
                && !stalled
            {
                stalled = true;
                shift += d;
            }
            if let Some((at, rewind, down)) = sc.restart {
                if h >= at && !restarted {
                    restarted = true;
                    shift += down;
                    // the restarted node rewrites its last blocks fast, then goes live
                    std::thread::sleep(down);
                    for old in recent.drain(..) {
                        mark(height_of(&old));
                        if tx.blocking_send(wrap(old)).is_err() {
                            return;
                        }
                    }
                } else if !restarted {
                    recent.push_back(l.clone());
                    while recent.front().is_some_and(|o| height_of(o) + rewind < h) {
                        recent.pop_front();
                    }
                }
            }
            if paced {
                let mut delay = Duration::from_millis((local_ms(&l) - lt0).max(0) as u64) + shift;
                if stream == 0 {
                    delay += sc.status_delay;
                }
                if stream == 1 {
                    delay += sc.diff_delay;
                }
                let at = t0 + delay;
                let now = Instant::now();
                if at > now {
                    std::thread::sleep(at - now);
                }
            }
            mark(h);
            if tx.blocking_send(wrap(l)).is_err() {
                return;
            }
        }
    })
}

struct Consumer {
    state: OrderBookState,
    st: Stats,
    seen_diff: u64,
    last_cleanup: Instant,
}

impl Consumer {
    fn new() -> Self {
        Self {
            state: OrderBookState::from_snapshot(Snapshots::new(HashMap::new()), Vec::new(), 0, 0, true, true, false),
            st: Stats::default(),
            seen_diff: 0,
            last_cleanup: Instant::now(),
        }
    }

    /// Apply one event; returns its byte length (for the throttle).
    fn apply(&mut self, ev: FileEvent, sent: &Sent, paced: bool) -> u64 {
        let (line, source, idx) = match ev {
            FileEvent::OrderStatus(l) => (l, EventSource::OrderStatuses, 0),
            FileEvent::OrderDiff(l) => (l, EventSource::OrderDiffs, 1),
            FileEvent::Fill(l) => (l, EventSource::Fills, 2),
            _ => return 0,
        };
        self.st.lines[idx] += 1;
        let len = line.len() as u64;
        let Some((b, batch)) = super::parse_event_line(&line, source) else { return len };
        let state = &mut self.state;
        let st = &mut self.st;
        match batch {
            EventBatch::Orders(batch) => {
                drop(state.apply_order_statuses_hft(batch));
            }
            EventBatch::BookDiffs(batch) => {
                let kinds: Vec<bool> = batch
                    .events_ref()
                    .iter()
                    .filter(|d| !d.coin().is_spot())
                    .map(|d| matches!(d.diff(), OrderDiff::New { .. }))
                    .collect();
                drop(state.apply_order_diffs_hft(batch));
                if paced && let Some(t) = sent.lock().unwrap().remove(&b) {
                    *st.diff_wait_ms.entry(t.elapsed().as_millis() as u64).or_default() += 1;
                }
                // A block below one already applied is a restart replay: the
                // book never goes back in height, so only first-time blocks
                // are scored.
                if b >= self.seen_diff {
                    self.seen_diff = b;
                    // the stamp every change of this batch is published with
                    let late = state.height().saturating_sub(b);
                    for is_new in kinds {
                        let (n, l) = if is_new { (&mut st.adds, &mut st.late_adds) } else { (&mut st.mods, &mut st.late_mods) };
                        *n += 1;
                        if late > 0 {
                            *l.entry(late).or_default() += 1;
                        }
                    }
                }
            }
            EventBatch::Fills(_) => {}
        }
        if self.last_cleanup.elapsed() > Duration::from_secs(1) {
            // the listener's maintenance tick (bounds the pending caches)
            let _ = self.state.cleanup_stale_pending();
            self.last_cleanup = Instant::now();
        }
        len
    }
}

/// Wall-clock origin of a paced run: the node write time of the first diff
/// line in range.
fn first_local(dir: &Path, range: (u64, u64)) -> i64 {
    lines(&dir.join("diffs.jsonl.zst")).find(|l| height_of(l) >= range.0).map(|l| local_ms(&l)).expect("range empty")
}

async fn run(dir: &Path, sc: &Scenario, merged: bool) -> Stats {
    let paced = matches!(sc.feed, Feed::Paced);
    let lt0 = if paced { first_local(dir, sc.range) } else { 0 };
    let sent: Sent = Arc::new(Mutex::new(HashMap::new()));
    let files = ["statuses.jsonl.zst", "diffs.jsonl.zst", "fills.jsonl.zst"];
    // paced: give the readers time to skip to the range; backlog: let the channels fill
    let t0 = Instant::now() + Duration::from_secs(if paced { 3 } else { 0 });
    let mut c = Consumer::new();
    let mut buf = Vec::with_capacity(BATCH);
    let mut throttle = 0_u64;
    let bpm = match sc.feed {
        Feed::Backlog { consumer_bytes_per_ms } => consumer_bytes_per_ms,
        Feed::Paced => 0,
    };
    let mut hs = Vec::new();
    if merged {
        let mut rxs = Vec::new();
        for (i, f) in files.iter().enumerate() {
            let (tx, rx) = tokio::sync::mpsc::channel(CAP);
            hs.push(producer(i, dir.join(f), sc.clone(), t0, lt0, tx, sent.clone()));
            rxs.push(rx);
        }
        let Ok([srx, drx, frx]) = <[_; 3]>::try_from(rxs) else { unreachable!() };
        let mut m = MergedEvents::new([srx, drx, frx]);
        if !paced {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        while m.recv_many(&mut buf, BATCH).await > 0 {
            for ev in buf.drain(..) {
                throttle += c.apply(ev, &sent, paced);
            }
            if bpm > 0 && throttle >= bpm {
                tokio::time::sleep(Duration::from_millis(throttle / bpm)).await;
                throttle %= bpm;
            }
        }
    } else {
        let (tx, mut rx) = tokio::sync::mpsc::channel(CAP);
        for (i, f) in files.iter().enumerate() {
            hs.push(producer(i, dir.join(f), sc.clone(), t0, lt0, tx.clone(), sent.clone()));
        }
        drop(tx);
        if !paced {
            tokio::time::sleep(Duration::from_secs(2)).await;
        }
        while rx.recv_many(&mut buf, BATCH).await > 0 {
            for ev in buf.drain(..) {
                throttle += c.apply(ev, &sent, paced);
            }
            if bpm > 0 && throttle >= bpm {
                tokio::time::sleep(Duration::from_millis(throttle / bpm)).await;
                throttle %= bpm;
            }
        }
    }
    hs.into_iter().for_each(|h| h.join().unwrap());
    for coin in c.state.compute_universe() {
        let Some((_, _, snap)) = c.state.compute_snapshot_for_coin(&coin, crate::order_book::PxBand::default()) else {
            continue;
        };
        for (side, orders) in snap.as_ref().iter().enumerate() {
            for (q, o) in orders.iter().enumerate() {
                c.st.book.insert(o.oid, (side as u8, o.limit_px.value(), o.sz.value(), q));
                c.st.meta.insert(
                    o.oid,
                    format!(
                        "{:?}|{:?}|{}|{:?}|{}|{}|{}|{}|{}",
                        o.user, o.tif, o.order_type, o.cloid, o.trigger_condition, o.is_trigger, o.reduce_only, o.is_position_tpsl, o.timestamp
                    ),
                );
            }
        }
    }
    c.st.awaiting = c.state.awaiting_status_count();
    c.st
}

fn slice() -> PathBuf {
    PathBuf::from(std::env::var("OBS_REPLAY_SLICE").expect("set OBS_REPLAY_SLICE"))
}

fn env_u64(k: &str, d: u64) -> u64 {
    std::env::var(k).ok().and_then(|v| v.parse().ok()).unwrap_or(d)
}

/// The whole slice in heavy backlog: old shared channel vs the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs OBS_REPLAY_SLICE (a captured node slice)"]
async fn stream_replay_backlog() {
    let dir = slice();
    let sc = Scenario {
        name: "backlog",
        feed: Feed::Backlog { consumer_bytes_per_ms: env_u64("OBS_REPLAY_BPMS", 150_000) },
        range: (env_u64("OBS_REPLAY_FROM", 0), env_u64("OBS_REPLAY_TO", u64::MAX)),
        status_delay: Duration::ZERO,
        diff_delay: Duration::ZERO,
        status_stall: None,
        restart: None,
    };
    let mut first: Option<Stats> = None;
    for merged in [false, true] {
        let t = Instant::now();
        let s = run(&dir, &sc, merged).await;
        eprintln!("{} {} ({:.0?}): {}", sc.name, if merged { "MERGED" } else { "shared" }, t.elapsed(), s.summary());
        assert!(s.adds > 0 && s.mods > 0, "the slice must exercise adds and updates/removes");
        // the book follows the diffs alone: nothing late, whichever channel layout
        assert_eq!(s.late_add_blocks(), 0, "new levels added after their block");
        assert_eq!(s.late_mod_blocks(), 0, "updates/removes applied after their block");
        if let Some(f) = &first {
            same_book_and_meta(f, &s, "backlog shared vs merged");
        } else {
            first = Some(s);
        }
    }
}

/// Real node timing (about a minute, by default the start of the slice) plus
/// the edge cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs OBS_REPLAY_SLICE (a captured node slice)"]
async fn stream_replay_paced_edge_cases() {
    let dir = slice();
    let from = env_u64("OBS_PACED_FROM", 0).max(lines(&dir.join("diffs.jsonl.zst")).next().map_or(0, |l| height_of(&l)));
    let to = env_u64("OBS_PACED_TO", from + 850);
    let mid = from + (to - from) / 2;
    let base = Scenario {
        name: "paced",
        feed: Feed::Paced,
        range: (from, to),
        status_delay: Duration::ZERO,
        diff_delay: Duration::ZERO,
        status_stall: None,
        restart: None,
    };
    let scenarios = [
        base.clone(),
        Scenario { name: "statuses-lag-200ms", status_delay: Duration::from_millis(200), ..base.clone() },
        Scenario { name: "statuses-stall-3s", status_stall: Some((mid, Duration::from_secs(3))), ..base.clone() },
        Scenario { name: "statuses-lag-5s", status_delay: Duration::from_secs(5), ..base.clone() },
        Scenario { name: "statuses-stall-30s", status_stall: Some((mid, Duration::from_secs(30))), ..base.clone() },
        Scenario { name: "statuses-ahead-500ms", diff_delay: Duration::from_millis(500), ..base.clone() },
        Scenario { name: "node-restart", restart: Some((mid, 100, Duration::from_secs(3))), ..base.clone() },
    ];
    let mut reference: Option<Stats> = None;
    // Nothing waits across streams: a diff's write-to-apply time is the
    // consumer's own processing only, whatever the statuses do.
    let bound_ms = env_u64("OBS_WAIT_BOUND_MS", 50);
    let only = std::env::var("OBS_PACED_ONLY").ok();
    for sc in scenarios.iter().filter(|s| only.as_deref().is_none_or(|o| o.split(',').any(|n| n == s.name))) {
        for merged in [false, true] {
            let s = run(&dir, sc, merged).await;
            eprintln!("{} {}: {}", sc.name, if merged { "MERGED" } else { "shared" }, s.summary());
            assert!(s.lines[1] > 0);
            assert!(s.max_wait_ms() <= bound_ms, "{}: a diff waited {} ms", sc.name, s.max_wait_ms());
            assert_eq!(s.late_add_blocks(), 0, "{}: new levels added after their block", sc.name);
            assert_eq!(s.late_mod_blocks(), 0, "{}: updates/removes after their block", sc.name);
            // same book (levels, sizes, queue order) and same L4 metadata as the
            // in-sync reference, however the statuses lagged, stalled or led
            match &reference {
                // A restart that rewrites blocks already applied is not
                // idempotent for an oid re-used inside the rewound window
                // (2 of ~50k orders here: queue position / size): reported,
                // not asserted - the same holds for any design that applies
                // the rewritten diffs.
                Some(r) if sc.restart.is_some() => report_book_diff(r, &s, sc.name),
                Some(r) => same_book_and_meta(r, &s, sc.name),
                None => reference = Some(s),
            }
        }
    }
}

fn report_book_diff(a: &Stats, b: &Stats, label: &str) {
    let diff: Vec<_> = a.book.iter().filter(|(k, v)| b.book.get(*k) != Some(*v)).collect();
    eprintln!("{label}: {} of {} orders differ from the reference", diff.len(), a.book.len());
    for (k, v) in diff.iter().take(10) {
        eprintln!("{label}: oid {k}: reference {v:?} vs {:?}", b.book.get(k));
    }
}

/// Book (side, px, sz, queue position) identical; L4 metadata identical for
/// every order that has its status in both runs.
fn same_book_and_meta(a: &Stats, b: &Stats, label: &str) {
    assert!(!a.book.is_empty());
    assert_eq!(a.book.len(), b.book.len(), "{label}: resting order count");
    let diff_book = a.book.iter().filter(|(k, v)| b.book.get(*k) != Some(*v)).count();
    assert_eq!(diff_book, 0, "{label}: {diff_book} orders differ in side/px/sz/queue");
    let diff_meta = a.meta.iter().filter(|(k, v)| b.meta.get(*k) != Some(*v)).count();
    eprintln!("{label}: book identical ({} orders); metadata differs on {diff_meta} (awaiting {} / {})", a.book.len(), a.awaiting, b.awaiting);
    assert!(diff_meta <= a.awaiting.max(b.awaiting), "{label}: metadata differs beyond the orders still awaiting a status");
}
