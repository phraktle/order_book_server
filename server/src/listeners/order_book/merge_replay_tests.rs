//! Offline replay of a captured node slice through the watcher merge.
//!
//! `OBS_REPLAY_SLICE=<dir>` holds `statuses.jsonl.zst`, `diffs.jsonl.zst` and
//! `fills.jsonl.zst`: the node's `node_order_statuses_streaming`,
//! `node_raw_book_diffs_streaming` and `node_fills_streaming` lines for one
//! block range, verbatim (zstd; read through `zstd -dc`). Run:
//!
//!   OBS_REPLAY_SLICE=/path cargo test --release -p server merge_replay -- --ignored --nocapture --test-threads=1
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
use crate::order_book::Oid;
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
const HOLD: Duration = Duration::from_millis(crate::DEFAULT_MERGE_HOLD_MS);

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
    holds_expired: u64,
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
            "lines s/d/f={:?} adds={} late_adds={} {:?} mods={} late_mods={} {:?} diff_wait_ms p50={} p99={} max={} holds_expired={}",
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
            self.holds_expired
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
                let pending: usize =
                    batch.events_ref().iter().filter(|s| state.pending_new_diffs_has(&Oid::new(s.order.oid))).count();
                drop(state.apply_order_statuses_hft(batch));
                let late = state.height().saturating_sub(b);
                st.adds += pending;
                if late > 0 && pending > 0 {
                    *st.late_adds.entry(late).or_default() += pending;
                }
            }
            EventBatch::BookDiffs(batch) => {
                let kinds: Vec<(bool, bool)> = batch
                    .events_ref()
                    .iter()
                    .filter(|d| !d.coin().is_spot())
                    .map(|d| match d.diff() {
                        OrderDiff::New { .. } => (true, state.pending_order_statuses_has(&d.oid())),
                        _ => (false, false),
                    })
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
                    let late = state.height().saturating_sub(b);
                    for (is_new, paired) in kinds {
                        if is_new {
                            if paired {
                                st.adds += 1;
                                if late > 0 {
                                    *st.late_adds.entry(late).or_default() += 1;
                                }
                            }
                        } else {
                            st.mods += 1;
                            if late > 0 {
                                *st.late_mods.entry(late).or_default() += 1;
                            }
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
    let expired0 = crate::metrics::MERGE_HOLD_EXPIRED_TOTAL.get();
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
        let mut m = MergedEvents::new([srx, drx, frx], HOLD);
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
    c.st.holds_expired = crate::metrics::MERGE_HOLD_EXPIRED_TOTAL.get() - expired0;
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
async fn merge_replay_backlog() {
    let dir = slice();
    let sc = Scenario {
        name: "backlog",
        feed: Feed::Backlog { consumer_bytes_per_ms: env_u64("OBS_REPLAY_BPMS", 150_000) },
        range: (env_u64("OBS_REPLAY_FROM", 0), env_u64("OBS_REPLAY_TO", u64::MAX)),
        status_delay: Duration::ZERO,
        status_stall: None,
        restart: None,
    };
    for merged in [false, true] {
        let t = Instant::now();
        let s = run(&dir, &sc, merged).await;
        eprintln!("{} {} ({:.0?}): {}", sc.name, if merged { "MERGED" } else { "shared" }, t.elapsed(), s.summary());
        assert!(s.adds > 0 && s.mods > 0, "the slice must exercise adds and updates/removes");
        if merged {
            assert_eq!(s.late_add_blocks(), 0, "merged: new levels added after their block");
            assert_eq!(s.late_mod_blocks(), 0, "merged: updates/removes applied after their block");
        }
    }
}

/// Real node timing (about a minute, by default the start of the slice) plus
/// the edge cases.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs OBS_REPLAY_SLICE (a captured node slice)"]
async fn merge_replay_paced_edge_cases() {
    let dir = slice();
    let from = env_u64("OBS_PACED_FROM", 0).max(lines(&dir.join("diffs.jsonl.zst")).next().map_or(0, |l| height_of(&l)));
    let to = env_u64("OBS_PACED_TO", from + 850);
    let mid = from + (to - from) / 2;
    let base = Scenario {
        name: "paced",
        feed: Feed::Paced,
        range: (from, to),
        status_delay: Duration::ZERO,
        status_stall: None,
        restart: None,
    };
    let scenarios = [
        base.clone(),
        Scenario { name: "statuses-lag-200ms", status_delay: Duration::from_millis(200), ..base.clone() },
        Scenario { name: "statuses-stall-3s", status_stall: Some((mid, Duration::from_secs(3))), ..base.clone() },
        Scenario { name: "node-restart", restart: Some((mid, 100, Duration::from_secs(3))), ..base.clone() },
    ];
    // Slack on top of the hold for the consumer's own batch processing.
    let bound_ms = HOLD.as_millis() as u64 + env_u64("OBS_WAIT_SLACK_MS", 50);
    let only = std::env::var("OBS_PACED_ONLY").ok();
    for sc in scenarios.iter().filter(|s| only.as_deref().is_none_or(|o| o == s.name)) {
        for merged in [false, true] {
            let s = run(&dir, sc, merged).await;
            eprintln!("{} {}: {}", sc.name, if merged { "MERGED" } else { "shared" }, s.summary());
            assert!(s.lines[1] > 0);
            if merged {
                // never hold diffs beyond the bound, whatever the statuses do
                assert!(s.max_wait_ms() <= bound_ms, "{}: a diff waited {} ms", sc.name, s.max_wait_ms());
                if sc.name == "paced" {
                    assert_eq!(s.late_add_blocks(), 0, "paced: new levels added after their block");
                }
                assert_eq!(s.late_mod_blocks(), 0, "{}: updates/removes after their block", sc.name);
            }
        }
    }
}
