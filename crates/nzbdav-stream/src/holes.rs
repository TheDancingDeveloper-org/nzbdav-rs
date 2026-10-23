//! The hole model: when a missing article becomes a recorded hole.
//!
//! The rules follow zurg's `internal/nzb/holes.go` and mirror the WI-143
//! damage ledger in rustnzb's `nzb-core` (same vocabulary, same rules), so a
//! ledger backed by that crate can replace [`InMemoryHoleLedger`] once
//! nzbdav-rs re-pins to it (WI-167):
//!
//! - A transport failure (connect, auth, protocol error, timeout) is never a
//!   miss. Only a `430` is evidence that an article is gone.
//! - The first refusal is provisional. It confirms only once every enabled
//!   server has refused the article and at least the confirm delay has passed
//!   since the first refusal: a provider is known to answer `430` for an
//!   article it holds while busy (a FatherBrown article was refused twice
//!   350 ms apart, then served a minute later).
//! - A confirmed hole is believed for [`HOLE_TTL`]; after that one caller
//!   re-probes while the others keep the fast answer.
//! - Every record is keyed to the set of servers that were asked
//!   ([`server_fingerprint`]); adding or removing a server invalidates it.
//! - The ledger is bounded: a provisional hole nobody confirms lapses after
//!   the confirm deadline plus a margin, a confirmed hole long past its TTL is
//!   swept, and past a capacity the least recently used records are evicted.
//!
//! Callers translate their own failures into an [`Outcome`] before calling
//! [`HoleLedger::note`]: a body is [`Outcome::Present`], a `430` is
//! [`Outcome::Refused`] and everything else is [`Outcome::Transport`].

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::time::Instant;
use tracing::debug;

/// How long a confirmed hole is believed before a re-probe (zurg `holeTTL`).
pub const HOLE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
/// Minimum gap between the first `430` and the confirming one
/// (zurg `holeConfirmDelay`).
pub const HOLE_CONFIRM_DELAY: Duration = Duration::from_secs(1);
/// Upper bound on the confirming re-ask (zurg `holeConfirmFetch`).
pub const HOLE_CONFIRM_FETCH: Duration = Duration::from_secs(5);
/// A re-probe claim older than this is abandoned and another caller may
/// re-probe.
const REPROBE_CLAIM_TIMEOUT: Duration = Duration::from_secs(30);
/// Slack on top of the confirm delay and deadline before an unconfirmed
/// provisional hole lapses.
const PROVISIONAL_MARGIN: Duration = Duration::from_secs(60);
/// How long a confirmed hole is kept past its TTL, so the readers that hit it
/// keep the fast answer while one of them re-probes. After that it is swept.
const EXPIRED_GRACE: Duration = Duration::from_secs(60 * 60);
/// Least often the ledger sweeps lapsed records when it inserts one.
const SWEEP_INTERVAL: Duration = Duration::from_secs(60);
/// Records the in-memory ledger holds before evicting the least recently
/// used.
pub const DEFAULT_HOLE_CAPACITY: usize = 100_000;

/// State of a hole in the ledger.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HoleState {
    /// Refused, not yet confirmed; counts as uncertain damage.
    Provisional,
    /// Every server refused it across the confirm delay; counts as damage.
    Confirmed,
}

/// The result of one attempt on one server, in the ledger's vocabulary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// A body came back: the article is there.
    Present,
    /// The server answered `430`.
    Refused { server: String },
    /// The server did not answer. Never evidence that the article is missing.
    Transport { server: String },
}

/// What the ledger made of an outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Noted {
    /// The article's hole state after the outcome, `None` when it has none.
    pub state: Option<HoleState>,
    /// The hole is provisional and a confirming re-ask should follow.
    pub confirm_needed: bool,
}

/// Guidance before fetching an article.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Consult {
    /// No hole on record: fetch.
    Fetch,
    /// A provisional refusal is on record: join the confirm in flight, if
    /// there is one, rather than fetching again.
    Provisional,
    /// A confirmed hole within its TTL (or one another caller is
    /// re-probing): do not fetch.
    Skip,
    /// A confirmed hole whose TTL ran out, claimed by this caller: re-probe.
    Reprobe,
}

/// A snapshot of one hole, for the census and the repair job.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HoleRecord {
    pub message_id: String,
    pub state: HoleState,
    /// Servers whose last answer was `430`.
    pub refused_by: Vec<String>,
    /// Servers whose last answer was a transport failure.
    pub unreachable: Vec<String>,
    pub first_refused_at: Instant,
    pub confirmed_at: Option<Instant>,
    pub expires_at: Option<Instant>,
    pub server_fingerprint: String,
}

/// The damage ledger. The provider consults it before a fetch and notes
/// every outcome; the damage census and the repair job read and feed it.
pub trait HoleLedger: Send + Sync {
    /// Record one outcome for an article. `servers` is every server the
    /// fetch could have used; it fingerprints the record.
    fn note(&self, message_id: &str, outcome: &Outcome, servers: &[String]) -> Noted;
    /// Guidance before fetching an article from `servers`.
    fn consult(&self, message_id: &str, servers: &[String]) -> Consult;
    /// The article's hole state, if it has one.
    fn state(&self, message_id: &str) -> Option<HoleState>;
    /// Every hole on record.
    fn holes(&self) -> Vec<HoleRecord>;
}

/// A stable fingerprint of the set of servers that were asked.
pub fn server_fingerprint(servers: &[String]) -> String {
    let set: BTreeSet<&str> = servers.iter().map(String::as_str).collect();
    set.into_iter().collect::<Vec<_>>().join(",")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Evidence {
    Refused,
    Transport,
}

struct Entry {
    state: HoleState,
    evidence: HashMap<String, Evidence>,
    first_refused_at: Instant,
    confirmed_at: Option<Instant>,
    expires_at: Option<Instant>,
    fingerprint: String,
    reprobe_claimed_at: Option<Instant>,
    /// Use counter value at the last note or consult, for LRU eviction.
    touched: u64,
}

impl Entry {
    fn expired(&self, now: Instant) -> bool {
        self.expires_at.is_some_and(|exp| now >= exp)
    }

    /// No longer evidence of anything: an unconfirmed provisional hole past
    /// `provisional_ttl`, or a confirmed hole past its TTL plus the grace.
    fn lapsed(&self, now: Instant, provisional_ttl: Duration) -> bool {
        match self.state {
            HoleState::Provisional => now.duration_since(self.first_refused_at) >= provisional_ttl,
            HoleState::Confirmed => self
                .expires_at
                .is_some_and(|exp| now >= exp + EXPIRED_GRACE),
        }
    }

    fn servers_with(&self, kind: Evidence) -> Vec<String> {
        let mut out: Vec<String> = self
            .evidence
            .iter()
            .filter(|(_, k)| **k == kind)
            .map(|(s, _)| s.clone())
            .collect();
        out.sort();
        out
    }
}

/// In-memory ledger. Only holes are held: a present answer drops the record,
/// so memory grows with damage, not with traffic, and is bounded: lapsed
/// records are swept when new ones go in, and past the capacity the least
/// recently used are evicted. Nothing survives a restart until a persisted
/// ledger (nzb-core, WI-167) replaces this one.
pub struct InMemoryHoleLedger {
    confirm_after: Duration,
    ttl: Duration,
    provisional_ttl: Duration,
    capacity: usize,
    inner: Mutex<Inner>,
}

struct Inner {
    holes: HashMap<String, Entry>,
    /// Use counter; orders records for LRU eviction.
    clock: u64,
    next_sweep: Option<Instant>,
}

impl Inner {
    fn tick(&mut self) -> u64 {
        self.clock += 1;
        self.clock
    }
}

impl Default for InMemoryHoleLedger {
    fn default() -> Self {
        Self::new(HOLE_CONFIRM_DELAY)
    }
}

impl InMemoryHoleLedger {
    /// A ledger confirming a hole on a refusal at least `confirm_after` after
    /// the first. A zero delay confirms on the first refusal from every
    /// server.
    pub fn new(confirm_after: Duration) -> Self {
        Self {
            confirm_after,
            ttl: HOLE_TTL,
            provisional_ttl: confirm_after + HOLE_CONFIRM_FETCH + PROVISIONAL_MARGIN,
            capacity: DEFAULT_HOLE_CAPACITY,
            inner: Mutex::new(Inner {
                holes: HashMap::new(),
                clock: 0,
                next_sweep: None,
            }),
        }
    }

    /// How long a confirmed hole is believed.
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// How long a provisional hole waits for its confirm before it lapses.
    pub fn with_provisional_ttl(mut self, ttl: Duration) -> Self {
        self.provisional_ttl = ttl;
        self
    }

    /// Records held before the least recently used are evicted (at least 1).
    pub fn with_capacity(mut self, capacity: usize) -> Self {
        self.capacity = capacity.max(1);
        self
    }

    /// Records held, lapsed ones included until the next sweep.
    pub fn len(&self) -> usize {
        self.inner.lock().holes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn sweep(&self, inner: &mut Inner, now: Instant) {
        inner
            .holes
            .retain(|_, e| !e.lapsed(now, self.provisional_ttl));
        inner.next_sweep = Some(now + SWEEP_INTERVAL);
    }

    /// Make room for one new record: sweep lapsed records when due or when
    /// full, then evict the least recently used down to 90% of capacity.
    fn make_room(&self, inner: &mut Inner, now: Instant) {
        let full = inner.holes.len() >= self.capacity;
        if full || inner.next_sweep.is_none_or(|at| now >= at) {
            self.sweep(inner, now);
        }
        if inner.holes.len() < self.capacity {
            return;
        }
        let keep = (self.capacity * 9 / 10).min(self.capacity - 1);
        let mut by_use: Vec<(u64, String)> = inner
            .holes
            .iter()
            .map(|(id, e)| (e.touched, id.clone()))
            .collect();
        by_use.sort_unstable();
        let evict = inner.holes.len() - keep;
        for (_, id) in by_use.into_iter().take(evict) {
            inner.holes.remove(&id);
        }
        debug!(
            evicted = evict,
            capacity = self.capacity,
            "hole ledger full, evicted least recently used"
        );
    }
}

impl HoleLedger for InMemoryHoleLedger {
    fn note(&self, message_id: &str, outcome: &Outcome, servers: &[String]) -> Noted {
        let now = Instant::now();
        let fingerprint = server_fingerprint(servers);
        let mut inner = self.inner.lock();
        let touched = inner.tick();

        // A lapsed record is gone whatever the outcome. One keyed to another
        // server set, or a confirmed hole past its TTL, is no evidence any
        // more: a conclusive outcome starts it over.
        if inner.holes.get(message_id).is_some_and(|e| {
            e.lapsed(now, self.provisional_ttl)
                || (!matches!(outcome, Outcome::Transport { .. })
                    && (e.fingerprint != fingerprint || e.expired(now)))
        }) {
            inner.holes.remove(message_id);
        }
        if matches!(outcome, Outcome::Refused { .. }) && !inner.holes.contains_key(message_id) {
            self.make_room(&mut inner, now);
        }
        let holes = &mut inner.holes;

        match outcome {
            Outcome::Present => {
                holes.remove(message_id);
                Noted {
                    state: None,
                    confirm_needed: false,
                }
            }
            Outcome::Transport { server } => {
                // Inconclusive: note who was unreachable on an existing hole,
                // release any re-probe claim so another caller can try, and
                // never create or advance a hole.
                let Some(entry) = holes.get_mut(message_id) else {
                    return Noted {
                        state: None,
                        confirm_needed: false,
                    };
                };
                entry.touched = touched;
                entry.evidence.insert(server.clone(), Evidence::Transport);
                entry.reprobe_claimed_at = None;
                Noted {
                    state: Some(entry.state),
                    confirm_needed: false,
                }
            }
            Outcome::Refused { server } => {
                let entry = holes
                    .entry(message_id.to_string())
                    .or_insert_with(|| Entry {
                        state: HoleState::Provisional,
                        evidence: HashMap::new(),
                        first_refused_at: now,
                        confirmed_at: None,
                        expires_at: None,
                        fingerprint,
                        reprobe_claimed_at: None,
                        touched,
                    });
                entry.touched = touched;
                entry.evidence.insert(server.clone(), Evidence::Refused);
                if entry.state == HoleState::Confirmed {
                    return Noted {
                        state: Some(HoleState::Confirmed),
                        confirm_needed: false,
                    };
                }

                let all_refused = !servers.is_empty()
                    && servers
                        .iter()
                        .all(|s| entry.evidence.get(s) == Some(&Evidence::Refused));
                let waited = now.duration_since(entry.first_refused_at) >= self.confirm_after;
                if all_refused && waited {
                    entry.state = HoleState::Confirmed;
                    entry.confirmed_at = Some(now);
                    entry.expires_at = Some(now + self.ttl);
                    entry.reprobe_claimed_at = None;
                }
                Noted {
                    state: Some(entry.state),
                    confirm_needed: entry.state == HoleState::Provisional,
                }
            }
        }
    }

    fn consult(&self, message_id: &str, servers: &[String]) -> Consult {
        let now = Instant::now();
        let mut inner = self.inner.lock();
        let touched = inner.tick();
        let holes = &mut inner.holes;
        let Some(entry) = holes.get_mut(message_id) else {
            return Consult::Fetch;
        };
        if entry.fingerprint != server_fingerprint(servers)
            || entry.lapsed(now, self.provisional_ttl)
        {
            holes.remove(message_id);
            return Consult::Fetch;
        }
        entry.touched = touched;
        match entry.state {
            HoleState::Provisional => Consult::Provisional,
            HoleState::Confirmed if !entry.expired(now) => Consult::Skip,
            HoleState::Confirmed => {
                let claimed = entry
                    .reprobe_claimed_at
                    .is_some_and(|at| now.duration_since(at) < REPROBE_CLAIM_TIMEOUT);
                if claimed {
                    Consult::Skip
                } else {
                    entry.reprobe_claimed_at = Some(now);
                    Consult::Reprobe
                }
            }
        }
    }

    fn state(&self, message_id: &str) -> Option<HoleState> {
        let now = Instant::now();
        self.inner
            .lock()
            .holes
            .get(message_id)
            .filter(|e| !e.lapsed(now, self.provisional_ttl))
            .map(|e| e.state)
    }

    fn holes(&self) -> Vec<HoleRecord> {
        let mut inner = self.inner.lock();
        self.sweep(&mut inner, Instant::now());
        let mut out: Vec<HoleRecord> = inner
            .holes
            .iter()
            .map(|(id, e)| HoleRecord {
                message_id: id.clone(),
                state: e.state,
                refused_by: e.servers_with(Evidence::Refused),
                unreachable: e.servers_with(Evidence::Transport),
                first_refused_at: e.first_refused_at,
                confirmed_at: e.confirmed_at,
                expires_at: e.expires_at,
                server_fingerprint: e.fingerprint.clone(),
            })
            .collect();
        out.sort_by(|a, b| a.message_id.cmp(&b.message_id));
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn servers(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    fn refused(server: &str) -> Outcome {
        Outcome::Refused {
            server: server.to_string(),
        }
    }

    fn transport(server: &str) -> Outcome {
        Outcome::Transport {
            server: server.to_string(),
        }
    }

    #[tokio::test(start_paused = true)]
    async fn first_refusal_is_provisional_and_confirms_after_the_delay() {
        let ledger = InMemoryHoleLedger::default();
        let s = servers(&["a"]);
        let n = ledger.note("x", &refused("a"), &s);
        assert_eq!(n.state, Some(HoleState::Provisional));
        assert!(n.confirm_needed);

        tokio::time::advance(Duration::from_millis(350)).await;
        let n = ledger.note("x", &refused("a"), &s);
        assert_eq!(n.state, Some(HoleState::Provisional), "350 ms is too soon");

        tokio::time::advance(Duration::from_millis(650)).await;
        let n = ledger.note("x", &refused("a"), &s);
        assert_eq!(n.state, Some(HoleState::Confirmed));
        assert!(!n.confirm_needed);
        assert_eq!(ledger.consult("x", &s), Consult::Skip);
    }

    #[tokio::test(start_paused = true)]
    async fn confirming_needs_every_server_to_refuse() {
        let ledger = InMemoryHoleLedger::default();
        let s = servers(&["a", "b"]);
        ledger.note("x", &refused("a"), &s);
        tokio::time::advance(Duration::from_secs(2)).await;
        let n = ledger.note("x", &refused("a"), &s);
        assert_eq!(n.state, Some(HoleState::Provisional));
        let n = ledger.note("x", &refused("b"), &s);
        assert_eq!(n.state, Some(HoleState::Confirmed));
    }

    #[tokio::test(start_paused = true)]
    async fn transport_failure_never_creates_or_advances_a_hole() {
        let ledger = InMemoryHoleLedger::default();
        let s = servers(&["a", "b"]);
        let n = ledger.note("x", &transport("a"), &s);
        assert_eq!(n.state, None);
        assert_eq!(ledger.state("x"), None);
        assert_eq!(ledger.consult("x", &s), Consult::Fetch);

        ledger.note("x", &refused("a"), &s);
        tokio::time::advance(Duration::from_secs(2)).await;
        let n = ledger.note("x", &transport("b"), &s);
        assert_eq!(n.state, Some(HoleState::Provisional));
        assert_eq!(ledger.holes()[0].unreachable, vec!["b".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn present_closes_the_hole() {
        let ledger = InMemoryHoleLedger::new(Duration::ZERO);
        let s = servers(&["a"]);
        assert_eq!(
            ledger.note("x", &refused("a"), &s).state,
            Some(HoleState::Confirmed)
        );
        ledger.note("x", &Outcome::Present, &s);
        assert_eq!(ledger.state("x"), None);
        assert!(ledger.holes().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn expired_hole_is_reprobed_by_one_caller() {
        let ledger = InMemoryHoleLedger::new(Duration::ZERO);
        let s = servers(&["a"]);
        ledger.note("x", &refused("a"), &s);
        tokio::time::advance(HOLE_TTL).await;

        assert_eq!(ledger.consult("x", &s), Consult::Reprobe);
        assert_eq!(
            ledger.consult("x", &s),
            Consult::Skip,
            "others keep the fast answer"
        );

        // A transport failure on the re-probe releases the claim.
        ledger.note("x", &transport("a"), &s);
        assert_eq!(ledger.consult("x", &s), Consult::Reprobe);

        // An abandoned claim times out.
        tokio::time::advance(REPROBE_CLAIM_TIMEOUT).await;
        assert_eq!(ledger.consult("x", &s), Consult::Reprobe);
    }

    #[tokio::test(start_paused = true)]
    async fn refusal_after_expiry_starts_a_fresh_provisional_hole() {
        let ledger = InMemoryHoleLedger::default();
        let s = servers(&["a"]);
        ledger.note("x", &refused("a"), &s);
        tokio::time::advance(HOLE_CONFIRM_DELAY).await;
        ledger.note("x", &refused("a"), &s);
        assert_eq!(ledger.state("x"), Some(HoleState::Confirmed));

        tokio::time::advance(HOLE_TTL).await;
        let n = ledger.note("x", &refused("a"), &s);
        assert_eq!(n.state, Some(HoleState::Provisional));
        assert!(n.confirm_needed);
    }

    #[tokio::test(start_paused = true)]
    async fn changed_server_set_invalidates_the_record() {
        let ledger = InMemoryHoleLedger::new(Duration::ZERO);
        ledger.note("x", &refused("a"), &servers(&["a"]));
        assert_eq!(ledger.consult("x", &servers(&["a"])), Consult::Skip);
        assert_eq!(ledger.consult("x", &servers(&["a", "b"])), Consult::Fetch);
        assert_eq!(ledger.state("x"), None);
    }

    /// A provisional hole nobody confirms lapses after the confirm deadline
    /// plus the margin, and a later refusal starts a fresh one.
    #[tokio::test(start_paused = true)]
    async fn unconfirmed_provisional_hole_lapses() {
        let ledger = InMemoryHoleLedger::default();
        let s = servers(&["a", "b"]);
        ledger.note("x", &refused("a"), &s);
        let lapse = HOLE_CONFIRM_DELAY + HOLE_CONFIRM_FETCH + PROVISIONAL_MARGIN;
        tokio::time::advance(lapse - Duration::from_millis(1)).await;
        assert_eq!(ledger.state("x"), Some(HoleState::Provisional));

        tokio::time::advance(Duration::from_millis(1)).await;
        assert_eq!(ledger.state("x"), None);
        assert!(ledger.holes().is_empty());
        assert!(ledger.is_empty(), "swept");

        // The stale refusal from "a" does not count towards a confirm.
        ledger.note("y", &refused("a"), &s);
        tokio::time::advance(lapse).await;
        ledger.note("y", &refused("b"), &s);
        assert_eq!(ledger.state("y"), Some(HoleState::Provisional));
        assert_eq!(ledger.holes()[0].refused_by, vec!["b".to_string()]);
        assert_eq!(ledger.consult("x", &s), Consult::Fetch);
    }

    /// Confirmed holes long past their TTL are swept when a new record goes
    /// in, without anyone consulting them again.
    #[tokio::test(start_paused = true)]
    async fn expired_confirmed_holes_are_swept_on_insert() {
        let ledger = InMemoryHoleLedger::new(Duration::ZERO);
        let s = servers(&["a"]);
        for id in ["x", "y", "z"] {
            ledger.note(id, &refused("a"), &s);
        }
        assert_eq!(ledger.len(), 3);

        // Past the TTL but within the grace: kept, so one caller re-probes.
        tokio::time::advance(HOLE_TTL).await;
        ledger.note("w", &refused("a"), &s);
        assert_eq!(ledger.len(), 4);
        assert_eq!(ledger.consult("x", &s), Consult::Reprobe);

        tokio::time::advance(EXPIRED_GRACE).await;
        ledger.note("v", &refused("a"), &s);
        // x, y and z went; w is still within its TTL.
        assert_eq!(ledger.len(), 2);
        assert_eq!(ledger.state("w"), Some(HoleState::Confirmed));
        assert_eq!(ledger.state("x"), None);
        assert_eq!(ledger.state("v"), Some(HoleState::Confirmed));
    }

    /// Past the capacity the least recently used records are evicted; a
    /// consult counts as a use.
    #[tokio::test(start_paused = true)]
    async fn capacity_evicts_least_recently_used() {
        let ledger = InMemoryHoleLedger::new(Duration::ZERO).with_capacity(10);
        let s = servers(&["a"]);
        for i in 0..10 {
            ledger.note(&format!("id{i}"), &refused("a"), &s);
        }
        assert_eq!(ledger.len(), 10);
        // id0 is the oldest insert but was just used.
        assert_eq!(ledger.consult("id0", &s), Consult::Skip);

        ledger.note("new", &refused("a"), &s);
        // Evicted down to 90% of capacity (9), then the new record: 10.
        assert_eq!(ledger.len(), 10);
        assert_eq!(ledger.state("id0"), Some(HoleState::Confirmed));
        assert_eq!(ledger.state("id1"), None, "least recently used went");
        assert_eq!(ledger.state("id2"), Some(HoleState::Confirmed));
        assert_eq!(ledger.state("new"), Some(HoleState::Confirmed));

        // Updating a record already held never evicts.
        ledger.note("id5", &refused("a"), &s);
        assert_eq!(ledger.len(), 10);

        for i in 0..100 {
            ledger.note(&format!("more{i}"), &refused("a"), &s);
            assert!(ledger.len() <= 10);
        }
    }

    #[test]
    fn fingerprint_is_order_and_duplicate_insensitive() {
        assert_eq!(
            server_fingerprint(&servers(&["b", "a", "b"])),
            server_fingerprint(&servers(&["a", "b"]))
        );
    }
}
