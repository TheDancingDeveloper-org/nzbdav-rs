//! `PrioritizedSemaphore` — a tiered permit system for article fetches.
//!
//! Permits are handed out by tier, highest first:
//!
//! 1. [`PermitTier::Demand`]: the segment a client is waiting on right now
//!    (playback, range seeks).
//! 2. [`PermitTier::ReadAhead`]: prefetch beyond the demanded segment, and
//!    ingest (pipeline first-segment and metadata fetches).
//! 3. [`PermitTier::Repair`]: repairing a missing or damaged range.
//!
//! Two floors protect the lower-capacity tiers:
//!
//! - The **demand reserve** is set aside: ingest and repair never take
//!   the last `demand_reserved - held(Demand)` free permits, so playback
//!   always has headroom without having to preempt anyone. Read-ahead may
//!   borrow the unused reserve while no demand waiter is queued, so a lone
//!   stream on an idle pool can use every connection; the moment a demand
//!   request queues, the next freed permit goes to it and read-ahead stops
//!   borrowing. Reserve use is derived from the held counts (non-demand
//!   permits beyond `total - demand_reserved`), so it cannot drift as
//!   permits turn over, and repair never takes a reserve permit.
//! - The **repair floor** is guaranteed against read-ahead while repair is
//!   active (a repair permit is held or one is waited for). Repair gets
//!   freed permits ahead of read-ahead until it holds its floor, and
//!   read-ahead leaves the unmet part of the floor free in the share (it
//!   borrows the reserve only once repair is at its floor). With no repair work
//!   the floor costs read-ahead nothing. This is zurg's "a repair is
//!   guaranteed a quarter of the allowance against read-ahead, so the file
//!   being played can still repair the gap it is stuck on".
//!
//! Demand is strictly first: while demand waiters queue, no lower tier is
//! granted a permit. Above its floor, repair is strictly last.
//!
//! The grant rules mirror rustnzb's `nzb_nntp::budget::ServerBudget`
//! (Playback / Download / Repair there; Import has no counterpart here), so
//! the two behave the same if nzbdav moves onto that budget.
//!
//! Callers that never use [`PrioritizedSemaphore::acquire_repair`] see the
//! old two-tier behaviour: high can use the whole pool, low is held to
//! `total - reserved` plus whatever demand already uses of its reserve.

use std::collections::VecDeque;
use std::sync::Arc;

use parking_lot::Mutex;
use tokio::sync::oneshot;

/// Default share of the pool guaranteed to repair against read-ahead.
const DEFAULT_REPAIR_FLOOR_PCT: usize = 25;

/// Who is asking for a permit. Earlier variants have higher priority.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PermitTier {
    /// The segment a client is blocked on.
    Demand,
    /// Prefetch beyond the demanded segment, and ingest.
    ReadAhead,
    /// Repair of a missing or damaged range.
    Repair,
}

impl PermitTier {
    const COUNT: usize = 3;

    fn idx(self) -> usize {
        match self {
            PermitTier::Demand => 0,
            PermitTier::ReadAhead => 1,
            PermitTier::Repair => 2,
        }
    }
}

struct State {
    total: usize,
    demand_reserved: usize,
    repair_floor: usize,
    held: [usize; PermitTier::COUNT],
    waiters: [VecDeque<Waiter>; PermitTier::COUNT],
}

struct Waiter {
    tx: oneshot::Sender<PrioritizedPermit>,
    /// May borrow the unused demand reserve (read-ahead, not ingest).
    borrows_reserve: bool,
}

impl State {
    fn held_total(&self) -> usize {
        self.held.iter().sum()
    }

    fn free(&self) -> usize {
        self.total.saturating_sub(self.held_total())
    }

    fn waiting(&self, tier: PermitTier) -> bool {
        !self.waiters[tier.idx()].is_empty()
    }

    fn prune(&mut self) {
        for queue in &mut self.waiters {
            queue.retain(|w| !w.tx.is_closed());
        }
    }

    fn unmet_demand(&self) -> usize {
        self.demand_reserved
            .saturating_sub(self.held[PermitTier::Demand.idx()])
    }

    /// Reserve permits held by tiers other than demand: whatever they hold
    /// beyond the non-reserve share. Derived from the held counts, so it
    /// cannot drift as permits turn over.
    fn reserve_borrowed(&self) -> usize {
        let others = self.held_total() - self.held[PermitTier::Demand.idx()];
        others.saturating_sub(self.total - self.demand_reserved)
    }

    fn unmet_repair(&self) -> usize {
        let repair = PermitTier::Repair;
        let active = self.held[repair.idx()] > 0 || self.waiting(repair);
        if active {
            self.repair_floor.saturating_sub(self.held[repair.idx()])
        } else {
            0
        }
    }

    /// The tier and queue position of the waiter that should get the next
    /// permit, if any can.
    fn next_grantable(&self) -> Option<(PermitTier, usize)> {
        let free = self.free();
        if free == 0 {
            return None;
        }
        if self.waiting(PermitTier::Demand) {
            return Some((PermitTier::Demand, 0));
        }
        let unmet_demand = self.unmet_demand();
        // Repair's floor ranks above read-ahead, so the part of the reserve
        // read-ahead has borrowed does not hold repair back from it.
        let reserve_left = unmet_demand.saturating_sub(self.reserve_borrowed());
        let repair_below_floor = self.held[PermitTier::Repair.idx()] < self.repair_floor;
        if self.waiting(PermitTier::Repair) && repair_below_floor && free > reserve_left {
            return Some((PermitTier::Repair, 0));
        }
        // No demand waiter is queued here, so read-ahead may borrow the
        // unused reserve; ingest may not. Either way the unmet repair floor
        // must still fit in the share, so read-ahead borrows only once
        // repair is at its floor (or idle).
        let unmet_repair = self.unmet_repair();
        let share_free = (self.total - self.demand_reserved)
            .saturating_sub(self.held_total() - self.held[PermitTier::Demand.idx()]);
        let readahead = self.waiters[PermitTier::ReadAhead.idx()]
            .iter()
            .position(|w| {
                if w.borrows_reserve && unmet_repair == 0 {
                    true
                } else {
                    share_free > unmet_repair && free > unmet_repair + unmet_demand
                }
            });
        if let Some(pos) = readahead {
            return Some((PermitTier::ReadAhead, pos));
        }
        if self.waiting(PermitTier::Repair) && free > unmet_demand {
            return Some((PermitTier::Repair, 0));
        }
        None
    }
}

struct Inner {
    state: Mutex<State>,
}

impl Inner {
    /// Hand free permits to waiters, highest priority first.
    fn dispatch(self: &Arc<Self>, state: &mut State) {
        state.prune();
        while let Some((tier, pos)) = state.next_grantable() {
            let Some(Waiter { tx, .. }) = state.waiters[tier.idx()].remove(pos) else {
                break;
            };
            state.held[tier.idx()] += 1;
            let permit = PrioritizedPermit {
                inner: Some(Arc::clone(self)),
                tier,
            };
            if let Err(mut permit) = tx.send(permit) {
                // The waiter left between the prune and the send. Undo by
                // hand: dropping the permit would re-lock the state.
                permit.inner = None;
                state.held[tier.idx()] -= 1;
            }
        }
    }

    fn release(self: &Arc<Self>, tier: PermitTier) {
        let mut state = self.state.lock();
        state.held[tier.idx()] -= 1;
        self.dispatch(&mut state);
    }
}

/// A semaphore with three priority tiers; see the module docs.
pub struct PrioritizedSemaphore {
    inner: Arc<Inner>,
}

/// RAII permit that releases back to the semaphore on drop.
pub struct PrioritizedPermit {
    inner: Option<Arc<Inner>>,
    tier: PermitTier,
}

impl PrioritizedPermit {
    /// The tier this permit was granted at.
    pub fn tier(&self) -> PermitTier {
        self.tier
    }
}

impl std::fmt::Debug for PrioritizedPermit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrioritizedPermit")
            .field("tier", &self.tier)
            .finish()
    }
}

impl Drop for PrioritizedPermit {
    fn drop(&mut self) {
        if let Some(inner) = self.inner.take() {
            inner.release(self.tier);
        }
    }
}

impl PrioritizedSemaphore {
    /// Create a new semaphore with the default repair floor (a quarter of
    /// the pool, rounded up, capped so read-ahead always keeps at least one
    /// permit).
    ///
    /// * `total_permits` — maximum concurrent permits across all tiers.
    /// * `high_priority_reserved` — permits set aside for demand. Must be
    ///   `<= total_permits`.
    ///
    /// # Panics
    ///
    /// Panics if `high_priority_reserved > total_permits`.
    pub fn new(total_permits: usize, high_priority_reserved: usize) -> Self {
        assert!(
            high_priority_reserved <= total_permits,
            "reserved ({high_priority_reserved}) must be <= total ({total_permits})"
        );
        let shared = total_permits - high_priority_reserved;
        let repair_floor = (total_permits * DEFAULT_REPAIR_FLOOR_PCT)
            .div_ceil(100)
            .min(shared.saturating_sub(1));
        Self::with_repair_floor(total_permits, high_priority_reserved, repair_floor)
    }

    /// Create a new semaphore with an explicit repair floor.
    ///
    /// The floor comes out of the permits read-ahead may use, never out of
    /// the demand reserve.
    ///
    /// # Panics
    ///
    /// Panics if `high_priority_reserved > total_permits` or
    /// `high_priority_reserved + repair_floor > total_permits`.
    pub fn with_repair_floor(
        total_permits: usize,
        high_priority_reserved: usize,
        repair_floor: usize,
    ) -> Self {
        assert!(
            high_priority_reserved <= total_permits,
            "reserved ({high_priority_reserved}) must be <= total ({total_permits})"
        );
        assert!(
            high_priority_reserved + repair_floor <= total_permits,
            "reserved ({high_priority_reserved}) + repair floor ({repair_floor}) \
             must be <= total ({total_permits})"
        );
        Self {
            inner: Arc::new(Inner {
                state: Mutex::new(State {
                    total: total_permits,
                    demand_reserved: high_priority_reserved,
                    repair_floor,
                    held: [0; PermitTier::COUNT],
                    waiters: Default::default(),
                }),
            }),
        }
    }

    /// Wait for a permit at `tier`. Dropping the future gives up the place
    /// in line; a permit already granted to it is returned.
    /// A read-ahead ask may borrow the unused demand reserve.
    pub async fn acquire(&self, tier: PermitTier) -> PrioritizedPermit {
        self.acquire_inner(tier, tier == PermitTier::ReadAhead)
            .await
    }

    async fn acquire_inner(&self, tier: PermitTier, borrows_reserve: bool) -> PrioritizedPermit {
        loop {
            let rx = {
                let mut state = self.inner.state.lock();
                let (tx, rx) = oneshot::channel();
                state.waiters[tier.idx()].push_back(Waiter {
                    tx,
                    borrows_reserve,
                });
                self.inner.dispatch(&mut state);
                rx
            };
            // The sender is only dropped without sending when the waiter is
            // pruned as closed, which cannot happen while `rx` is alive.
            if let Ok(permit) = rx.await {
                return permit;
            }
        }
    }

    /// Acquire a demand permit: the segment a client is waiting on. May use
    /// the whole pool, including the reserve.
    pub async fn acquire_high(&self) -> PrioritizedPermit {
        self.acquire(PermitTier::Demand).await
    }

    /// Acquire a read-ahead permit: prefetch beyond the demanded segment.
    /// May borrow the unused demand reserve while no demand waiter queues.
    pub async fn acquire_readahead(&self) -> PrioritizedPermit {
        self.acquire(PermitTier::ReadAhead).await
    }

    /// Acquire an ingest permit. Ingest sits on the read-ahead tier but
    /// never borrows the demand reserve.
    pub async fn acquire_low(&self) -> PrioritizedPermit {
        self.acquire_inner(PermitTier::ReadAhead, false).await
    }

    /// Acquire a repair permit: lowest tier, but guaranteed the repair floor
    /// against read-ahead and ingest.
    pub async fn acquire_repair(&self) -> PrioritizedPermit {
        self.acquire(PermitTier::Repair).await
    }

    /// Permits currently held at `tier`.
    pub fn held(&self, tier: PermitTier) -> usize {
        self.inner.state.lock().held[tier.idx()]
    }

    /// Permits currently held across all tiers.
    pub fn held_total(&self) -> usize {
        self.inner.state.lock().held_total()
    }

    /// Live waiters queued at `tier`.
    pub fn waiting(&self, tier: PermitTier) -> usize {
        let state = self.inner.state.lock();
        state.waiters[tier.idx()]
            .iter()
            .filter(|w| !w.tx.is_closed())
            .count()
    }

    /// `(demand_reserved, repair_floor)` in permits.
    pub fn floors(&self) -> (usize, usize) {
        let state = self.inner.state.lock();
        (state.demand_reserved, state.repair_floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::sync::Notify;
    use tokio::task::JoinHandle;
    use tokio::time::timeout;

    #[tokio::test]
    async fn test_new_creates_permits() {
        let sem = PrioritizedSemaphore::new(10, 3);
        // High priority should be able to acquire (uses shared pool of 10)
        let _h = sem.acquire_high().await;
        // Low priority should also work (shared=10, low_gate=7)
        let _l = sem.acquire_low().await;
    }

    #[tokio::test]
    async fn test_high_priority_gets_reserved() {
        let sem = PrioritizedSemaphore::new(2, 2);
        // High priority can acquire both permits
        let _h1 = sem.acquire_high().await;
        let _h2 = sem.acquire_high().await;

        // Low priority should block because low_gate capacity is 0
        let sem2 = PrioritizedSemaphore::new(2, 2);
        let result = timeout(Duration::from_millis(100), sem2.acquire_low()).await;
        assert!(
            result.is_err(),
            "acquire_low should block when all permits are reserved"
        );
    }

    #[tokio::test]
    async fn test_low_priority_uses_shared() {
        let sem = PrioritizedSemaphore::new(4, 1);
        // low_gate capacity = 3, so 3 low acquires should succeed
        let _l1 = sem.acquire_low().await;
        let _l2 = sem.acquire_low().await;
        let _l3 = sem.acquire_low().await;

        // 4th low acquire should block (low_gate exhausted)
        let result = timeout(Duration::from_millis(100), sem.acquire_low()).await;
        assert!(result.is_err(), "4th acquire_low should block");
    }

    #[tokio::test]
    async fn test_permit_drop_releases() {
        let sem = PrioritizedSemaphore::new(1, 1);
        let h = sem.acquire_high().await;
        drop(h);
        // After drop, we should be able to acquire again
        let _h2 = sem.acquire_high().await;
    }

    #[tokio::test]
    async fn test_concurrent_high_low() {
        let sem = Arc::new(PrioritizedSemaphore::new(2, 1));
        // low_gate capacity = 1, shared = 2

        // Exhaust all shared permits with high priority
        let h1 = sem.acquire_high().await;
        let h2 = sem.acquire_high().await;

        // Low should be blocked (shared exhausted)
        let sem_clone = Arc::clone(&sem);
        let handle = tokio::spawn(async move { sem_clone.acquire_low().await });

        // Give the spawned task a chance to start waiting
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!handle.is_finished(), "low acquire should be blocked");

        // Release one high permit — low should now unblock
        drop(h1);
        let result = timeout(Duration::from_millis(200), handle).await;
        assert!(
            result.is_ok(),
            "low acquire should unblock after high permit released"
        );

        drop(h2);
    }

    /// Spawn `n` tasks that each take a permit at `tier` and hold it until
    /// `release` fires.
    fn spawn_holders(
        sem: &Arc<PrioritizedSemaphore>,
        tier: PermitTier,
        n: usize,
        release: &Arc<Notify>,
    ) -> Vec<JoinHandle<()>> {
        (0..n)
            .map(|_| {
                let sem = Arc::clone(sem);
                let release = Arc::clone(release);
                tokio::spawn(async move {
                    let _permit = sem.acquire(tier).await;
                    release.notified().await;
                })
            })
            .collect()
    }

    /// Like [`spawn_holders`], for ingest asks (read-ahead tier, no
    /// borrowing of the demand reserve).
    fn spawn_ingest_holders(
        sem: &Arc<PrioritizedSemaphore>,
        n: usize,
        release: &Arc<Notify>,
    ) -> Vec<JoinHandle<()>> {
        (0..n)
            .map(|_| {
                let sem = Arc::clone(sem);
                let release = Arc::clone(release);
                tokio::spawn(async move {
                    let _permit = sem.acquire_low().await;
                    release.notified().await;
                })
            })
            .collect()
    }

    /// Wait until `cond` holds, failing the test after a second.
    async fn settle(what: &str, mut cond: impl FnMut() -> bool) {
        timeout(Duration::from_secs(1), async {
            while !cond() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("timed out waiting for {what}"));
    }

    #[test]
    fn default_repair_floor_is_a_quarter_capped() {
        assert_eq!(PrioritizedSemaphore::new(20, 5).floors(), (5, 5));
        assert_eq!(PrioritizedSemaphore::new(10, 3).floors(), (3, 3));
        // Read-ahead keeps at least one permit.
        assert_eq!(PrioritizedSemaphore::new(4, 3).floors(), (3, 0));
        assert_eq!(PrioritizedSemaphore::new(1, 0).floors(), (0, 0));
        assert_eq!(PrioritizedSemaphore::new(2, 2).floors(), (2, 0));
    }

    #[test]
    #[should_panic(expected = "repair floor")]
    fn floor_cannot_eat_demand_reserve() {
        let _ = PrioritizedSemaphore::with_repair_floor(10, 8, 3);
    }

    #[tokio::test]
    async fn demand_waiter_beats_readahead_waiter() {
        let sem = Arc::new(PrioritizedSemaphore::with_repair_floor(2, 0, 0));
        let a = sem.acquire_readahead().await;
        let _b = sem.acquire_readahead().await;

        let ra = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_readahead().await })
        };
        settle("read-ahead waiter", || {
            sem.waiting(PermitTier::ReadAhead) == 1
        })
        .await;
        let demand = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_high().await })
        };
        settle("demand waiter", || sem.waiting(PermitTier::Demand) == 1).await;

        drop(a);
        let permit = timeout(Duration::from_secs(1), demand)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permit.tier(), PermitTier::Demand);
        assert_eq!(sem.waiting(PermitTier::ReadAhead), 1);
        drop(permit);
        let permit = timeout(Duration::from_secs(1), ra).await.unwrap().unwrap();
        assert_eq!(permit.tier(), PermitTier::ReadAhead);
    }

    #[tokio::test]
    async fn readahead_beats_repair_above_floor() {
        let sem = Arc::new(PrioritizedSemaphore::with_repair_floor(4, 0, 1));
        let _r = sem.acquire_repair().await; // repair at its floor
        let a = sem.acquire_readahead().await;
        let _b = sem.acquire_readahead().await;
        let _c = sem.acquire_readahead().await;

        let repair = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_repair().await })
        };
        settle("repair waiter", || sem.waiting(PermitTier::Repair) == 1).await;
        let ra = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_readahead().await })
        };
        settle("read-ahead waiter", || {
            sem.waiting(PermitTier::ReadAhead) == 1
        })
        .await;

        drop(a);
        let permit = timeout(Duration::from_secs(1), ra).await.unwrap().unwrap();
        assert_eq!(permit.tier(), PermitTier::ReadAhead);
        assert_eq!(sem.waiting(PermitTier::Repair), 1);
        drop(permit);
        let permit = timeout(Duration::from_secs(1), repair)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permit.tier(), PermitTier::Repair);
    }

    /// Acceptance: 20 permits, 15 demand waiters and 15 repair waiters.
    /// Repair holds at least 5 while demand is saturated. Both tiers are
    /// queued at the same time (interleaved, repair first) behind a full
    /// pool, together with 15 read-ahead waiters, so they contend for every
    /// permit as it frees up. Without the floor, read-ahead would take the
    /// 5 permits demand leaves and repair would get none.
    async fn repair_floor_under_saturated_demand(sem: PrioritizedSemaphore) -> usize {
        let sem = Arc::new(sem);
        let unblock = Arc::new(Notify::new());
        let blockers = spawn_holders(&sem, PermitTier::Demand, 20, &unblock);
        settle("pool full", || sem.held_total() == 20).await;

        let release = Arc::new(Notify::new());
        let mut tasks = Vec::new();
        for _ in 0..15 {
            tasks.extend(spawn_holders(&sem, PermitTier::Repair, 1, &release));
            tasks.extend(spawn_holders(&sem, PermitTier::ReadAhead, 1, &release));
            tasks.extend(spawn_holders(&sem, PermitTier::Demand, 1, &release));
        }
        settle("all tiers queued", || {
            sem.waiting(PermitTier::Demand) == 15
                && sem.waiting(PermitTier::Repair) == 15
                && sem.waiting(PermitTier::ReadAhead) == 15
        })
        .await;

        for t in blockers {
            while !t.is_finished() {
                unblock.notify_waiters();
                tokio::task::yield_now().await;
            }
            t.await.unwrap();
        }

        settle("turnover", || sem.held_total() == 20).await;
        assert_eq!(sem.held(PermitTier::Demand), 15);
        assert_eq!(sem.waiting(PermitTier::Demand), 0);
        let repair = sem.held(PermitTier::Repair);

        for t in tasks {
            // Late holders missed the first notify; keep waking them.
            while !t.is_finished() {
                release.notify_waiters();
                tokio::task::yield_now().await;
            }
            t.await.unwrap();
        }
        assert_eq!(sem.held_total(), 0);
        repair
    }

    #[tokio::test]
    async fn repair_holds_floor_while_demand_saturated() {
        let repair = repair_floor_under_saturated_demand(PrioritizedSemaphore::new(20, 5)).await;
        assert!(repair >= 5, "repair held {repair}");
    }

    /// Control for the acceptance test: with no floor, repair gets nothing,
    /// so the test above depends on the floor.
    #[tokio::test]
    async fn without_floor_readahead_takes_what_demand_leaves() {
        let repair =
            repair_floor_under_saturated_demand(PrioritizedSemaphore::with_repair_floor(20, 5, 0))
                .await;
        assert_eq!(repair, 0);
    }

    /// Ingest cannot take repair's floor: as saturating ingest holders turn
    /// over, freed permits go to repair until it holds its floor, then
    /// ingest gets the rest. Ingest never borrows the demand reserve.
    #[tokio::test]
    async fn readahead_cannot_take_repair_floor() {
        let sem = Arc::new(PrioritizedSemaphore::new(20, 5));
        let held: Vec<_> = {
            let mut v = Vec::new();
            for _ in 0..15 {
                v.push(sem.acquire_low().await);
            }
            v
        };
        // Ingest is at its share (20 - 5 reserved).
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_low())
                .await
                .is_err()
        );

        let release = Arc::new(Notify::new());
        let mut tasks = spawn_ingest_holders(&sem, 15, &release);
        settle("read-ahead queued", || {
            sem.waiting(PermitTier::ReadAhead) == 15
        })
        .await;
        tasks.extend(spawn_holders(&sem, PermitTier::Repair, 15, &release));
        settle("repair queued", || sem.waiting(PermitTier::Repair) == 15).await;

        drop(held);
        settle("turnover", || {
            sem.held(PermitTier::Repair) + sem.held(PermitTier::ReadAhead) == 15
        })
        .await;
        assert_eq!(sem.held(PermitTier::Repair), 5);
        assert_eq!(sem.held(PermitTier::ReadAhead), 10);
        // The demand reserve stays free.
        assert_eq!(sem.held_total(), 15);

        // Demand still gets its reserve immediately.
        let mut demand = Vec::new();
        for _ in 0..5 {
            demand.push(
                timeout(Duration::from_millis(100), sem.acquire_high())
                    .await
                    .expect("demand reserve must be free"),
            );
        }
        drop(demand);

        for t in tasks {
            while !t.is_finished() {
                release.notify_waiters();
                tokio::task::yield_now().await;
            }
            t.await.unwrap();
        }
        assert_eq!(sem.held_total(), 0);
    }

    /// Read-ahead borrowing the demand reserve still cannot take repair's
    /// floor: turnover gives repair 5 and read-ahead the other 15.
    #[tokio::test]
    async fn borrowing_readahead_cannot_take_repair_floor() {
        let sem = Arc::new(PrioritizedSemaphore::new(20, 5));
        let held: Vec<_> = {
            let mut v = Vec::new();
            for _ in 0..20 {
                v.push(sem.acquire_readahead().await);
            }
            v
        };
        let release = Arc::new(Notify::new());
        let mut tasks = spawn_holders(&sem, PermitTier::ReadAhead, 20, &release);
        settle("read-ahead queued", || {
            sem.waiting(PermitTier::ReadAhead) == 20
        })
        .await;
        tasks.extend(spawn_holders(&sem, PermitTier::Repair, 15, &release));
        settle("repair queued", || sem.waiting(PermitTier::Repair) == 15).await;

        drop(held);
        settle("turnover", || sem.held_total() == 20).await;
        assert_eq!(sem.held(PermitTier::Repair), 5);
        assert_eq!(sem.held(PermitTier::ReadAhead), 15);

        for t in tasks {
            while !t.is_finished() {
                release.notify_waiters();
                tokio::task::yield_now().await;
            }
            t.await.unwrap();
        }
        assert_eq!(sem.held_total(), 0);
    }

    #[tokio::test]
    async fn readahead_leaves_unmet_floor_free_while_repair_active() {
        let sem = PrioritizedSemaphore::new(20, 5);
        let _r = sem.acquire_repair().await;
        // Ingest: free must stay above unmet demand (5) + unmet repair (4).
        let mut ra = Vec::new();
        for _ in 0..10 {
            ra.push(sem.acquire_low().await);
        }
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_low())
                .await
                .is_err(),
            "ingest must not take the unmet repair floor"
        );
        // Read-ahead does not borrow the demand reserve while the repair
        // floor is unmet: the floor must stay in the share.
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_readahead())
                .await
                .is_err(),
            "read-ahead must not take the unmet repair floor"
        );
        // Repair can still reach its floor straight away.
        for _ in 0..4 {
            ra.push(
                timeout(Duration::from_millis(100), sem.acquire_repair())
                    .await
                    .expect("repair floor must be free"),
            );
        }
        // With the floor met, read-ahead may borrow the idle reserve.
        for _ in 0..5 {
            ra.push(
                timeout(Duration::from_millis(100), sem.acquire_readahead())
                    .await
                    .expect("read-ahead may borrow once the floor is met"),
            );
        }
    }

    #[tokio::test]
    async fn idle_repair_floor_costs_readahead_nothing() {
        let sem = PrioritizedSemaphore::new(20, 5);
        let mut ra = Vec::new();
        for _ in 0..20 {
            ra.push(
                timeout(Duration::from_millis(100), sem.acquire_readahead())
                    .await
                    .expect("idle read-ahead gets the whole pool"),
            );
        }
    }

    /// Read-ahead borrows the idle demand reserve, and gives way the moment
    /// demand queues: the next freed permit goes to demand, and read-ahead
    /// stops borrowing while demand waits.
    #[tokio::test]
    async fn readahead_borrows_reserve_until_demand_arrives() {
        let sem = Arc::new(PrioritizedSemaphore::new(4, 1));
        let mut ra = Vec::new();
        for _ in 0..4 {
            ra.push(sem.acquire_readahead().await);
        }
        let demand = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_high().await })
        };
        settle("demand waiter", || sem.waiting(PermitTier::Demand) == 1).await;
        let more_ra = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_readahead().await })
        };
        settle("read-ahead waiter", || {
            sem.waiting(PermitTier::ReadAhead) == 1
        })
        .await;

        drop(ra.pop());
        let permit = timeout(Duration::from_secs(1), demand)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permit.tier(), PermitTier::Demand);
        assert_eq!(sem.waiting(PermitTier::ReadAhead), 1);
        drop(permit);
        let permit = timeout(Duration::from_secs(1), more_ra)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(permit.tier(), PermitTier::ReadAhead);
    }

    /// Prefetch turnover (windows drain, then refill) must not let repair
    /// into the idle demand reserve.
    #[tokio::test]
    async fn readahead_turnover_keeps_demand_reserve_from_repair() {
        let sem = PrioritizedSemaphore::new(20, 5);
        // 20 read-ahead permits: 15 in the share, 5 borrowed from the reserve.
        let mut ra = Vec::new();
        for _ in 0..20 {
            ra.push(sem.acquire_readahead().await);
        }
        // 5 drain with nothing waiting, then 5 refill.
        let mut first: Vec<_> = ra.drain(..5).collect();
        drop(first.drain(..));
        for _ in 0..5 {
            ra.push(sem.acquire_readahead().await);
        }
        // 5 more drain: 15 read-ahead held, the reserve is idle again.
        drop(ra.drain(..5).collect::<Vec<_>>());
        assert_eq!(sem.held_total(), 15);

        // Repair may take only what is left of the non-reserve share: none.
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_repair())
                .await
                .is_err(),
            "repair took a demand reserve permit"
        );
        let demand = timeout(Duration::from_millis(100), sem.acquire_high())
            .await
            .expect("demand must get a reserve permit");
        assert_eq!(demand.tier(), PermitTier::Demand);
        assert_eq!(sem.held(PermitTier::Repair), 0);
    }

    #[tokio::test]
    async fn ingest_never_borrows_demand_reserve() {
        let sem = PrioritizedSemaphore::new(20, 5);
        let mut held = Vec::new();
        for _ in 0..15 {
            held.push(sem.acquire_low().await);
        }
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_low())
                .await
                .is_err()
        );
        // A borrowing read-ahead behind it is not blocked by the ingest head.
        timeout(Duration::from_millis(100), sem.acquire_readahead())
            .await
            .expect("read-ahead may borrow the reserve");
    }

    #[tokio::test]
    async fn repair_cannot_take_demand_reserve() {
        let sem = PrioritizedSemaphore::new(20, 5);
        let mut r = Vec::new();
        for _ in 0..15 {
            r.push(sem.acquire_repair().await);
        }
        assert!(
            timeout(Duration::from_millis(50), sem.acquire_repair())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn dropped_waiter_does_not_leak_a_permit() {
        let sem = Arc::new(PrioritizedSemaphore::new(1, 0));
        let held = sem.acquire_readahead().await;
        let waiter = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_repair().await })
        };
        settle("repair waiter", || sem.waiting(PermitTier::Repair) == 1).await;
        waiter.abort();
        let _ = waiter.await;
        drop(held);
        assert_eq!(sem.held_total(), 0);
        timeout(Duration::from_millis(100), sem.acquire_readahead())
            .await
            .expect("permit must be back in the pool");
    }

    /// No starvation and no deadlock: every tier loops acquire/release
    /// under contention and all finish; the pool is never oversubscribed.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn mixed_contention_makes_progress() {
        let sem = Arc::new(PrioritizedSemaphore::new(8, 2));
        let tiers = [
            PermitTier::Demand,
            PermitTier::ReadAhead,
            PermitTier::Repair,
        ];
        let mut tasks = Vec::new();
        for i in 0..24 {
            let sem = Arc::clone(&sem);
            let tier = tiers[i % tiers.len()];
            tasks.push(tokio::spawn(async move {
                for _ in 0..200 {
                    let permit = sem.acquire(tier).await;
                    assert!(sem.held_total() <= 8);
                    tokio::task::yield_now().await;
                    drop(permit);
                }
            }));
        }
        timeout(Duration::from_secs(20), async {
            for t in tasks {
                t.await.unwrap();
            }
        })
        .await
        .expect("all tiers must finish");
        assert_eq!(sem.held_total(), 0);
    }

    /// Repair keeps making progress while read-ahead is continuously
    /// saturated, and read-ahead keeps making progress under repair
    /// pressure.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn neither_lower_tier_starves_the_other() {
        let sem = Arc::new(PrioritizedSemaphore::new(8, 2));
        let spawn_loop = |tier: PermitTier, workers: usize| {
            (0..workers)
                .map(|_| {
                    let sem = Arc::clone(&sem);
                    tokio::spawn(async move {
                        for _ in 0..100 {
                            let _permit = sem.acquire(tier).await;
                            tokio::time::sleep(Duration::from_millis(1)).await;
                        }
                    })
                })
                .collect::<Vec<_>>()
        };
        let mut tasks = spawn_loop(PermitTier::ReadAhead, 16);
        tasks.extend(spawn_loop(PermitTier::Repair, 16));
        timeout(Duration::from_secs(30), async {
            for t in tasks {
                t.await.unwrap();
            }
        })
        .await
        .expect("read-ahead and repair must both finish");
    }
}
