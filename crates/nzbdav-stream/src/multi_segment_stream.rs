//! `MultiSegmentStream` — an `AsyncRead` that fetches Usenet articles in
//! sequential order with bounded lookahead prefetching.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::task::{Context, Poll};

use bytes::{Buf, BytesMut};
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::stream::FuturesOrdered;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::{mpsc, watch};
use tracing::{debug, error};

use crate::error::StreamError;
use crate::provider::UsenetArticleProvider;

/// An `AsyncRead` stream that fetches and decodes articles in order.
///
/// On creation a background tokio task is spawned that fetches segments
/// through the provider and sends decoded payloads through a bounded
/// channel. The channel capacity equals `lookahead`, providing natural
/// backpressure: the fetcher won't race too far ahead of the reader.
///
/// Fetches go through the provider's hole model, so a segment every pool
/// refuses holds the reader for the confirming re-ask rather than failing
/// at once; [`held_provisionally`](Self::held_provisionally) reports that.
pub struct MultiSegmentStream {
    receiver: mpsc::Receiver<Result<Vec<u8>, StreamError>>,
    buffer: BytesMut,
    bytes_read: Arc<AtomicU64>,
    done: bool,
    /// Index of the segment the reader needs next; segments still waiting
    /// for a permit are promoted to demand when it reaches them.
    head: watch::Sender<usize>,
    provider: Arc<UsenetArticleProvider>,
    segment_ids: Arc<[String]>,
    /// Segments handed to the reader so far; the next one is what it waits on.
    delivered: usize,
}

impl MultiSegmentStream {
    /// Create a new stream that will fetch the given segment message-ids
    /// in order.
    ///
    /// * `provider` — article fetch + decode backend
    /// * `segment_ids` — ordered list of article message-ids
    /// * `lookahead` — how many segments to prefetch ahead of the reader
    pub fn new(
        provider: Arc<UsenetArticleProvider>,
        segment_ids: Vec<String>,
        lookahead: usize,
    ) -> Self {
        let (tx, rx) = mpsc::channel(lookahead);
        let bytes_read = Arc::new(AtomicU64::new(0));
        let ids: Arc<[String]> = segment_ids.clone().into();
        let reader_provider = Arc::clone(&provider);

        let segment_count = segment_ids.len();
        // Parallel, in-order prefetch. Up to `lookahead` fetches can be
        // in flight at once; FuturesOrdered yields results in the original
        // submission order, preserving the stream's byte ordering.
        //
        // Only the head of the window — the segment the reader needs next
        // (it advances as the reader consumes segments) — is demand.
        // Everything beyond it waits at the read-ahead tier and
        // is promoted to demand if the head reaches it before it gets a
        // permit, so read-ahead never outranks repair.
        let concurrency = lookahead.max(1);
        let (head_tx, head_rx) = watch::channel(0usize);
        let spawn_result = tokio::spawn(async move {
            debug!(
                segments = segment_count,
                concurrency, "prefetch task started"
            );
            let mut pending: FuturesOrdered<BoxFuture<'static, Result<Vec<u8>, StreamError>>> =
                FuturesOrdered::new();
            let mut next_to_spawn = 0usize;
            let spawn_one = |idx: usize,
                             provider: &Arc<UsenetArticleProvider>,
                             segment_ids: &Vec<String>|
             -> BoxFuture<'static, Result<Vec<u8>, StreamError>> {
                let p = Arc::clone(provider);
                let mid = segment_ids[idx].clone();
                let mut head = head_rx.clone();
                Box::pin(async move {
                    let promote = async move {
                        // A closed channel means the stream is shutting
                        // down; stay at read-ahead.
                        if head.wait_for(|&h| h >= idx).await.is_err() {
                            std::future::pending::<()>().await;
                        }
                    };
                    match p.fetch_decoded_promotable(&mid, promote).await {
                        // A confirmed hole the repair job rebuilt.
                        Err(StreamError::ArticleNotFound(e)) => match p.patched(&mid).await {
                            Some(bytes) => Ok(bytes),
                            None => Err(StreamError::ArticleNotFound(e)),
                        },
                        other => other,
                    }
                })
            };

            while next_to_spawn < segment_ids.len() && pending.len() < concurrency {
                pending.push_back(spawn_one(next_to_spawn, &provider, &segment_ids));
                next_to_spawn += 1;
            }

            while let Some(result) = pending.next().await {
                if tx.send(result).await.is_err() {
                    debug!("receiver dropped, stopping prefetch");
                    return;
                }
                if next_to_spawn < segment_ids.len() {
                    pending.push_back(spawn_one(next_to_spawn, &provider, &segment_ids));
                    next_to_spawn += 1;
                }
            }
            debug!(segments = segment_count, "prefetch task finished");
        });
        debug!(
            spawned = spawn_result.is_finished(),
            "MultiSegmentStream background task spawned"
        );

        Self {
            receiver: rx,
            buffer: BytesMut::new(),
            bytes_read,
            done: false,
            head: head_tx,
            provider: reader_provider,
            segment_ids: ids,
            delivered: 0,
        }
    }

    /// Whether the reader is blocked on a segment held on a provisional
    /// refusal while its confirm is in flight. Such a reader must be left
    /// out of any hedging or second-ask logic: it is already being re-asked.
    pub fn held_provisionally(&self) -> bool {
        !self.done
            && self.buffer.is_empty()
            && self
                .segment_ids
                .get(self.delivered)
                .is_some_and(|id| self.provider.held_provisionally(id))
    }

    /// Total bytes that have been read out of this stream so far.
    pub fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }
}

impl AsyncRead for MultiSegmentStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // If we have buffered data, copy it out first.
        if !self.buffer.is_empty() {
            let to_copy = self.buffer.len().min(buf.remaining());
            buf.put_slice(&self.buffer[..to_copy]);
            self.buffer.advance(to_copy);
            self.bytes_read.fetch_add(to_copy as u64, Ordering::Relaxed);
            return Poll::Ready(Ok(()));
        }

        // If we already know we're done, signal EOF.
        if self.done {
            return Poll::Ready(Ok(()));
        }

        // Try to receive the next segment from the prefetch task.
        match self.receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok(data))) => {
                self.head.send_modify(|h| *h += 1);
                self.delivered += 1;
                let to_copy = data.len().min(buf.remaining());
                buf.put_slice(&data[..to_copy]);
                self.bytes_read.fetch_add(to_copy as u64, Ordering::Relaxed);
                // Buffer any remainder.
                if to_copy < data.len() {
                    self.buffer.extend_from_slice(&data[to_copy..]);
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Some(Err(e))) => {
                error!(error = %e, "segment fetch error");
                self.done = true;
                Poll::Ready(Err(std::io::Error::other(e)))
            }
            Poll::Ready(None) => {
                // Channel closed — all segments sent.
                self.done = true;
                Poll::Ready(Ok(()))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prioritized_semaphore::PermitTier;
    use crate::testing::{MockArticleBackend, build_yenc_articles};
    use std::time::Duration;
    use tokio::io::AsyncReadExt;

    /// Stream reassembly must match the source file byte-for-byte.
    #[tokio::test]
    async fn multi_segment_stream_reassembles_file_exactly() {
        let data: Vec<u8> = (0..16384u32).flat_map(|n| n.to_le_bytes()).collect();
        let (provider, mids, original) =
            crate::testing::mock_provider_for_file("test.bin", data, 4);
        let mut stream = MultiSegmentStream::new(provider, mids, 4);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, original);
    }

    /// A provider over a 4-segment file whose segment 1 every server
    /// refuses, confirming holes on the first refusal, with `patches`.
    fn holed_provider(
        patches: Option<Arc<crate::patches::PatchStore>>,
    ) -> (
        Arc<UsenetArticleProvider>,
        Vec<String>,
        Vec<u8>,
        Arc<MockArticleBackend>,
    ) {
        let data: Vec<u8> = (0..4096u32).map(|n| (n % 251) as u8).collect();
        let parts: Vec<(u32, u64, usize)> = (0..4)
            .map(|i| (i as u32 + 1, i as u64 * 1024, 1024))
            .collect();
        let articles = build_yenc_articles("h.bin", &data, &parts);
        let mids: Vec<String> = (0..4).map(|i| format!("h{i}@t.test")).collect();
        let mut backend = MockArticleBackend::new();
        for (i, (mid, art)) in mids.iter().zip(articles).enumerate() {
            if i != 1 {
                backend.add(mid.clone(), art);
            }
        }
        let backend = Arc::new(backend);
        let mut provider = UsenetArticleProvider::with_fake(Arc::clone(&backend) as _)
            .with_ledger(Arc::new(crate::holes::InMemoryHoleLedger::new(
                Duration::ZERO,
            )))
            .with_hole_config(crate::provider::HoleConfig {
                confirm_delay: Duration::ZERO,
                confirm_fetch: Duration::from_secs(5),
            });
        if let Some(p) = patches {
            provider = provider.with_patches(p);
        }
        (Arc::new(provider), mids, data, backend)
    }

    fn holed_file(mids: &[String]) -> crate::census::CensusFile {
        crate::census::CensusFile {
            name: "h.bin".into(),
            articles: mids
                .iter()
                .enumerate()
                .map(|(i, m)| crate::census::ArticleSpan {
                    message_id: m.clone(),
                    offset: i as u64 * 1024,
                    len: 1024,
                })
                .collect(),
        }
    }

    /// Without a patch, a confirmed hole fails the read as it always has.
    #[tokio::test]
    async fn hole_without_patch_fails_the_read() {
        for patches in [
            None,
            Some(Arc::new(crate::patches::PatchStore::in_memory(1 << 20))),
        ] {
            let (provider, mids, _, _) = holed_provider(patches);
            let mut stream = MultiSegmentStream::new(provider, mids, 4);
            let mut out = Vec::new();
            let err = stream.read_to_end(&mut out).await.unwrap_err();
            assert!(err.to_string().contains("h1@t.test"), "{err}");
        }
    }

    /// A read over a confirmed hole with a patch returns the patched bytes,
    /// with no fetch for the hole once it is confirmed.
    #[tokio::test]
    async fn hole_with_patch_serves_patched_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let release = uuid::Uuid::from_u128(7);
        let stamp = crate::patches::PatchStamp {
            nzb_blob_id: None,
            server_fingerprint: "fake-0".into(),
        };
        let (_, mids, data, _) = holed_provider(None);
        {
            let store = crate::patches::PatchStore::open(dir.path(), 1 << 20).unwrap();
            store
                .insert(
                    release,
                    &stamp,
                    &holed_file(&mids),
                    1024,
                    data[1024..2048].to_vec(),
                )
                .await
                .unwrap();
        }
        // A new process: the patch comes back from disk.
        let store = Arc::new(crate::patches::PatchStore::open(dir.path(), 1 << 20).unwrap());
        let (provider, mids, data, _) = holed_provider(Some(Arc::clone(&store)));
        for _ in 0..2 {
            let mut stream = MultiSegmentStream::new(Arc::clone(&provider), mids.clone(), 4);
            let mut out = Vec::new();
            stream.read_to_end(&mut out).await.unwrap();
            assert_eq!(out, data);
        }
        assert_eq!(
            provider.ledger().state("h1@t.test"),
            Some(crate::holes::HoleState::Confirmed)
        );

        // Deleting the release takes the patch away again.
        store.drop_release(release);
        let mut stream = MultiSegmentStream::new(provider, mids, 4);
        assert!(stream.read_to_end(&mut Vec::new()).await.is_err());
    }

    fn holed_stamp() -> crate::patches::PatchStamp {
        crate::patches::PatchStamp {
            nzb_blob_id: Some(uuid::Uuid::from_u128(1)),
            server_fingerprint: "fake-0".into(),
        }
    }

    /// A read through `DavMultipartFileStream` over a patched hole: the
    /// file parts map virtual ranges onto segment data, and the hole's bytes
    /// come from the patch, for a straight read and after a seek into it.
    #[tokio::test]
    async fn multipart_read_over_patched_hole() {
        use nzbdav_core::models::{FilePart, LongRange};
        use tokio::io::AsyncSeekExt;

        let store = Arc::new(crate::patches::PatchStore::in_memory(1 << 20));
        let (provider, mids, data, _) = holed_provider(Some(Arc::clone(&store)));
        store
            .insert(
                uuid::Uuid::from_u128(7),
                &holed_stamp(),
                &holed_file(&mids),
                1024,
                data[1024..2048].to_vec(),
            )
            .await
            .unwrap();
        // The virtual file starts 512 bytes into segment 0; segment 1 is the
        // hole.
        let parts = vec![
            FilePart {
                segment_ids: mids[..2].to_vec(),
                segment_id_byte_range: LongRange::new(512, 2048),
                file_part_byte_range: LongRange::new(0, 1536),
            },
            FilePart {
                segment_ids: mids[2..].to_vec(),
                segment_id_byte_range: LongRange::new(0, 2048),
                file_part_byte_range: LongRange::new(1536, 3584),
            },
        ];
        let mut stream = crate::multipart_file_stream::DavMultipartFileStream::new(
            Arc::clone(&provider),
            parts.clone(),
            3584,
            4,
        );
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data[512..]);

        let mut stream =
            crate::multipart_file_stream::DavMultipartFileStream::new(provider, parts, 3584, 4);
        stream.seek(std::io::SeekFrom::Start(600)).await.unwrap();
        let mut buf = vec![0u8; 100];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, data[1112..1212]);
    }

    /// The hole and its patch outlive the process: after a restart the
    /// ledger knows the hole before any read, and the read is served from
    /// the patch without asking the servers for the hole again.
    #[tokio::test]
    async fn recorded_hole_and_patch_need_no_server_after_restart() {
        let dir = tempfile::tempdir().unwrap();
        let release = uuid::Uuid::from_u128(7);
        {
            let store = Arc::new(crate::patches::PatchStore::open(dir.path(), 1 << 20).unwrap());
            let (provider, mids, data, _) = holed_provider(Some(Arc::clone(&store)));
            let mut stream = MultiSegmentStream::new(Arc::clone(&provider), mids.clone(), 4);
            assert!(stream.read_to_end(&mut Vec::new()).await.is_err());
            store
                .insert(
                    release,
                    &holed_stamp(),
                    &holed_file(&mids),
                    1024,
                    data[1024..2048].to_vec(),
                )
                .await
                .unwrap();
            let recorded = store
                .record_holes(release, &holed_stamp(), &provider.ledger().holes())
                .unwrap();
            assert_eq!(recorded, 1);
        }

        let store = Arc::new(crate::patches::PatchStore::open(dir.path(), 1 << 20).unwrap());
        let (provider, mids, data, backend) = holed_provider(Some(store));
        assert_eq!(
            provider.ledger().state("h1@t.test"),
            Some(crate::holes::HoleState::Confirmed),
            "restored before any read"
        );
        let mut stream = MultiSegmentStream::new(provider, mids, 4);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);
        assert_eq!(backend.fetch_count(), 3, "the hole is not fetched");
    }

    /// The prefetch loop should fetch segments concurrently up to the
    /// lookahead window. Today it does not — one fetch at a time — so peak
    /// in-flight is 1. A fix using `FuturesOrdered` should raise this to
    /// roughly `lookahead`.
    #[tokio::test]
    async fn multi_segment_fetches_in_parallel_up_to_lookahead() {
        // Use a deliberate delay so sequential fetches take meaningful time
        // while parallel fetches can overlap.
        let data = vec![0u8; 4096];
        let segments = &[
            (1u32, 0u64, 1024),
            (2, 1024, 1024),
            (3, 2048, 1024),
            (4, 3072, 1024),
        ];
        let articles = build_yenc_articles("f.bin", &data, segments);
        let mids: Vec<String> = (0..articles.len())
            .map(|i| format!("m{i}@t.test"))
            .collect();

        let mut backend = MockArticleBackend::new().with_delay(Duration::from_millis(50));
        for (mid, art) in mids.iter().zip(articles) {
            backend.add(mid.clone(), art);
        }
        let backend_arc: std::sync::Arc<MockArticleBackend> = std::sync::Arc::new(backend);

        // Wrap the Arc in a provider via a small shim backend.
        struct Shim(std::sync::Arc<MockArticleBackend>);
        #[async_trait::async_trait]
        impl crate::provider::ArticleBackend for Shim {
            async fn fetch_decoded(&self, m: &str) -> Result<Vec<u8>, StreamError> {
                self.0.fetch_decoded(m).await
            }
            async fn stat(&self, m: &str) -> Result<bool, StreamError> {
                self.0.stat(m).await
            }
            async fn yenc_headers(
                &self,
                m: &str,
            ) -> Result<crate::provider::YencHeaders, StreamError> {
                self.0.yenc_headers(m).await
            }
            fn total_connections(&self) -> usize {
                self.0.total_connections()
            }
        }
        let provider = std::sync::Arc::new(crate::provider::UsenetArticleProvider::with_fake(
            std::sync::Arc::new(Shim(std::sync::Arc::clone(&backend_arc))),
        ));

        // lookahead=4, so all 4 segments should be able to be in flight at once.
        let mut stream = MultiSegmentStream::new(provider, mids, 4);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();

        assert_eq!(out.len(), 4096);
        let peak = backend_arc.peak_in_flight();
        assert!(
            peak >= 2,
            "expected >=2 concurrent fetches with lookahead=4, saw peak={peak}"
        );
    }

    /// Mock backend whose fetches each wait for a gate permit, on a pool of
    /// `connections`.
    struct Gated {
        inner: MockArticleBackend,
        gate: tokio::sync::Semaphore,
        connections: usize,
        /// Fetches that reached the backend (holding a permit).
        entered: AtomicU64,
    }

    #[async_trait::async_trait]
    impl crate::provider::ArticleBackend for Gated {
        async fn fetch_decoded(&self, m: &str) -> Result<Vec<u8>, StreamError> {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.gate.acquire().await.unwrap().forget();
            self.inner.fetch_decoded(m).await
        }
        async fn stat(&self, m: &str) -> Result<bool, StreamError> {
            self.inner.stat(m).await
        }
        async fn yenc_headers(&self, m: &str) -> Result<crate::provider::YencHeaders, StreamError> {
            self.inner.yenc_headers(m).await
        }
        fn total_connections(&self) -> usize {
            self.connections
        }
    }

    fn gated_provider(
        segments: usize,
        connections: usize,
    ) -> (Arc<UsenetArticleProvider>, Arc<Gated>, Vec<String>, Vec<u8>) {
        let data: Vec<u8> = (0..segments * 512).map(|i| (i % 251) as u8).collect();
        let parts: Vec<(u32, u64, usize)> = (0..segments)
            .map(|i| (i as u32 + 1, (i * 512) as u64, 512))
            .collect();
        let articles = build_yenc_articles("g.bin", &data, &parts);
        let mids: Vec<String> = (0..segments).map(|i| format!("g{i}@t.test")).collect();
        let mut inner = MockArticleBackend::new();
        for (mid, art) in mids.iter().zip(articles) {
            inner.add(mid.clone(), art);
        }
        let gated = Arc::new(Gated {
            inner,
            gate: tokio::sync::Semaphore::new(0),
            connections,
            entered: AtomicU64::new(0),
        });
        let provider = Arc::new(UsenetArticleProvider::with_fake(
            Arc::clone(&gated) as Arc<dyn crate::provider::ArticleBackend>
        ));
        (provider, gated, mids, data)
    }

    async fn settle(what: &str, cond: impl Fn() -> bool) {
        for _ in 0..2000 {
            if cond() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        panic!("timed out waiting for: {what}");
    }

    /// The segment the reader needs next is fetched at demand; the rest of
    /// the lookahead window is fetched at read-ahead.
    #[tokio::test]
    async fn head_is_demand_and_window_is_readahead() {
        let (provider, gated, mids, data) = gated_provider(6, 4);
        let sem = provider.priority_semaphore();
        let mut stream = MultiSegmentStream::new(Arc::clone(&provider), mids, 4);

        settle("window in flight", || sem.held_total() == 4).await;
        assert_eq!(sem.held(PermitTier::Demand), 1);
        assert_eq!(sem.held(PermitTier::ReadAhead), 3);
        assert_eq!(sem.held(PermitTier::Repair), 0);

        gated.gate.add_permits(64);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);
        assert_eq!(sem.held_total(), 0);
    }

    /// A read-ahead fetch still waiting for a permit is promoted to demand
    /// once the reader reaches it, so playback is never stuck behind
    /// read-ahead (or repair) for the segment it needs.
    #[tokio::test]
    async fn waiting_readahead_is_promoted_when_reader_reaches_it() {
        let (provider, gated, mids, data) = gated_provider(3, 4);
        let sem = provider.priority_semaphore();
        // Take three of the four permits so segments 1 and 2 must wait.
        let mut others = Vec::new();
        for _ in 0..3 {
            others.push(sem.acquire_readahead().await);
        }
        let mut stream = MultiSegmentStream::new(Arc::clone(&provider), mids, 3);
        settle("head on demand", || {
            sem.held(PermitTier::Demand) == 1 && sem.waiting(PermitTier::ReadAhead) == 2
        })
        .await;

        // Someone else's demand queues, so segment 0's permit goes there
        // rather than being borrowed by read-ahead.
        let outside = {
            let sem = Arc::clone(&sem);
            tokio::spawn(async move { sem.acquire_high().await })
        };
        settle("outside demand queued", || {
            sem.waiting(PermitTier::Demand) == 1
        })
        .await;

        // Finish and read segment 0: the reader now needs segment 1, which
        // is promoted to demand while still waiting.
        gated.gate.add_permits(1);
        let mut first = vec![0u8; 512];
        stream.read_exact(&mut first).await.unwrap();
        assert_eq!(first, data[..512]);
        settle("segment 1 promoted", || {
            sem.waiting(PermitTier::Demand) == 1
        })
        .await;
        assert_eq!(sem.waiting(PermitTier::ReadAhead), 1);

        // The outside permit frees: it goes to segment 1 at demand, not to
        // the read-ahead segment 2.
        drop(outside.await.unwrap());
        settle("segment 1 granted", || sem.held(PermitTier::Demand) == 1).await;
        assert_eq!(sem.held(PermitTier::ReadAhead), 3);
        assert_eq!(sem.waiting(PermitTier::ReadAhead), 1);

        drop(others);
        gated.gate.add_permits(64);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data[512..]);
        assert_eq!(sem.held_total(), 0);
    }

    /// A lone stream on an idle 20-connection pool uses all 20 connections:
    /// read-ahead borrows the unused demand reserve.
    #[tokio::test]
    async fn lone_stream_uses_whole_idle_pool() {
        let (provider, gated, mids, data) = gated_provider(40, 20);
        let sem = provider.priority_semaphore();
        let mut stream = MultiSegmentStream::new(Arc::clone(&provider), mids, 40);

        settle("pool saturated", || sem.held_total() == 20).await;
        assert_eq!(sem.held(PermitTier::Demand), 1);
        assert_eq!(sem.held(PermitTier::ReadAhead), 19);
        assert_eq!(gated.entered.load(Ordering::SeqCst), 20);

        gated.gate.add_permits(1024);
        let mut out = Vec::new();
        stream.read_to_end(&mut out).await.unwrap();
        assert_eq!(out, data);
        assert_eq!(sem.held_total(), 0);
    }
}
