//! The background PAR2 repair job (WI-161) against a fake backend: a real
//! PAR2 set is built here (index packets and recovery volumes), the release
//! is posted as yEnc articles with some of them missing, and the job repairs
//! or settles it through the hole ledger and the patch store.

use std::sync::Arc;
use std::time::Duration;

use md5::{Digest, Md5};
use nzbdav_core::models::{
    Par2BlockSource, Par2CarriedFile, Par2ProtectedFile, Par2SetRecord, Par2VolumeRecord,
};
use nzbdav_stream::holes::InMemoryHoleLedger;
use nzbdav_stream::provider::{ArticleBackend, HoleConfig};
use nzbdav_stream::testing::{MockArticleBackend, build_yenc_articles};
use nzbdav_stream::{
    MultiSegmentStream, PatchStore, RepairConfig, RepairOutcome, RepairService, RepairTarget,
    Scheduled, SkipReason, UsenetArticleProvider,
};
use rust_par2::SliceHasher;
use tokio::io::AsyncReadExt;
use uuid::Uuid;

const SLICE: usize = 1024;
/// Articles deliberately not aligned with slices.
const PART: usize = 1000;
const FILE_LEN: usize = 40_000;
const SET_ID: [u8; 16] = [0x5e; 16];
const FILE_ID: [u8; 16] = [0xf1; 16];
const NAME: &str = "movie.mkv";

fn packet(kind: &[u8; 16], body: &[u8]) -> Vec<u8> {
    let mut hashed = Vec::with_capacity(32 + body.len());
    hashed.extend_from_slice(&SET_ID);
    hashed.extend_from_slice(kind);
    hashed.extend_from_slice(body);
    let mut out = Vec::with_capacity(64 + body.len());
    out.extend_from_slice(b"PAR2\0PKT");
    out.extend_from_slice(&((64 + body.len()) as u64).to_le_bytes());
    out.extend_from_slice(&Md5::digest(&hashed));
    out.extend_from_slice(&hashed);
    out
}

fn slices(data: &[u8]) -> Vec<&[u8]> {
    data.chunks(SLICE).collect()
}

/// Main, FileDesc and IFSC packets for one file.
fn index_packets(data: &[u8]) -> Vec<u8> {
    let mut main = Vec::new();
    main.extend_from_slice(&(SLICE as u64).to_le_bytes());
    main.extend_from_slice(&1u32.to_le_bytes());
    main.extend_from_slice(&FILE_ID);

    let mut desc = Vec::new();
    desc.extend_from_slice(&FILE_ID);
    desc.extend_from_slice(&Md5::digest(data));
    desc.extend_from_slice(&Md5::digest(&data[..data.len().min(16 * 1024)]));
    desc.extend_from_slice(&(data.len() as u64).to_le_bytes());
    let mut name = NAME.as_bytes().to_vec();
    name.resize(name.len().div_ceil(4) * 4, 0);
    desc.extend_from_slice(&name);

    let mut ifsc = Vec::new();
    ifsc.extend_from_slice(&FILE_ID);
    for slice in slices(data) {
        let sum = SliceHasher::checksum(slice, SLICE as u64);
        ifsc.extend_from_slice(&sum.md5);
        ifsc.extend_from_slice(&sum.crc32.to_le_bytes());
    }

    let mut out = packet(b"PAR 2.0\0Main\0\0\0\0", &main);
    out.extend(packet(b"PAR 2.0\0FileDesc", &desc));
    out.extend(packet(b"PAR 2.0\0IFSC\0\0\0\0", &ifsc));
    out
}

/// A recovery volume holding the rows for `exponents`.
fn volume(data: &[u8], exponents: std::ops::Range<u32>) -> Vec<u8> {
    let inputs = slices(data);
    let constants = rust_par2::matrix::par2_input_constants(inputs.len());
    let mut out = Vec::new();
    for exp in exponents {
        let mut row = vec![0u8; SLICE];
        for (slice, &c) in inputs.iter().zip(&constants) {
            let mut padded = slice.to_vec();
            padded.resize(SLICE, 0);
            rust_par2::gf_simd_public::mul_add_buffer(
                &mut row,
                &padded,
                rust_par2::gf::pow(c, exp),
            );
        }
        let mut body = exp.to_le_bytes().to_vec();
        body.extend_from_slice(&row);
        out.extend(packet(b"PAR 2.0\0RecvSlic", &body));
    }
    out
}

/// Post `data` as articles of `part` bytes, leaving out the indices in
/// `missing`. Returns the message ids.
fn post(
    backend: &mut MockArticleBackend,
    name: &str,
    data: &[u8],
    part: usize,
    missing: &[usize],
) -> Vec<String> {
    let segments: Vec<(u32, u64, usize)> = (0..data.len().div_ceil(part))
        .map(|i| {
            (
                (i + 1) as u32,
                (i * part) as u64,
                part.min(data.len() - i * part),
            )
        })
        .collect();
    let mut ids = Vec::new();
    for (i, article) in build_yenc_articles(name, data, &segments)
        .into_iter()
        .enumerate()
    {
        let id = format!("{name}-{i}@test");
        if !missing.contains(&i) {
            backend.add(id.clone(), article);
        }
        ids.push(id);
    }
    ids
}

struct Fixture {
    service: Arc<RepairService>,
    provider: Arc<UsenetArticleProvider>,
    backend: Arc<MockArticleBackend>,
    patches: Arc<PatchStore>,
    target: RepairTarget,
    data: Vec<u8>,
    data_ids: Vec<String>,
}

/// A 40-slice file with 10 recovery blocks in two volumes of 5, missing the
/// data articles in `missing` and the volume articles in `missing_volume`.
fn fixture(missing: &[usize], missing_volume: &[usize], config: RepairConfig) -> Fixture {
    let data: Vec<u8> = (0..FILE_LEN as u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let mut backend = MockArticleBackend::new();
    let data_ids = post(&mut backend, NAME, &data, PART, missing);
    let vol0 = volume(&data, 0..5);
    let vol1 = volume(&data, 5..10);
    let vol0_ids = post(
        &mut backend,
        "movie.vol00+05.par2",
        &vol0,
        3000,
        missing_volume,
    );
    let vol1_ids = post(&mut backend, "movie.vol05+05.par2", &vol1, 3000, &[]);

    let record = Par2SetRecord {
        recovery_set_id: SET_ID,
        slice_size: SLICE as u64,
        index_packets: index_packets(&data),
        index_segment_ids: Vec::new(),
        files: vec![Par2ProtectedFile {
            file_id: FILE_ID,
            name: NAME.into(),
            size: data.len() as u64,
            carried: Some(Par2CarriedFile {
                subject_name: NAME.into(),
                segment_ids: data_ids.clone(),
                part_size: Some(PART as u64),
            }),
            dav_item_ids: Vec::new(),
        }],
        // Listed out of order: the job reads lowest exponents first.
        volumes: vec![
            Par2VolumeRecord {
                name: "movie.vol05+05.par2".into(),
                first_exponent: Some(5),
                blocks: 5,
                blocks_source: Par2BlockSource::Parsed,
                size: vol1.len() as u64,
                segment_ids: vol1_ids,
            },
            Par2VolumeRecord {
                name: "movie.vol00+05.par2".into(),
                first_exponent: Some(0),
                blocks: 5,
                blocks_source: Par2BlockSource::Parsed,
                size: vol0.len() as u64,
                segment_ids: vol0_ids,
            },
        ],
        part_size: Some(PART as u64),
    };

    let backend = Arc::new(backend.with_delay(Duration::from_millis(1)));
    let patches = Arc::new(PatchStore::in_memory(64 << 20));
    let provider = Arc::new(
        UsenetArticleProvider::with_fake(Arc::clone(&backend) as Arc<dyn ArticleBackend>)
            .with_ledger(Arc::new(InMemoryHoleLedger::new(Duration::ZERO)))
            .with_hole_config(HoleConfig {
                confirm_delay: Duration::ZERO,
                confirm_fetch: Duration::from_secs(5),
            })
            .with_patches(Arc::clone(&patches)),
    );
    let service = Arc::new(RepairService::new(
        Arc::clone(&provider) as _,
        provider.ledger(),
        Arc::clone(&patches),
        config,
    ));
    let target = RepairTarget {
        key: "release:set".into(),
        release: Uuid::from_u128(42),
        nzb_blob_id: Some(Uuid::from_u128(43)),
        record,
    };
    Fixture {
        service,
        provider,
        backend,
        patches,
        target,
        data,
        data_ids,
    }
}

async fn run(fx: &Fixture) -> RepairOutcome {
    match fx.service.schedule(fx.target.clone()) {
        Scheduled::Queued(mut rx) => rx.wait_for(Option::is_some).await.unwrap().clone().unwrap(),
        other => panic!("expected a queued job, got {other:?}"),
    }
}

async fn read_file(fx: &Fixture) -> std::io::Result<Vec<u8>> {
    let mut stream = MultiSegmentStream::new(Arc::clone(&fx.provider), fx.data_ids.clone(), 4);
    let mut out = Vec::new();
    stream.read_to_end(&mut out).await?;
    Ok(out)
}

/// Acceptance: 3 missing articles, 10 recovery blocks. The job repairs the
/// release and later reads return the original bytes.
#[tokio::test]
async fn three_missing_articles_are_repaired_and_read_back() {
    let fx = fixture(&[3, 17, 39], &[], RepairConfig::default());
    assert!(
        read_file(&fx).await.is_err(),
        "holes fail the read before repair"
    );

    let outcome = run(&fx).await;
    let RepairOutcome::Repaired { slices, bytes_kept } = outcome else {
        panic!("expected a repair, got {outcome:?}");
    };
    assert!(slices >= 3, "{slices}");
    // Only the lost articles' spans are kept.
    assert_eq!(bytes_kept, 3 * PART as u64);
    assert_eq!(fx.patches.totals().spans, 3);
    let stamp = fx.patches.stamp(fx.target.release).unwrap();
    assert_eq!(stamp.nzb_blob_id, fx.target.nzb_blob_id);
    assert_eq!(
        fx.patches.holes(fx.target.release).len(),
        3,
        "holes recorded"
    );
    assert!(fx.patches.verdict(fx.target.release).is_none());

    assert_eq!(read_file(&fx).await.unwrap(), fx.data);
}

/// Acceptance: 12 missing articles, 10 recovery blocks. The job records a
/// terminal verdict with the set's shape and does not try again.
#[tokio::test]
async fn twelve_missing_with_ten_blocks_settles_and_is_not_retried() {
    let missing: Vec<usize> = (0..12).map(|i| i * 3).collect();
    let fx = fixture(&missing, &[], RepairConfig::default());

    let outcome = run(&fx).await;
    let RepairOutcome::Unrepairable { diagnosis } = outcome else {
        panic!("expected a verdict, got {outcome:?}");
    };
    assert!(diagnosis.contains("10 recovery blocks"), "{diagnosis}");
    assert!(
        diagnosis.contains(
            "the recovery set covers 1 files of 1 MiB and this document carries 1 of them"
        ),
        "{diagnosis}"
    );
    let verdict = fx
        .patches
        .verdict(fx.target.release)
        .expect("verdict persisted");
    assert_eq!(verdict.diagnosis, diagnosis);
    assert_eq!(fx.patches.holes(fx.target.release).len(), 12);
    assert_eq!(fx.patches.totals().spans, 0, "nothing patched");

    let fetches = fx.backend.fetch_count();
    match fx.service.schedule(fx.target.clone()) {
        Scheduled::Skipped(SkipReason::Settled(v)) => assert_eq!(v, verdict),
        other => panic!("expected the verdict to hold, got {other:?}"),
    }
    assert_eq!(fx.backend.fetch_count(), fetches, "no retry");
}

/// A volume that is not read to its end never yields a verdict: its unread
/// rows might have covered the damage. The attempt fails and cools down.
#[tokio::test]
async fn unread_volume_never_settles_a_release() {
    let missing: Vec<usize> = (0..12).map(|i| i * 3).collect();
    let fx = fixture(&missing, &[1], RepairConfig::default());

    let outcome = run(&fx).await;
    assert!(
        matches!(outcome, RepairOutcome::Failed { .. }),
        "expected a failed attempt, got {outcome:?}"
    );
    assert!(fx.patches.verdict(fx.target.release).is_none());
    match fx.service.schedule(fx.target.clone()) {
        Scheduled::Skipped(SkipReason::CoolingDown(_)) => {}
        other => panic!("expected a cooldown, got {other:?}"),
    }
}

/// Memory stays bounded: no more articles in flight than the prefetch
/// bound, whatever the damage.
#[tokio::test]
async fn reads_stay_within_the_prefetch_bound() {
    let config = RepairConfig {
        prefetch_slices: 3,
        ..RepairConfig::default()
    };
    let fx = fixture(&[5, 6], &[], config);
    let outcome = run(&fx).await;
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert!(
        fx.backend.peak_in_flight() <= 3,
        "{}",
        fx.backend.peak_in_flight()
    );
    assert_eq!(read_file(&fx).await.unwrap(), fx.data);
}

/// A second report for a set with a job folds into it; with repair
/// switched off nothing is scheduled.
#[tokio::test]
async fn one_job_per_set_and_the_switch() {
    let fx = fixture(&[8], &[], RepairConfig::default());
    let Scheduled::Queued(mut first) = fx.service.schedule(fx.target.clone()) else {
        panic!("expected a queued job");
    };
    let Scheduled::Folded(mut second) = fx.service.schedule(fx.target.clone()) else {
        panic!("expected the second report to fold in");
    };
    let a = first.wait_for(Option::is_some).await.unwrap().clone();
    let b = second.wait_for(Option::is_some).await.unwrap().clone();
    assert_eq!(a, b);
    assert!(matches!(a, Some(RepairOutcome::Repaired { .. })), "{a:?}");
    assert!(fx.service.signal(&fx.target.key).is_none(), "job done");

    let off = fixture(
        &[8],
        &[],
        RepairConfig {
            enabled: false,
            ..RepairConfig::default()
        },
    );
    assert!(matches!(
        off.service.schedule(off.target.clone()),
        Scheduled::Skipped(SkipReason::Disabled)
    ));
}
