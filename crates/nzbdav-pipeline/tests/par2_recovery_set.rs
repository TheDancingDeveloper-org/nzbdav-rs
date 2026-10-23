//! Import records each release's PAR2 recovery sets (WI-159).
//!
//! Runs the full queue item pipeline against a mock article backend and an
//! in-memory SQLite database, with real PAR2 files (see
//! `tests/fixtures/par2/README.md`).

use std::path::Path;
use std::sync::Arc;

use chrono::Utc;
use nzbdav_core::database::DavDatabase;
use nzbdav_core::models::{DavItem, Par2BlockSource, Par2SetRecord, QueueItem};
use nzbdav_core::seed::seed_root_items;
use nzbdav_core::sqlite_db::SqliteDavDatabase;
use nzbdav_pipeline::par2_sets::parse_set_record;
use nzbdav_pipeline::queue_item_processor::{PipelineConfig, QueueItemProcessor};
use nzbdav_stream::census::Par2RecoverySet;
use nzbdav_stream::load_recovery_sets;
use nzbdav_stream::testing::{MockArticleBackend, build_yenc_articles};
use parking_lot::Mutex;
use uuid::Uuid;

const PART: usize = 20_000;

fn fixture(name: &str) -> Vec<u8> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/par2")
        .join(name);
    std::fs::read(&path).unwrap_or_else(|e| panic!("{}: {e}", path.display()))
}

/// A release under construction: files posted as articles of `PART` bytes.
struct Release {
    backend: MockArticleBackend,
    nzb: String,
}

impl Release {
    fn new() -> Self {
        Self {
            backend: MockArticleBackend::new(),
            nzb: String::new(),
        }
    }

    /// Post `data` under `name`; returns its message ids.
    fn post(&mut self, name: &str, data: &[u8]) -> Vec<String> {
        let segments: Vec<(u32, u64, usize)> = (0..data.len().div_ceil(PART))
            .map(|i| {
                let offset = i * PART;
                ((i + 1) as u32, offset as u64, PART.min(data.len() - offset))
            })
            .collect();
        let total = segments.len();
        let mut xml = format!(
            "<file poster=\"p@x\" date=\"100\" subject=\"&quot;{name}&quot; yEnc (1/{total})\">\
             <groups><group>alt.test</group></groups><segments>"
        );
        let mut ids = Vec::new();
        for (article, &(part, _, len)) in build_yenc_articles(name, data, &segments)
            .into_iter()
            .zip(&segments)
        {
            let id = format!("{name}-{part}@test");
            xml.push_str(&format!(
                "<segment number=\"{part}\" bytes=\"{len}\">{id}</segment>"
            ));
            self.backend.add(id.clone(), article);
            ids.push(id);
        }
        xml.push_str("</segments></file>");
        self.nzb.push_str(&xml);
        ids
    }

    fn nzb(&self) -> Vec<u8> {
        format!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
             <nzb xmlns=\"http://www.newzbin.com/DTD/2003/nzb\">{}</nzb>",
            self.nzb
        )
        .into_bytes()
    }
}

struct Imported {
    db: SqliteDavDatabase,
    history_id: Uuid,
    job_dir_id: Uuid,
}

async fn import(release: Release) -> Imported {
    let nzb = release.nzb();
    let conn = nzbdav_core::db::open(":memory:").unwrap();
    let db = SqliteDavDatabase::new(Arc::new(Mutex::new(conn)));
    seed_root_items(&db).await.unwrap();

    let queue_item = QueueItem {
        id: Uuid::new_v4(),
        created_at: Utc::now().naive_utc(),
        file_name: "release.nzb".into(),
        job_name: "release".into(),
        nzb_file_size: nzb.len() as i64,
        total_segment_bytes: 0,
        category: String::new(),
        priority: 0,
        post_processing: -1,
        pause_until: None,
    };
    let processor = QueueItemProcessor::new(
        release.backend.into_provider(),
        PipelineConfig {
            ensure_importable_video: false,
            ..PipelineConfig::default()
        },
    );
    let result = processor.process(&db, &queue_item, &nzb).await.unwrap();
    Imported {
        db,
        history_id: queue_item.id,
        job_dir_id: result.job_dir_id,
    }
}

async fn item(db: &SqliteDavDatabase, job_dir_id: Uuid, name: &str) -> DavItem {
    db.get_dav_children(job_dir_id)
        .await
        .unwrap()
        .into_iter()
        .find(|i| i.name == name)
        .unwrap_or_else(|| panic!("no DAV item {name}"))
}

#[tokio::test]
async fn import_records_set_volumes_and_file_mapping() {
    let data = fixture("testdata.bin");
    let mut release = Release::new();
    let data_ids = release.post("testdata.bin", &data);
    let index_ids = release.post("testdata.bin.par2", &fixture("testdata.bin.par2"));
    let volume_ids = release.post(
        "testdata.bin.vol0+1.par2",
        &fixture("testdata.bin.vol0+1.par2"),
    );
    let imported = import(release).await;
    let db = &imported.db;

    let sets = db.get_par2_sets(imported.history_id).await.unwrap();
    assert_eq!(sets.len(), 1);
    let set = &sets[0];
    assert_eq!(
        hex(&set.recovery_set_id),
        "eab6403859a33ba2af446b0297d24aa4"
    );
    assert_eq!(set.index_segment_ids, index_ids);
    assert_eq!(set.part_size, Some(PART as u64));

    // The protected file maps to the NZB file's articles and its DAV item.
    let data_item = item(db, imported.job_dir_id, "testdata.bin").await;
    assert_eq!(set.files.len(), 1);
    let file = &set.files[0];
    assert_eq!(file.name, "testdata.bin");
    assert_eq!(file.size, data.len() as u64);
    let carried = file.carried.as_ref().expect("file is carried");
    assert_eq!(carried.segment_ids, data_ids);
    assert_eq!(carried.part_size, Some(PART as u64));
    assert_eq!(file.dav_item_ids, vec![data_item.id]);

    // And the reverse.
    let refs = db.get_par2_files_for_dav_item(data_item.id).await.unwrap();
    assert_eq!(refs.len(), 1);
    assert_eq!(refs[0].history_item_id, imported.history_id);
    assert_eq!(refs[0].recovery_set_id, set.recovery_set_id);
    assert_eq!(refs[0].file_id, file.file_id);

    // The recovery volume: its articles and blocks, but no DAV item.
    assert_eq!(set.volumes.len(), 1);
    let volume = &set.volumes[0];
    assert_eq!(volume.name, "testdata.bin.vol0+1.par2");
    assert_eq!(volume.segment_ids, volume_ids);
    assert_eq!(volume.first_exponent, Some(0));
    assert_eq!(volume.blocks, 1);
    assert_eq!(volume.blocks_source, Par2BlockSource::FileName);
    assert_eq!(set.recovery_blocks(), 1);
    let children = db.get_dav_children(imported.job_dir_id).await.unwrap();
    assert!(
        children.iter().all(|c| !c.name.ends_with(".par2")),
        "PAR2 files are not DAV items: {:?}",
        children.iter().map(|c| &c.name).collect::<Vec<_>>()
    );

    // The stored packets parse back to the set, slice checksums included.
    let parsed = parse_set_record(set).expect("index packets parse");
    assert_eq!(parsed.recovery_set_id, set.recovery_set_id);
    assert_eq!(parsed.slice_size, set.slice_size);
    let slices = data.len().div_ceil(set.slice_size as usize);
    assert_eq!(
        parsed.files.values().next().unwrap().slices.len(),
        slices,
        "IFSC checksums kept"
    );
    assert!(set.index_packets.len() <= fixture("testdata.bin.par2").len());

    // The census loads the same set.
    let census_sets: Vec<Par2RecoverySet> =
        load_recovery_sets(db, imported.history_id).await.unwrap();
    assert_eq!(census_sets.len(), 1);
    assert_eq!(census_sets[0].slice_size, set.slice_size);
    assert_eq!(census_sets[0].files[0].name, "testdata.bin");
    assert_eq!(census_sets[0].volumes[0].segment_ids, volume_ids);

    // Deleting the history item removes the set and the mapping.
    db.delete_history_item(imported.history_id).await.unwrap();
    assert!(
        db.get_par2_sets(imported.history_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        db.get_par2_files_for_dav_item(data_item.id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn deleting_the_release_items_removes_the_set() {
    let mut release = Release::new();
    release.post("testdata.bin", &fixture("testdata.bin"));
    release.post("testdata.bin.par2", &fixture("testdata.bin.par2"));
    let imported = import(release).await;

    assert_eq!(
        imported
            .db
            .get_par2_sets(imported.history_id)
            .await
            .unwrap()
            .len(),
        1
    );
    imported
        .db
        .delete_dav_items_by_history(imported.history_id)
        .await
        .unwrap();
    assert!(
        imported
            .db
            .get_par2_sets(imported.history_id)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn obfuscated_volume_joins_its_set() {
    let mut release = Release::new();
    release.post("testdata.bin", &fixture("testdata.bin"));
    release.post("testdata.bin.par2", &fixture("testdata.bin.par2"));
    let volume_ids = release.post("5f2c9e1a.par2", &fixture("testdata.bin.vol0+1.par2"));
    let imported = import(release).await;

    let sets = imported
        .db
        .get_par2_sets(imported.history_id)
        .await
        .unwrap();
    assert_eq!(sets.len(), 1);
    let volume = &sets[0].volumes[0];
    assert_eq!(volume.segment_ids, volume_ids);
    assert_eq!(volume.first_exponent, None);
    assert_eq!(volume.blocks, 1);
    assert_eq!(volume.blocks_source, Par2BlockSource::Estimated);
}

#[tokio::test]
async fn release_with_two_sets_records_both() {
    let mut release = Release::new();
    release.post("testdata.bin", &fixture("testdata.bin"));
    release.post("testdata.bin.par2", &fixture("testdata.bin.par2"));
    release.post(
        "testdata.bin.vol0+1.par2",
        &fixture("testdata.bin.vol0+1.par2"),
    );
    release.post("other.par2", &fixture("other.par2"));
    let imported = import(release).await;

    let sets = imported
        .db
        .get_par2_sets(imported.history_id)
        .await
        .unwrap();
    let by_id = |id: &str| -> &Par2SetRecord {
        sets.iter()
            .find(|s| hex(&s.recovery_set_id) == id)
            .unwrap_or_else(|| panic!("set {id} not recorded"))
    };
    assert_eq!(sets.len(), 2);

    let first = by_id("eab6403859a33ba2af446b0297d24aa4");
    assert!(first.files[0].carried.is_some());
    assert_eq!(first.volumes.len(), 1);

    // The other set protects a 200000-byte file the release does not carry.
    let other = by_id("5726f92830947ecdc4cacc24dcea8d15");
    assert_eq!(other.files[0].size, 200_000);
    assert!(other.files[0].carried.is_none());
    assert!(other.files[0].dav_item_ids.is_empty());
    assert!(other.volumes.is_empty());
}

#[tokio::test]
async fn malformed_par2_is_skipped_and_the_import_succeeds() {
    let mut release = Release::new();
    release.post("testdata.bin", &fixture("testdata.bin"));
    release.post("testdata.bin.par2", &[0x5a; 600]);
    let imported = import(release).await;

    assert!(
        imported
            .db
            .get_par2_sets(imported.history_id)
            .await
            .unwrap()
            .is_empty()
    );
    item(&imported.db, imported.job_dir_id, "testdata.bin").await;
}

#[tokio::test]
async fn release_without_par2_has_no_set() {
    let mut release = Release::new();
    release.post("testdata.bin", &fixture("testdata.bin"));
    let imported = import(release).await;

    assert!(
        imported
            .db
            .get_par2_sets(imported.history_id)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        load_recovery_sets(&imported.db, imported.history_id)
            .await
            .unwrap()
            .is_empty()
    );
}

fn hex(id: &[u8; 16]) -> String {
    id.iter().map(|b| format!("{b:02x}")).collect()
}
