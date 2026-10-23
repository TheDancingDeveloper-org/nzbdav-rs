//! PAR2 recovery sets recorded at import (`par2_sets`, `par2_file_map`).
//!
//! A history item's sets are written together and replace whatever was
//! stored for it before, so a re-import never leaves a stale set behind.

use rusqlite::{Connection, OptionalExtension, params};
use uuid::Uuid;

use crate::error::{DavError, Result};
use crate::models::{Par2FileRef, Par2SetRecord};

/// Replace the sets stored for `history_item_id` with `sets`.
pub fn replace(
    conn: &Connection,
    history_item_id: Uuid,
    job_dir_id: Uuid,
    sets: &[Par2SetRecord],
) -> Result<()> {
    let tx = conn.unchecked_transaction()?;
    delete_by_history_item_id(&tx, history_item_id)?;
    for set in sets {
        let set_id = Uuid::new_v4().to_string();
        let data = bincode::serialize(set).map_err(|e| DavError::Serialization(e.to_string()))?;
        tx.execute(
            "INSERT INTO par2_sets (id, history_item_id, job_dir_id, recovery_set_id, slice_size, recovery_blocks, data)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                set_id,
                history_item_id.to_string(),
                job_dir_id.to_string(),
                set.recovery_set_id.as_slice(),
                set.slice_size as i64,
                set.recovery_blocks() as i64,
                data,
            ],
        )?;
        let mut stmt = tx.prepare(
            "INSERT OR IGNORE INTO par2_file_map (set_id, file_id, dav_item_id)
             SELECT ?1, ?2, id FROM dav_items WHERE id = ?3",
        )?;
        for file in &set.files {
            for dav_item_id in &file.dav_item_ids {
                stmt.execute(params![
                    set_id,
                    file.file_id.as_slice(),
                    dav_item_id.to_string()
                ])?;
            }
        }
    }
    tx.commit()?;
    Ok(())
}

/// Sets stored for a history item, in insertion order.
pub fn get_by_history_item_id(
    conn: &Connection,
    history_item_id: Uuid,
) -> Result<Vec<Par2SetRecord>> {
    let mut stmt =
        conn.prepare("SELECT data FROM par2_sets WHERE history_item_id = ?1 ORDER BY rowid")?;
    let rows = stmt.query_map(params![history_item_id.to_string()], |row| {
        row.get::<_, Vec<u8>>(0)
    })?;
    let mut sets = Vec::new();
    for row in rows {
        let data = row?;
        sets.push(bincode::deserialize(&data).map_err(|e| DavError::Serialization(e.to_string()))?);
    }
    Ok(sets)
}

/// Protected files a DAV item reads from.
pub fn get_files_for_dav_item(conn: &Connection, dav_item_id: Uuid) -> Result<Vec<Par2FileRef>> {
    let mut stmt = conn.prepare(
        "SELECT s.history_item_id, s.recovery_set_id, m.file_id
         FROM par2_file_map m JOIN par2_sets s ON s.id = m.set_id
         WHERE m.dav_item_id = ?1 ORDER BY s.rowid, m.file_id",
    )?;
    let rows = stmt.query_map(params![dav_item_id.to_string()], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, Vec<u8>>(1)?,
            row.get::<_, Vec<u8>>(2)?,
        ))
    })?;
    let mut refs = Vec::new();
    for row in rows {
        let (history, set_id, file_id) = row?;
        refs.push(Par2FileRef {
            history_item_id: Uuid::parse_str(&history)
                .map_err(|e| DavError::Serialization(e.to_string()))?,
            recovery_set_id: id16(&set_id)?,
            file_id: id16(&file_id)?,
        });
    }
    Ok(refs)
}

/// Whether any set is stored for a history item.
pub fn exists_for_history_item_id(conn: &Connection, history_item_id: Uuid) -> Result<bool> {
    Ok(conn
        .query_row(
            "SELECT 1 FROM par2_sets WHERE history_item_id = ?1 LIMIT 1",
            params![history_item_id.to_string()],
            |_| Ok(()),
        )
        .optional()?
        .is_some())
}

/// Delete the sets of a history item (the file map cascades).
pub fn delete_by_history_item_id(conn: &Connection, history_item_id: Uuid) -> Result<()> {
    conn.execute(
        "DELETE FROM par2_sets WHERE history_item_id = ?1",
        params![history_item_id.to_string()],
    )?;
    Ok(())
}

/// Delete every stored set.
pub fn delete_all(conn: &Connection) -> Result<()> {
    conn.execute("DELETE FROM par2_sets", [])?;
    Ok(())
}

fn id16(bytes: &[u8]) -> Result<[u8; 16]> {
    bytes
        .try_into()
        .map_err(|_| DavError::Serialization(format!("expected 16-byte id, got {}", bytes.len())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dav_items;
    use crate::db::open_in_memory;
    use crate::models::{
        DavItem, ItemSubType, ItemType, Par2BlockSource, Par2CarriedFile, Par2ProtectedFile,
        Par2VolumeRecord,
    };

    fn item(name: &str, parent_id: Option<Uuid>, history_item_id: Option<Uuid>) -> DavItem {
        DavItem {
            id: Uuid::new_v4(),
            id_prefix: "x".to_string(),
            created_at: chrono::Utc::now().naive_utc(),
            parent_id,
            name: name.to_string(),
            file_size: None,
            item_type: ItemType::Directory,
            sub_type: ItemSubType::Directory,
            path: format!("/{name}"),
            release_date: None,
            last_health_check: None,
            next_health_check: None,
            history_item_id,
            file_blob_id: None,
            nzb_blob_id: None,
        }
    }

    fn set(set_id: u8, dav_item_ids: Vec<Uuid>) -> Par2SetRecord {
        Par2SetRecord {
            recovery_set_id: [set_id; 16],
            slice_size: 1024,
            index_packets: vec![1, 2, 3],
            index_segment_ids: vec!["idx@test".into()],
            files: vec![Par2ProtectedFile {
                file_id: [7; 16],
                name: "movie.mkv".into(),
                size: 4096,
                carried: Some(Par2CarriedFile {
                    subject_name: "abc".into(),
                    segment_ids: vec!["a@test".into(), "b@test".into()],
                    part_size: Some(2048),
                }),
                dav_item_ids,
            }],
            volumes: vec![Par2VolumeRecord {
                name: "movie.vol00+02.par2".into(),
                first_exponent: Some(0),
                blocks: 2,
                blocks_source: Par2BlockSource::FileName,
                size: 3000,
                segment_ids: vec!["v@test".into()],
            }],
            part_size: Some(2048),
        }
    }

    fn fixture() -> (Connection, Uuid, DavItem, DavItem) {
        let conn = open_in_memory().unwrap();
        let history = Uuid::new_v4();
        let dir = item("job", None, Some(history));
        let file = item("movie.mkv", Some(dir.id), Some(history));
        dav_items::insert(&conn, &dir).unwrap();
        dav_items::insert(&conn, &file).unwrap();
        (conn, history, dir, file)
    }

    #[test]
    fn round_trip_and_reverse_mapping() {
        let (conn, history, dir, file) = fixture();
        let sets = vec![set(1, vec![file.id]), set(2, vec![])];
        replace(&conn, history, dir.id, &sets).unwrap();

        assert_eq!(get_by_history_item_id(&conn, history).unwrap(), sets);
        let refs = get_files_for_dav_item(&conn, file.id).unwrap();
        assert_eq!(
            refs,
            vec![Par2FileRef {
                history_item_id: history,
                recovery_set_id: [1; 16],
                file_id: [7; 16],
            }]
        );
        let blocks: i64 = conn
            .query_row("SELECT SUM(recovery_blocks) FROM par2_sets", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(blocks, 4);
    }

    #[test]
    fn replace_drops_previous_sets() {
        let (conn, history, dir, file) = fixture();
        replace(&conn, history, dir.id, &[set(1, vec![file.id])]).unwrap();
        replace(&conn, history, dir.id, &[set(3, vec![])]).unwrap();

        let stored = get_by_history_item_id(&conn, history).unwrap();
        assert_eq!(stored.len(), 1);
        assert_eq!(stored[0].recovery_set_id, [3; 16]);
        assert!(get_files_for_dav_item(&conn, file.id).unwrap().is_empty());
    }

    #[test]
    fn unknown_dav_items_are_not_mapped() {
        let (conn, history, dir, _) = fixture();
        replace(&conn, history, dir.id, &[set(1, vec![Uuid::new_v4()])]).unwrap();
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM par2_file_map", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn deleting_the_job_dir_cascades() {
        let (conn, history, dir, file) = fixture();
        replace(&conn, history, dir.id, &[set(1, vec![file.id])]).unwrap();
        dav_items::delete_by_history_item_id(&conn, history).unwrap();

        assert!(!exists_for_history_item_id(&conn, history).unwrap());
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM par2_file_map", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0);
    }

    #[test]
    fn deleting_a_mapped_item_drops_its_mapping_only() {
        let (conn, history, dir, file) = fixture();
        replace(&conn, history, dir.id, &[set(1, vec![file.id])]).unwrap();
        dav_items::delete(&conn, file.id).unwrap();

        assert!(get_files_for_dav_item(&conn, file.id).unwrap().is_empty());
        assert!(exists_for_history_item_id(&conn, history).unwrap());
    }
}
