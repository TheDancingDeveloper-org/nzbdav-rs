//! PAR2 recovery sets recorded at import.
//!
//! The import already reads each set's index to resolve file names. What it
//! learns is kept for the damage census and the repair job: the set's
//! critical packets, which NZB file (and which DAV items) carry each
//! protected file, and which articles carry each recovery volume. Volumes are
//! never downloaded and never become DAV items.

use std::collections::HashMap;

use nzbdav_core::models::{
    Par2BlockSource, Par2CarriedFile, Par2ProtectedFile, Par2SetRecord, Par2VolumeRecord,
};
use rust_par2::{Par2File, Par2FileSet};
use uuid::Uuid;

use crate::deobfuscation::NzbFileInfo;
use crate::deobfuscation::get_par2_file_descriptors::{Par2IndexSet, parse_index, parse_par2_name};

/// Bytes a recovery slice packet adds to its slice: 64-byte header plus the
/// 4-byte exponent.
const RECOVERY_PACKET_OVERHEAD: u64 = 68;

/// Build the records of the sets read at import.
///
/// `dav_items_by_segment` maps each article to the DAV items that read it.
pub fn build_set_records(
    sets: &[Par2IndexSet],
    file_infos: &[NzbFileInfo],
    dav_items_by_segment: &HashMap<&str, Vec<Uuid>>,
) -> Vec<Par2SetRecord> {
    let release_part_size = part_size(file_infos.iter());
    sets.iter()
        .map(|set| build_record(set, file_infos, dav_items_by_segment, release_part_size))
        .collect()
}

/// Parse a record's critical packets back into a PAR2 file set (slice
/// checksums included). `None` if the packets no longer parse.
pub fn parse_set_record(record: &Par2SetRecord) -> Option<Par2FileSet> {
    parse_index(&record.index_packets)
}

fn build_record(
    set: &Par2IndexSet,
    file_infos: &[NzbFileInfo],
    dav_items_by_segment: &HashMap<&str, Vec<Uuid>>,
    release_part_size: Option<u64>,
) -> Par2SetRecord {
    let file_set = &set.file_set;

    // Main-packet order: files sorted by file id.
    let mut descriptors: Vec<&Par2File> = file_set.files.values().collect();
    descriptors.sort_by_key(|d| d.file_id);

    let mut carried_infos = Vec::new();
    let files = descriptors
        .into_iter()
        .map(|desc| {
            let info = carrier_of(desc, file_infos);
            let dav_item_ids = info
                .map(|info| dav_items_of(&info.segment_ids, dav_items_by_segment))
                .unwrap_or_default();
            if let Some(info) = info {
                carried_infos.push(info);
            }
            Par2ProtectedFile {
                file_id: desc.file_id,
                name: desc.filename.clone(),
                size: desc.size,
                carried: info.map(|info| Par2CarriedFile {
                    subject_name: info.subject_name.clone(),
                    segment_ids: info.segment_ids.clone(),
                    part_size: info.first_segment_size,
                }),
                dav_item_ids,
            }
        })
        .collect();

    let volumes = set
        .members
        .iter()
        .filter_map(|&i| volume_record(set, i, &file_infos[i]))
        .collect();

    Par2SetRecord {
        recovery_set_id: file_set.recovery_set_id,
        slice_size: file_set.slice_size,
        index_packets: set.packets.clone(),
        index_segment_ids: file_infos[set.index].segment_ids.clone(),
        files,
        volumes,
        part_size: part_size(carried_infos.into_iter()).or(release_part_size),
    }
}

/// The NZB file whose first 16 KiB match the descriptor, preferring one
/// already resolved to the descriptor's name.
fn carrier_of<'a>(desc: &Par2File, file_infos: &'a [NzbFileInfo]) -> Option<&'a NzbFileInfo> {
    let mut matches = file_infos
        .iter()
        .filter(|f| !f.is_par2 && f.hash_16k == Some(desc.hash_16k));
    let first = matches.next()?;
    if first.resolved_name == desc.filename {
        return Some(first);
    }
    Some(
        matches
            .find(|f| f.resolved_name == desc.filename)
            .unwrap_or(first),
    )
}

fn dav_items_of(segment_ids: &[String], by_segment: &HashMap<&str, Vec<Uuid>>) -> Vec<Uuid> {
    let mut ids: Vec<Uuid> = Vec::new();
    for id in segment_ids
        .iter()
        .filter_map(|s| by_segment.get(s.as_str()))
    {
        for id in id {
            if !ids.contains(id) {
                ids.push(*id);
            }
        }
    }
    ids
}

/// The recovery volume a PAR2 file of the set represents, if it holds any
/// recovery blocks.
fn volume_record(set: &Par2IndexSet, i: usize, info: &NzbFileInfo) -> Option<Par2VolumeRecord> {
    let name = parse_par2_name(&info.resolved_name);
    let range = name.as_ref().and_then(|n| n.volume);
    let (blocks, blocks_source) = if i == set.index {
        // The index was read: its recovery packets were counted.
        (set.file_set.recovery_block_count, Par2BlockSource::Parsed)
    } else if let Some((_, blocks)) = range {
        (blocks, Par2BlockSource::FileName)
    } else {
        // Obfuscated name: estimate from the encoded size, allowing ~3% for
        // yEnc and article overhead, to the nearest block. A copy of the
        // index rounds to none.
        let decoded = info.file_size.saturating_mul(100) / 103;
        let per_block = set.file_set.slice_size + RECOVERY_PACKET_OVERHEAD;
        let blocks = u32::try_from((decoded + per_block / 2) / per_block).unwrap_or(u32::MAX);
        (blocks, Par2BlockSource::Estimated)
    };
    if blocks == 0 {
        return None;
    }
    Some(Par2VolumeRecord {
        name: info.resolved_name.clone(),
        first_exponent: range.map(|(first, _)| first),
        blocks,
        blocks_source,
        size: info.file_size,
        segment_ids: info.segment_ids.clone(),
    })
}

/// Most common first-segment size among multi-segment files: the release's
/// yEnc part size. Single-segment files say nothing about it.
fn part_size<'a>(infos: impl Iterator<Item = &'a NzbFileInfo>) -> Option<u64> {
    let mut counts: HashMap<u64, usize> = HashMap::new();
    for info in infos.filter(|f| f.segment_ids.len() > 1) {
        if let Some(size) = info.first_segment_size {
            *counts.entry(size).or_default() += 1;
        }
    }
    counts
        .into_iter()
        .max_by_key(|&(size, count)| (count, size))
        .map(|(size, _)| size)
}
