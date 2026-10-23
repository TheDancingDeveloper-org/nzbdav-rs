//! Step 2: Find each PAR2 set's index file and parse its packets.
//!
//! The PAR2 index file (the one without recovery blocks) is always the smallest
//! `.par2` file in its set. It contains file descriptor packets with real
//! filenames and 16KB hashes that we use for deobfuscation matching, and the
//! slice checksums the repair job needs.
//!
//! A release can carry several sets. PAR2 files are grouped by the set name in
//! their file name (`name.par2`, `name.volNN+MM.par2`) and one index is read
//! per group. Groups that turn out to share a recovery set id are merged.

use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use md5::{Digest, Md5};
use nzbdav_stream::provider::UsenetArticleProvider;
use rust_par2::{Par2File, Par2FileSet};
use tracing::{debug, info, warn};

use super::NzbFileInfo;
use crate::error::Result;

/// An index file larger than this is a recovery volume, not an index.
const MAX_INDEX_FILE_SIZE: u64 = 5 * 1024 * 1024; // 5MB

/// Upper bound on index files fetched per release. Obfuscated releases can
/// give every PAR2 file its own name, which would otherwise make each one a
/// group of its own.
const MAX_INDEX_FETCHES: usize = 8;

const PACKET_MAGIC: &[u8; 8] = b"PAR2\x00PKT";
const PACKET_HEADER_LEN: usize = 64;
/// Packets a repair needs besides the recovery slices themselves.
const CRITICAL_PACKET_TYPES: [&[u8; 16]; 4] = [
    b"PAR 2.0\x00Main\x00\x00\x00\x00",
    b"PAR 2.0\x00FileDesc",
    b"PAR 2.0\x00IFSC\x00\x00\x00\x00",
    b"PAR 2.0\x00Creator\x00",
];

/// One PAR2 recovery set, read from its index file.
#[derive(Debug, Clone)]
pub struct Par2IndexSet {
    pub file_set: Par2FileSet,
    /// The set's critical packets, deduplicated, without recovery slices.
    pub packets: Vec<u8>,
    /// Position in `file_infos` of the index file that was read.
    pub index: usize,
    /// Positions in `file_infos` of every PAR2 file named for this set,
    /// the index included.
    pub members: Vec<usize>,
}

impl Par2IndexSet {
    /// File descriptors of every set, for name matching.
    pub fn descriptors(sets: &[Par2IndexSet]) -> Vec<Par2File> {
        sets.iter()
            .flat_map(|s| s.file_set.files.values().cloned())
            .collect()
    }
}

/// Find the index file of each PAR2 set among the NZB files, fetch it, and
/// parse its packets.
///
/// Returns an empty vec if no PAR2 files are found (deobfuscation will fall
/// back to subject names). An index that does not parse is logged and
/// skipped. A failure to fetch the first index is returned, as before sets
/// were recorded; later indexes are best effort.
pub async fn get_par2_file_descriptors(
    provider: &Arc<UsenetArticleProvider>,
    file_infos: &[NzbFileInfo],
) -> Result<Vec<Par2IndexSet>> {
    let groups = group_par2_files(file_infos);
    if groups.is_empty() {
        debug!("no PAR2 files found, skipping PAR2 descriptor parsing");
        return Ok(Vec::new());
    }

    let mut sets: Vec<Par2IndexSet> = Vec::new();
    let mut unread: Vec<usize> = Vec::new();
    let mut fetches = 0usize;

    for group in groups {
        let candidate = group.index;
        let par2_file = &file_infos[candidate];
        if par2_file.file_size > MAX_INDEX_FILE_SIZE {
            info!(
                smallest_par2_size = par2_file.file_size,
                max_index_size = MAX_INDEX_FILE_SIZE,
                set = %group.name,
                "smallest PAR2 file is too large to be an index file, skipping PAR2 set"
            );
            unread.extend(group.members);
            continue;
        }
        // Every packet header names its set, so a group whose first packet
        // belongs to a set already read joins it without a fetch.
        if let Some(existing) = packet_set_id(par2_file)
            .and_then(|id| sets.iter_mut().find(|s| s.file_set.recovery_set_id == id))
        {
            debug!(set = %group.name, "PAR2 group belongs to a set already read");
            existing.members.extend(group.members);
            continue;
        }
        if fetches >= MAX_INDEX_FETCHES {
            debug!(set = %group.name, "PAR2 index fetch limit reached, skipping set");
            unread.extend(group.members);
            continue;
        }

        info!(
            subject_name = %par2_file.subject_name,
            file_size = par2_file.file_size,
            segments = par2_file.segment_ids.len(),
            "fetching PAR2 index file"
        );

        // Fetch all segments and concatenate in order.
        fetches += 1;
        let par2_data = match fetch_all_segments(provider, &par2_file.segment_ids).await {
            Ok(data) => data,
            Err(e) if fetches == 1 => return Err(e),
            Err(e) => {
                warn!(set = %group.name, error = %e, "failed to fetch PAR2 index, skipping set");
                unread.extend(group.members);
                continue;
            }
        };

        debug!(
            total_bytes = par2_data.len(),
            "PAR2 index file fetched, parsing packets"
        );

        // A PAR2 index that does not parse is skipped: deobfuscation falls
        // back to subject/yEnc filenames and the release has no set.
        let Some(file_set) = parse_index(&par2_data) else {
            warn!(
                set = %group.name,
                "failed to parse PAR2 index file (falling back to subject names)"
            );
            unread.extend(group.members);
            continue;
        };

        if let Some(existing) = sets
            .iter_mut()
            .find(|s| s.file_set.recovery_set_id == file_set.recovery_set_id)
        {
            debug!(set = %group.name, "PAR2 group belongs to a set already read");
            existing.members.extend(group.members);
            continue;
        }

        let packets = critical_packets(&par2_data, &file_set.recovery_set_id);
        info!(
            set = %group.name,
            descriptor_count = file_set.files.len(),
            slice_size = file_set.slice_size,
            "parsed PAR2 file descriptors"
        );
        for desc in file_set.files.values() {
            debug!(
                filename = %desc.filename,
                size = desc.size,
                hash_16k = %hex::encode(desc.hash_16k),
                "PAR2 file descriptor"
            );
        }
        sets.push(Par2IndexSet {
            file_set,
            packets,
            index: candidate,
            members: group.members,
        });
    }

    // Volumes whose set could not be told apart by name belong to the only
    // set there is; with several sets they cannot be placed.
    if !unread.is_empty() {
        if let [only] = sets.as_mut_slice() {
            only.members.extend(unread);
        } else if !sets.is_empty() {
            debug!(
                unplaced = unread.len(),
                "PAR2 files not matched to any of several sets"
            );
        }
    }
    for set in &mut sets {
        set.members.sort_unstable();
        set.members.dedup();
    }

    Ok(sets)
}

/// PAR2 files named for one set.
#[derive(Debug)]
struct Par2Group {
    name: String,
    /// Index file candidate.
    index: usize,
    members: Vec<usize>,
    has_plain_index: bool,
}

/// Group PAR2 files by set name, pick each group's index candidate (the
/// smallest file without a volume suffix, else the smallest file), and order
/// the groups so real indexes and small files are read first.
fn group_par2_files(file_infos: &[NzbFileInfo]) -> Vec<Par2Group> {
    let mut groups: Vec<Par2Group> = Vec::new();
    for (i, info) in file_infos.iter().enumerate() {
        if !info.is_par2 {
            continue;
        }
        let name = parse_par2_name(&info.resolved_name)
            .map(|n| n.set_name.to_ascii_lowercase())
            .unwrap_or_else(|| info.resolved_name.to_ascii_lowercase());
        match groups.iter_mut().find(|g| g.name == name) {
            Some(g) => g.members.push(i),
            None => groups.push(Par2Group {
                name,
                index: i,
                members: vec![i],
                has_plain_index: false,
            }),
        }
    }

    let is_volume = |i: usize| {
        parse_par2_name(&file_infos[i].resolved_name).is_some_and(|n| n.volume.is_some())
    };
    for group in &mut groups {
        let plain = group
            .members
            .iter()
            .copied()
            .filter(|&i| !is_volume(i))
            .min_by_key(|&i| file_infos[i].file_size);
        group.has_plain_index = plain.is_some();
        group.index = plain.unwrap_or_else(|| {
            group
                .members
                .iter()
                .copied()
                .min_by_key(|&i| file_infos[i].file_size)
                .unwrap_or(group.index)
        });
    }
    groups.sort_by_key(|g| (!g.has_plain_index, file_infos[g.index].file_size));
    groups
}

/// Recovery set id in the first packet header of a file, from the bytes
/// read during first-segment analysis.
fn packet_set_id(info: &NzbFileInfo) -> Option<[u8; 16]> {
    let head = info.first_16k.as_deref()?;
    if head.len() < PACKET_HEADER_LEN || &head[..8] != PACKET_MAGIC {
        return None;
    }
    head[32..48].try_into().ok()
}

/// Parse a PAR2 index. `None` when it carries no usable set.
pub(crate) fn parse_index(data: &[u8]) -> Option<Par2FileSet> {
    let mut cursor = Cursor::new(data);
    match rust_par2::parse_par2_reader(&mut cursor, data.len() as u64) {
        Ok(set) if set.slice_size > 0 && !set.files.is_empty() => Some(set),
        Ok(_) => None,
        Err(e) => {
            debug!(error = %e, "PAR2 index did not parse");
            None
        }
    }
}

/// Extract the critical packets of one set from raw PAR2 data: each packet
/// whose header and body hash check out, once, in file order.
pub(crate) fn critical_packets(data: &[u8], recovery_set_id: &[u8; 16]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut seen: HashSet<[u8; 16]> = HashSet::new();
    let mut pos = 0usize;
    while pos + PACKET_HEADER_LEN <= data.len() {
        if &data[pos..pos + 8] != PACKET_MAGIC {
            pos += 1;
            continue;
        }
        let len = u64::from_le_bytes(data[pos + 8..pos + 16].try_into().unwrap_or_default());
        let end = usize::try_from(len)
            .ok()
            .and_then(|len| pos.checked_add(len))
            .filter(|&end| end <= data.len());
        let Some(end) = end.filter(|_| len >= PACKET_HEADER_LEN as u64 && len % 4 == 0) else {
            pos += 1;
            continue;
        };
        let packet = &data[pos..end];
        let hash: [u8; 16] = packet[16..32].try_into().unwrap_or_default();
        let body_hash: [u8; 16] = Md5::digest(&packet[32..]).into();
        if hash != body_hash {
            pos += 1;
            continue;
        }
        let critical = CRITICAL_PACKET_TYPES
            .iter()
            .any(|t| &packet[48..64] == t.as_slice());
        if critical && &packet[32..48] == recovery_set_id.as_slice() && seen.insert(hash) {
            out.extend_from_slice(packet);
        }
        pos = end;
    }
    out
}

/// A PAR2 file name split into set name and volume range.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Par2Name<'a> {
    pub set_name: &'a str,
    /// `(first exponent, block count)` from `.volNN+MM`.
    pub volume: Option<(u32, u32)>,
}

/// Split `name.par2` / `name.volNN+MM.par2` (same rules as nzb-core's
/// `parse_par2_filename`). `None` when the name does not end in `.par2`.
pub(crate) fn parse_par2_name(filename: &str) -> Option<Par2Name<'_>> {
    let stem = filename
        .len()
        .checked_sub(5)
        .filter(|&n| filename.is_char_boundary(n))
        .filter(|&n| filename[n..].eq_ignore_ascii_case(".par2"))
        .map(|n| &filename[..n])?;
    let volume = stem.to_ascii_lowercase().rfind(".vol").and_then(|at| {
        let (first, blocks) = stem[at + 4..].split_once('+')?;
        Some((at, first.parse().ok()?, blocks.parse().ok()?))
    });
    Some(match volume {
        Some((at, first, blocks)) => Par2Name {
            set_name: &stem[..at],
            volume: Some((first, blocks)),
        },
        None => Par2Name {
            set_name: stem,
            volume: None,
        },
    })
}

/// Fetch all segments of a file sequentially and concatenate the decoded data.
async fn fetch_all_segments(
    provider: &Arc<UsenetArticleProvider>,
    segment_ids: &[String],
) -> Result<Vec<u8>> {
    let mut data = Vec::new();
    let total = segment_ids.len();

    for (i, message_id) in segment_ids.iter().enumerate() {
        let decoded = provider.fetch_decoded_low(message_id).await.map_err(|e| {
            warn!(
                segment = i,
                message_id = %message_id,
                error = %e,
                "failed to fetch PAR2 segment"
            );
            e
        })?;

        data.extend_from_slice(&decoded);

        let done = i + 1;
        if total > 5 && (done % 5 == 0 || done == total) {
            info!(
                progress = done,
                total,
                bytes = data.len(),
                "fetching PAR2 segments"
            );
        }
    }

    Ok(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn par2_names_split_into_set_and_volume() {
        assert_eq!(
            parse_par2_name("Movie.2024.par2"),
            Some(Par2Name {
                set_name: "Movie.2024",
                volume: None
            })
        );
        assert_eq!(
            parse_par2_name("Movie.2024.vol07+08.PAR2"),
            Some(Par2Name {
                set_name: "Movie.2024",
                volume: Some((7, 8))
            })
        );
        // A `.vol` that is not a range is part of the set name.
        assert_eq!(
            parse_par2_name("Movie.volume.par2"),
            Some(Par2Name {
                set_name: "Movie.volume",
                volume: None
            })
        );
        assert_eq!(parse_par2_name("movie.mkv"), None);
        assert_eq!(parse_par2_name("é.par"), None);
    }
}
