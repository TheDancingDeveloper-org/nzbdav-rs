//! On-demand Usenet streaming layer.
//!
//! Provides async streams that fetch articles from Usenet via `nzb-nntp`
//! connection pools and decode them via `nzb-decode`. No data is stored
//! locally — every read fetches from Usenet on demand.

pub mod aes_decoder_stream;
pub mod census;
pub mod error;
pub mod holes;
pub mod multi_segment_stream;
pub mod multipart_file_stream;
pub mod nzb_file_stream;
pub mod patches;
pub mod prioritized_semaphore;
pub mod provider;
pub mod recovery_set;
pub mod seekable_segment_stream;
pub mod testing;

pub use aes_decoder_stream::AesDecoderStream;
pub use census::{
    CensusBackend, CensusConfig, CensusError, CensusReport, CensusTarget, CensusTrigger,
    DamageCensus, Verdict,
};
pub use error::{Result, StreamError};
pub use holes::{
    Consult, HoleLedger, HoleRecord, HoleState, InMemoryHoleLedger, Noted, Outcome, StoredHole,
    server_fingerprint,
};
pub use multi_segment_stream::MultiSegmentStream;
pub use multipart_file_stream::DavMultipartFileStream;
pub use nzb_file_stream::NzbFileStream;
pub use patches::{PatchError, PatchStamp, PatchStore, PatchTotals, ReleaseVerdict};
pub use prioritized_semaphore::{PermitTier, PrioritizedPermit, PrioritizedSemaphore};
pub use provider::{HoleConfig, PoolStat, UsenetArticleProvider, YencHeaders};
pub use recovery_set::{census_files, load_recovery_sets};
pub use seekable_segment_stream::SeekableSegmentStream;

// Re-export nzb_nntp so consumers can use the same ConnectionPool/ServerConfig types
pub use nzb_nntp;
