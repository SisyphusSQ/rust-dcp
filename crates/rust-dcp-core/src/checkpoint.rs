use serde::{Deserialize, Serialize};

use crate::{DcpError, Result};

/// A branch in a vBucket failover history.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct FailoverEntry {
    /// UUID identifying the history branch.
    pub vbucket_uuid: u64,
    /// First sequence number on the history branch.
    pub seqno: u64,
}

/// Durable resume position for one vBucket.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DcpCheckpoint {
    /// Bucket UUID used to reject checkpoints from a recreated bucket.
    pub bucket_uuid: Option<String>,
    /// Partition identifier.
    pub vbucket: u16,
    /// Failover history branch UUID.
    pub vbucket_uuid: u64,
    /// Last contiguously processed sequence number.
    pub seqno: u64,
    /// Start of the DCP snapshot containing `seqno`.
    pub snapshot_start: u64,
    /// End of the DCP snapshot containing `seqno`.
    pub snapshot_end: u64,
    /// Collection manifest UID observed at this position.
    pub manifest_uid: Option<u64>,
}

impl DcpCheckpoint {
    /// Creates an empty earliest checkpoint for a vBucket.
    #[must_use]
    pub const fn earliest(vbucket: u16) -> Self {
        Self {
            bucket_uuid: None,
            vbucket,
            vbucket_uuid: 0,
            seqno: 0,
            snapshot_start: 0,
            snapshot_end: 0,
            manifest_uid: None,
        }
    }

    /// Validates ordering and snapshot invariants.
    ///
    /// # Errors
    ///
    /// Returns [`DcpError::Checkpoint`] when the sequence number lies outside
    /// the recorded snapshot or the snapshot bounds are reversed.
    pub fn validate(&self) -> Result<()> {
        if self.snapshot_start > self.snapshot_end {
            return Err(DcpError::Checkpoint(format!(
                "snapshot start {} exceeds snapshot end {} for vBucket {}",
                self.snapshot_start, self.snapshot_end, self.vbucket
            )));
        }

        if self.seqno > self.snapshot_end {
            return Err(DcpError::Checkpoint(format!(
                "seqno {} exceeds snapshot end {} for vBucket {}",
                self.seqno, self.snapshot_end, self.vbucket
            )));
        }

        if self.seqno != 0 && self.seqno < self.snapshot_start {
            return Err(DcpError::Checkpoint(format!(
                "seqno {} precedes snapshot start {} for vBucket {}",
                self.seqno, self.snapshot_start, self.vbucket
            )));
        }

        Ok(())
    }

    /// Returns a copy advanced within the supplied snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`DcpError::Checkpoint`] when the new position moves backwards
    /// or does not lie within the supplied snapshot.
    pub fn advanced_to(&self, seqno: u64, snapshot_start: u64, snapshot_end: u64) -> Result<Self> {
        if seqno < self.seqno {
            return Err(DcpError::Checkpoint(format!(
                "cannot move vBucket {} checkpoint backwards from {} to {}",
                self.vbucket, self.seqno, seqno
            )));
        }

        let mut next = self.clone();
        next.seqno = seqno;
        next.snapshot_start = snapshot_start;
        next.snapshot_end = snapshot_end;
        next.validate()?;
        Ok(next)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn earliest_checkpoint_is_valid() {
        assert!(DcpCheckpoint::earliest(7).validate().is_ok());
    }

    #[test]
    fn checkpoint_rejects_position_outside_snapshot() {
        let checkpoint = DcpCheckpoint {
            seqno: 12,
            snapshot_start: 1,
            snapshot_end: 10,
            ..DcpCheckpoint::earliest(7)
        };

        assert!(matches!(
            checkpoint.validate(),
            Err(DcpError::Checkpoint(_))
        ));
    }

    #[test]
    fn checkpoint_cannot_advance_backwards() {
        let checkpoint = DcpCheckpoint {
            seqno: 8,
            snapshot_start: 5,
            snapshot_end: 10,
            ..DcpCheckpoint::earliest(3)
        };

        assert!(checkpoint.advanced_to(7, 5, 10).is_err());
    }
}
