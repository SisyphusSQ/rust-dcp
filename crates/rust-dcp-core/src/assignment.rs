use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

use crate::{DcpError, Result};

/// Source of vBucket ownership for a subscription.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum AssignmentMode {
    /// The SDK owns every vBucket reported by the bucket topology.
    Standalone,
    /// An outer runtime supplies an explicitly fenced assignment.
    External(VBucketAssignment),
}

/// Fenced set of vBuckets owned by one consumer.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VBucketAssignment {
    generation: u64,
    vbuckets: BTreeSet<u16>,
}

impl VBucketAssignment {
    /// Creates an assignment, deduplicating vBucket identifiers.
    #[must_use]
    pub fn new(generation: u64, vbuckets: impl IntoIterator<Item = u16>) -> Self {
        Self {
            generation,
            vbuckets: vbuckets.into_iter().collect(),
        }
    }

    /// Splits a complete vBucket range into deterministic, contiguous chunks.
    ///
    /// Member numbers are one-based. Any remainder is assigned one vBucket at
    /// a time to the lowest member numbers, matching go-dcp's chunking
    /// semantics without permitting empty members.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the member bounds are invalid, the
    /// member count exceeds the vBucket count, or a vBucket identifier cannot
    /// be represented by `u16`.
    pub fn balanced(
        generation: u64,
        vbucket_count: usize,
        member_number: usize,
        total_members: usize,
    ) -> Result<Self> {
        let max_vbucket_count = usize::from(u16::MAX) + 1;
        if vbucket_count == 0 || vbucket_count > max_vbucket_count {
            return Err(DcpError::InvalidConfiguration(format!(
                "vBucket count must be in 1..={max_vbucket_count}"
            )));
        }
        if total_members == 0 || total_members > vbucket_count {
            return Err(DcpError::InvalidConfiguration(format!(
                "total members must be in 1..={vbucket_count}"
            )));
        }
        if member_number == 0 || member_number > total_members {
            return Err(DcpError::InvalidConfiguration(format!(
                "member number must be in 1..={total_members}"
            )));
        }

        let member_index = member_number - 1;
        let base_size = vbucket_count / total_members;
        let remainder = vbucket_count % total_members;
        let start = member_index * base_size + member_index.min(remainder);
        let len = base_size + usize::from(member_index < remainder);
        let vbuckets = (start..start + len)
            .map(|vbucket| {
                u16::try_from(vbucket).map_err(|error| {
                    DcpError::InvalidConfiguration(format!(
                        "vBucket identifier {vbucket} cannot be represented: {error}"
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self::new(generation, vbuckets))
    }

    /// Monotonic fence supplied by the assignment owner.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Ordered assigned vBuckets.
    #[must_use]
    pub fn vbuckets(&self) -> impl ExactSizeIterator<Item = u16> + '_ {
        self.vbuckets.iter().copied()
    }

    /// Returns whether this assignment owns `vbucket`.
    #[must_use]
    pub fn owns(&self, vbucket: u16) -> bool {
        self.vbuckets.contains(&vbucket)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn assignment_is_sorted_and_deduplicated() {
        let assignment = VBucketAssignment::new(4, [3, 1, 3, 2]);

        assert_eq!(assignment.vbuckets().collect::<Vec<_>>(), vec![1, 2, 3]);
        assert_eq!(assignment.generation(), 4);
        assert!(assignment.owns(2));
        assert!(!assignment.owns(9));
    }

    #[test]
    fn balanced_assignments_cover_every_vbucket_once() {
        let first = VBucketAssignment::balanced(7, 10, 1, 3).unwrap();
        let second = VBucketAssignment::balanced(7, 10, 2, 3).unwrap();
        let third = VBucketAssignment::balanced(7, 10, 3, 3).unwrap();

        assert_eq!(first.vbuckets().collect::<Vec<_>>(), vec![0, 1, 2, 3]);
        assert_eq!(second.vbuckets().collect::<Vec<_>>(), vec![4, 5, 6]);
        assert_eq!(third.vbuckets().collect::<Vec<_>>(), vec![7, 8, 9]);
        assert_eq!(first.generation(), 7);
    }

    #[test]
    fn balanced_assignment_rejects_invalid_membership_bounds() {
        assert!(VBucketAssignment::balanced(1, 10, 0, 2).is_err());
        assert!(VBucketAssignment::balanced(1, 10, 3, 2).is_err());
        assert!(VBucketAssignment::balanced(1, 2, 1, 3).is_err());
        assert!(VBucketAssignment::balanced(1, 65_537, 1, 1).is_err());
    }
}
