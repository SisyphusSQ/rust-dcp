use std::collections::BTreeSet;

use serde::{Deserialize, Serialize};

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
}
