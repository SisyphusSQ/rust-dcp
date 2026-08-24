use rust_dcp_core::VBucketAssignment;

use crate::{KubernetesMembershipError, Result};

/// Static `StatefulSet` ordinal assignment configuration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatefulSetMembershipConfig {
    stateful_set: String,
    pod_name: String,
    replicas: usize,
    start_ordinal: usize,
    vbucket_count: usize,
    generation: u64,
}

impl StatefulSetMembershipConfig {
    /// Creates an ordinal assignment configuration.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for invalid Kubernetes names, replica
    /// bounds, or vBucket bounds.
    pub fn new(
        stateful_set: impl Into<String>,
        pod_name: impl Into<String>,
        replicas: usize,
        vbucket_count: usize,
    ) -> Result<Self> {
        let config = Self {
            stateful_set: stateful_set.into(),
            pod_name: pod_name.into(),
            replicas,
            start_ordinal: 0,
            vbucket_count,
            generation: 1,
        };
        config.validate()?;
        Ok(config)
    }

    /// Sets the local assignment fence generation.
    #[must_use]
    pub const fn generation(mut self, generation: u64) -> Self {
        self.generation = generation;
        self
    }

    /// Sets `.spec.ordinals.start` for `StatefulSets` whose first pod ordinal is
    /// not zero.
    #[must_use]
    pub const fn start_ordinal(mut self, start_ordinal: usize) -> Self {
        self.start_ordinal = start_ordinal;
        self
    }

    fn validate(&self) -> Result<()> {
        validate_dns_name("StatefulSet", &self.stateful_set)?;
        validate_dns_name("pod", &self.pod_name)?;
        VBucketAssignment::balanced(self.generation, self.vbucket_count, 1, self.replicas)
            .map_err(|error| KubernetesMembershipError::Configuration(error.to_string()))?;
        self.start_ordinal
            .checked_add(self.replicas)
            .ok_or_else(|| {
                KubernetesMembershipError::Configuration(
                    "StatefulSet ordinal range overflows usize".into(),
                )
            })?;
        Ok(())
    }
}

/// Resolved static assignment for one `StatefulSet` pod ordinal.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StatefulSetMembership {
    assignment: VBucketAssignment,
    member_number: usize,
    total_members: usize,
}

impl StatefulSetMembership {
    /// Parses the canonical final ordinal and derives its vBucket assignment.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when the pod is not a member of the named
    /// `StatefulSet` or its ordinal is noncanonical or outside `replicas`.
    pub fn resolve(config: StatefulSetMembershipConfig) -> Result<Self> {
        config.validate()?;
        let StatefulSetMembershipConfig {
            stateful_set,
            pod_name,
            replicas,
            start_ordinal,
            vbucket_count,
            generation,
        } = config;
        let prefix = format!("{stateful_set}-");
        let suffix = pod_name.strip_prefix(&prefix).ok_or_else(|| {
            KubernetesMembershipError::Configuration(format!(
                "pod {pod_name:?} does not belong to StatefulSet {stateful_set:?}"
            ))
        })?;
        let ordinal = suffix.parse::<usize>().map_err(|error| {
            KubernetesMembershipError::Configuration(format!(
                "pod {pod_name:?} has an invalid StatefulSet ordinal: {error}"
            ))
        })?;
        if ordinal.to_string() != suffix {
            return Err(KubernetesMembershipError::Configuration(format!(
                "pod {pod_name:?} has a noncanonical StatefulSet ordinal"
            )));
        }
        let end_ordinal = start_ordinal.checked_add(replicas).ok_or_else(|| {
            KubernetesMembershipError::Configuration(
                "StatefulSet ordinal range overflows usize".into(),
            )
        })?;
        if ordinal < start_ordinal || ordinal >= end_ordinal {
            return Err(KubernetesMembershipError::Configuration(format!(
                "pod ordinal {ordinal} is outside StatefulSet range {start_ordinal}..{end_ordinal}"
            )));
        }
        let member_number = ordinal - start_ordinal + 1;
        let assignment =
            VBucketAssignment::balanced(generation, vbucket_count, member_number, replicas)
                .map_err(|error| KubernetesMembershipError::Assignment(error.to_string()))?;
        Ok(Self {
            assignment,
            member_number,
            total_members: replicas,
        })
    }

    /// Fenced vBucket assignment for this ordinal.
    #[must_use]
    pub const fn assignment(&self) -> &VBucketAssignment {
        &self.assignment
    }

    /// One-based `StatefulSet` member number.
    #[must_use]
    pub const fn member_number(&self) -> usize {
        self.member_number
    }

    /// Configured `StatefulSet` replica count.
    #[must_use]
    pub const fn total_members(&self) -> usize {
        self.total_members
    }
}

pub(crate) fn validate_dns_name(kind: &str, value: &str) -> Result<()> {
    let valid = !value.is_empty() && value.len() <= 253 && value.split('.').all(is_dns_label);
    if !valid {
        return Err(KubernetesMembershipError::Configuration(format!(
            "{kind} name must be a valid lowercase Kubernetes DNS subdomain"
        )));
    }
    Ok(())
}

pub(crate) fn validate_namespace(value: &str) -> Result<()> {
    if !is_dns_label(value) {
        return Err(KubernetesMembershipError::Configuration(
            "namespace must be a valid lowercase Kubernetes DNS label".into(),
        ));
    }
    Ok(())
}

fn is_dns_label(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 63
        && value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && value
            .as_bytes()
            .last()
            .is_some_and(u8::is_ascii_alphanumeric)
}
