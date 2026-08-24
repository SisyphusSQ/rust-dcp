use std::collections::{BTreeMap, HashSet};

use rust_dcp_core::VBucketAssignment;

use crate::{KubernetesMembershipError, Result, stateful::validate_dns_name};

/// Exact local pod identity used to fence same-name recreation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodIdentity {
    pod_name: String,
    uid: String,
}

impl PodIdentity {
    /// Creates a validated pod name and UID pair.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an invalid pod name or empty UID.
    pub fn new(pod_name: impl Into<String>, uid: impl Into<String>) -> Result<Self> {
        let pod_name = pod_name.into();
        let uid = uid.into();
        validate_dns_name("pod", &pod_name)?;
        validate_uid(&uid)?;
        Ok(Self { pod_name, uid })
    }

    /// Local pod name.
    #[must_use]
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    /// Local Kubernetes UID.
    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }
}

/// Pod fields required by the deterministic membership state machine.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodState {
    pod_name: String,
    uid: String,
    created_at_millis: u64,
    ready: bool,
    terminating: bool,
}

impl PodState {
    /// Creates one normalized Kubernetes pod observation.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an invalid pod name or UID.
    pub fn new(
        pod_name: impl Into<String>,
        uid: impl Into<String>,
        created_at_millis: u64,
        ready: bool,
        terminating: bool,
    ) -> Result<Self> {
        let pod_name = pod_name.into();
        let uid = uid.into();
        validate_dns_name("pod", &pod_name)?;
        validate_uid(&uid)?;
        Ok(Self {
            pod_name,
            uid,
            created_at_millis,
            ready,
            terminating,
        })
    }

    /// Pod name.
    #[must_use]
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    /// Kubernetes UID.
    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }

    /// Kubernetes creation timestamp in Unix milliseconds.
    #[must_use]
    pub const fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }

    /// Whether the Kubernetes Ready condition is `True`.
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        self.ready
    }

    /// Whether a deletion timestamp is present.
    #[must_use]
    pub const fn is_terminating(&self) -> bool {
        self.terminating
    }

    fn is_effective(&self) -> bool {
        self.ready && !self.terminating
    }
}

/// Stable member metadata included in an assignment snapshot.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PodMember {
    pod_name: String,
    uid: String,
    created_at_millis: u64,
}

impl PodMember {
    /// Pod name.
    #[must_use]
    pub fn pod_name(&self) -> &str {
        &self.pod_name
    }

    /// Exact Kubernetes UID.
    #[must_use]
    pub fn uid(&self) -> &str {
        &self.uid
    }

    /// Kubernetes creation timestamp in Unix milliseconds.
    #[must_use]
    pub const fn created_at_millis(&self) -> u64 {
        self.created_at_millis
    }
}

/// One atomically derived Ready-pod view and local vBucket assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KubernetesMembershipSnapshot {
    assignment: VBucketAssignment,
    members: Vec<PodMember>,
}

impl KubernetesMembershipSnapshot {
    /// Fenced local vBucket assignment.
    #[must_use]
    pub const fn assignment(&self) -> &VBucketAssignment {
        &self.assignment
    }

    /// Deterministically ordered Ready and nonterminating pods.
    #[must_use]
    pub fn members(&self) -> &[PodMember] {
        &self.members
    }
}

/// Normalized kube watcher event, including atomic initial-list boundaries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PodWatchEvent {
    /// One incremental create or update.
    Apply(PodState),
    /// One incremental deletion, which only needs the immutable pod identity.
    Delete(PodIdentity),
    /// Starts a complete relist without changing the active view yet.
    Init,
    /// Adds one pod to the pending complete relist.
    InitApply(PodState),
    /// Atomically replaces the active view with the completed relist.
    InitDone,
}

/// Deterministic state machine for kube watcher events.
#[derive(Clone, Debug)]
pub struct PodMembershipState {
    identity: PodIdentity,
    vbucket_count: usize,
    generation: u64,
    active: BTreeMap<String, PodState>,
    initializing: Option<BTreeMap<String, PodState>>,
    initialized: bool,
    published_once: bool,
}

impl PodMembershipState {
    /// Creates an empty watcher state for one exact local pod.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for invalid vBucket bounds.
    pub fn new(identity: PodIdentity, vbucket_count: usize) -> Result<Self> {
        VBucketAssignment::balanced(1, vbucket_count, 1, 1)
            .map_err(|error| KubernetesMembershipError::Configuration(error.to_string()))?;
        Ok(Self {
            identity,
            vbucket_count,
            generation: 0,
            active: BTreeMap::new(),
            initializing: None,
            initialized: false,
            published_once: false,
        })
    }

    /// Current local watcher generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Applies one normalized watcher event.
    ///
    /// Incremental events are rejected during an atomic relist. `InitApply`
    /// values remain invisible until `InitDone` swaps the complete set.
    ///
    /// # Errors
    ///
    /// Returns sequence, generation, duplicate-UID, assignment, or local-pod
    /// fencing errors.
    pub fn apply(&mut self, event: PodWatchEvent) -> Result<Option<KubernetesMembershipSnapshot>> {
        match event {
            PodWatchEvent::Init => {
                self.initializing = Some(BTreeMap::new());
                Ok(None)
            }
            PodWatchEvent::InitApply(pod) => {
                let buffer = self.initializing.as_mut().ok_or_else(|| {
                    KubernetesMembershipError::WatchSequence(
                        "InitApply arrived without Init".into(),
                    )
                })?;
                if buffer.contains_key(pod.pod_name()) {
                    return Err(KubernetesMembershipError::WatchSequence(format!(
                        "initial list repeated pod {:?}",
                        pod.pod_name()
                    )));
                }
                if pod.is_effective() {
                    buffer.insert(pod.pod_name.clone(), pod);
                }
                Ok(None)
            }
            PodWatchEvent::InitDone => {
                let next = self.initializing.take().ok_or_else(|| {
                    KubernetesMembershipError::WatchSequence("InitDone arrived without Init".into())
                })?;
                self.initialized = true;
                self.commit(next)
            }
            PodWatchEvent::Apply(pod) => {
                self.ensure_incremental_allowed("Apply")?;
                let mut next = self.active.clone();
                if pod.is_effective() {
                    next.insert(pod.pod_name.clone(), pod);
                } else {
                    next.remove(pod.pod_name());
                }
                self.commit(next)
            }
            PodWatchEvent::Delete(pod) => {
                self.ensure_incremental_allowed("Delete")?;
                let mut next = self.active.clone();
                if next
                    .get(pod.pod_name())
                    .is_some_and(|current| current.uid == pod.uid)
                {
                    next.remove(pod.pod_name());
                }
                self.commit(next)
            }
        }
    }

    fn ensure_incremental_allowed(&self, kind: &str) -> Result<()> {
        if !self.initialized {
            return Err(KubernetesMembershipError::WatchSequence(format!(
                "{kind} arrived before the initial list completed"
            )));
        }
        if self.initializing.is_some() {
            return Err(KubernetesMembershipError::WatchSequence(format!(
                "{kind} arrived during an atomic relist"
            )));
        }
        Ok(())
    }

    fn commit(
        &mut self,
        next: BTreeMap<String, PodState>,
    ) -> Result<Option<KubernetesMembershipSnapshot>> {
        validate_unique_uids(&next)?;
        if next == self.active {
            return Ok(None);
        }
        let next_generation = self
            .generation
            .checked_add(1)
            .ok_or(KubernetesMembershipError::GenerationOverflow)?;
        self.active = next;
        self.generation = next_generation;
        self.snapshot()
    }

    fn snapshot(&mut self) -> Result<Option<KubernetesMembershipSnapshot>> {
        let Some(local) = self.active.get(self.identity.pod_name()) else {
            if self.published_once {
                return Err(KubernetesMembershipError::Fenced {
                    pod_name: self.identity.pod_name.clone(),
                    expected_uid: self.identity.uid.clone(),
                    observed_uid: None,
                });
            }
            return Ok(None);
        };
        if local.uid != self.identity.uid {
            return Err(KubernetesMembershipError::Fenced {
                pod_name: self.identity.pod_name.clone(),
                expected_uid: self.identity.uid.clone(),
                observed_uid: Some(local.uid.clone()),
            });
        }
        let mut pods = self.active.values().cloned().collect::<Vec<_>>();
        pods.sort_by(|left, right| {
            left.created_at_millis
                .cmp(&right.created_at_millis)
                .then_with(|| left.uid.cmp(&right.uid))
                .then_with(|| left.pod_name.cmp(&right.pod_name))
        });
        let member_number = pods
            .iter()
            .position(|pod| pod.pod_name == self.identity.pod_name && pod.uid == self.identity.uid)
            .map(|index| index + 1)
            .ok_or_else(|| KubernetesMembershipError::Fenced {
                pod_name: self.identity.pod_name.clone(),
                expected_uid: self.identity.uid.clone(),
                observed_uid: None,
            })?;
        let assignment = VBucketAssignment::balanced(
            self.generation,
            self.vbucket_count,
            member_number,
            pods.len(),
        )
        .map_err(|error| KubernetesMembershipError::Assignment(error.to_string()))?;
        let members = pods
            .into_iter()
            .map(|pod| PodMember {
                pod_name: pod.pod_name,
                uid: pod.uid,
                created_at_millis: pod.created_at_millis,
            })
            .collect();
        self.published_once = true;
        Ok(Some(KubernetesMembershipSnapshot {
            assignment,
            members,
        }))
    }
}

fn validate_uid(uid: &str) -> Result<()> {
    if uid.is_empty() || uid.len() > 128 || !uid.bytes().all(|byte| byte.is_ascii_graphic()) {
        return Err(KubernetesMembershipError::Configuration(
            "pod UID must contain 1..=128 visible ASCII bytes".into(),
        ));
    }
    Ok(())
}

fn validate_unique_uids(pods: &BTreeMap<String, PodState>) -> Result<()> {
    let mut seen = HashSet::with_capacity(pods.len());
    for pod in pods.values() {
        if !seen.insert(&pod.uid) {
            return Err(KubernetesMembershipError::WatchSequence(format!(
                "effective pod list repeated UID {:?}",
                pod.uid
            )));
        }
    }
    Ok(())
}
