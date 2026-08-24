use std::collections::BTreeMap;

use rust_dcp_core::VBucketAssignment;
use serde::{Deserialize, Serialize};

use crate::{CouchbaseMembershipError, Result};

const REGISTRY_SCHEMA_VERSION: u16 = 1;
const MAX_ID_LEN: usize = 251;

/// Stable logical member ID plus a unique process incarnation fence.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberIdentity {
    member_id: String,
    incarnation: String,
}

impl MemberIdentity {
    /// Creates and validates one membership process identity.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an empty, oversized, whitespace, or
    /// control-character identifier.
    pub fn new(member_id: impl Into<String>, incarnation: impl Into<String>) -> Result<Self> {
        let member_id = member_id.into();
        let incarnation = incarnation.into();
        validate_identifier("member ID", &member_id)?;
        validate_identifier("incarnation", &incarnation)?;
        Ok(Self {
            member_id,
            incarnation,
        })
    }

    /// Stable logical member ID.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Unique process incarnation fence.
    #[must_use]
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }
}

/// Observable active-member metadata ordered by join time and member ID.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MemberInfo {
    member_id: String,
    incarnation: String,
    joined_at_millis: u64,
    heartbeat_at_millis: u64,
}

impl MemberInfo {
    /// Stable logical member ID.
    #[must_use]
    pub fn member_id(&self) -> &str {
        &self.member_id
    }

    /// Current process incarnation.
    #[must_use]
    pub fn incarnation(&self) -> &str {
        &self.incarnation
    }

    /// Unix timestamp in milliseconds at which this incarnation joined.
    #[must_use]
    pub const fn joined_at_millis(&self) -> u64 {
        self.joined_at_millis
    }

    /// Most recent heartbeat timestamp in Unix milliseconds.
    #[must_use]
    pub const fn heartbeat_at_millis(&self) -> u64 {
        self.heartbeat_at_millis
    }
}

/// One atomically derived membership view and external vBucket assignment.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MembershipSnapshot {
    assignment: VBucketAssignment,
    members: Vec<MemberInfo>,
}

impl MembershipSnapshot {
    /// Fenced vBucket assignment for the local member.
    #[must_use]
    pub const fn assignment(&self) -> &VBucketAssignment {
        &self.assignment
    }

    /// Ordered active-member view used to derive the assignment.
    #[must_use]
    pub fn members(&self) -> &[MemberInfo] {
        &self.members
    }
}

/// Versioned JSON document stored through Couchbase CAS operations.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegistryDocument {
    schema_version: u16,
    vbucket_count: usize,
    stale_after_millis: u64,
    generation: u64,
    members: BTreeMap<String, RegistryMember>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RegistryMember {
    member_id: String,
    incarnation: String,
    joined_at_millis: u64,
    heartbeat_at_millis: u64,
}

impl RegistryDocument {
    /// Creates an empty registry with immutable cluster-wide assignment bounds.
    ///
    /// # Errors
    ///
    /// Returns a configuration error for an invalid vBucket count or zero
    /// stale-member timeout.
    pub fn new(vbucket_count: usize, stale_after_millis: u64) -> Result<Self> {
        validate_vbucket_count(vbucket_count)?;
        if stale_after_millis == 0 {
            return Err(CouchbaseMembershipError::Configuration(
                "stale-member timeout must be greater than zero".into(),
            ));
        }
        Ok(Self {
            schema_version: REGISTRY_SCHEMA_VERSION,
            vbucket_count,
            stale_after_millis,
            generation: 0,
            members: BTreeMap::new(),
        })
    }

    /// Current membership fence generation.
    #[must_use]
    pub const fn generation(&self) -> u64 {
        self.generation
    }

    /// Validates persisted schema and immutable assignment bounds.
    ///
    /// # Errors
    ///
    /// Returns a registry error when any stored field is inconsistent.
    pub fn validate(&self) -> Result<()> {
        if self.schema_version != REGISTRY_SCHEMA_VERSION {
            return Err(CouchbaseMembershipError::Registry(format!(
                "unsupported schema version {}",
                self.schema_version
            )));
        }
        validate_vbucket_count(self.vbucket_count)
            .map_err(|error| CouchbaseMembershipError::Registry(error.to_string()))?;
        if self.stale_after_millis == 0 {
            return Err(CouchbaseMembershipError::Registry(
                "stale-member timeout must be greater than zero".into(),
            ));
        }
        if self.members.len() > self.vbucket_count {
            return Err(CouchbaseMembershipError::Registry(format!(
                "{} members exceed {} vBuckets",
                self.members.len(),
                self.vbucket_count
            )));
        }
        for (key, member) in &self.members {
            validate_identifier("member ID", key)
                .map_err(|error| CouchbaseMembershipError::Registry(error.to_string()))?;
            validate_identifier("incarnation", &member.incarnation)
                .map_err(|error| CouchbaseMembershipError::Registry(error.to_string()))?;
            if key != &member.member_id {
                return Err(CouchbaseMembershipError::Registry(format!(
                    "member map key {key:?} does not match payload {:?}",
                    member.member_id
                )));
            }
            if member.heartbeat_at_millis < member.joined_at_millis {
                return Err(CouchbaseMembershipError::Registry(format!(
                    "member {key:?} heartbeat precedes its join time"
                )));
            }
        }
        Ok(())
    }

    /// Verifies immutable registry settings used by every member.
    ///
    /// # Errors
    ///
    /// Returns a configuration error when members disagree about vBucket or
    /// stale-timeout settings.
    pub fn validate_settings(&self, vbucket_count: usize, stale_after_millis: u64) -> Result<()> {
        self.validate()?;
        if self.vbucket_count != vbucket_count {
            return Err(CouchbaseMembershipError::Configuration(format!(
                "registry vBucket count {} does not match local {vbucket_count}",
                self.vbucket_count
            )));
        }
        if self.stale_after_millis != stale_after_millis {
            return Err(CouchbaseMembershipError::Configuration(format!(
                "registry stale timeout {}ms does not match local {stale_after_millis}ms",
                self.stale_after_millis
            )));
        }
        Ok(())
    }

    /// Registers one incarnation or idempotently refreshes the same one.
    ///
    /// # Errors
    ///
    /// Returns duplicate, capacity, generation-overflow, or registry errors.
    pub fn register(&mut self, identity: &MemberIdentity, now_millis: u64) -> Result<()> {
        self.validate()?;
        if let Some(existing) = self.members.get_mut(identity.member_id()) {
            if existing.incarnation == identity.incarnation() {
                existing.heartbeat_at_millis = existing.heartbeat_at_millis.max(now_millis);
                return Ok(());
            }
            if !is_stale(existing, now_millis, self.stale_after_millis) {
                return Err(CouchbaseMembershipError::DuplicateMember {
                    member_id: identity.member_id().to_owned(),
                });
            }
        } else if self.members.len() >= self.vbucket_count {
            return Err(CouchbaseMembershipError::Configuration(format!(
                "cannot register more than {} members for {} vBuckets",
                self.vbucket_count, self.vbucket_count
            )));
        }

        let next_generation = self.next_generation()?;
        self.members.insert(
            identity.member_id().to_owned(),
            RegistryMember {
                member_id: identity.member_id().to_owned(),
                incarnation: identity.incarnation().to_owned(),
                joined_at_millis: now_millis,
                heartbeat_at_millis: now_millis,
            },
        );
        self.generation = next_generation;
        Ok(())
    }

    /// Refreshes the exact registered incarnation.
    ///
    /// # Errors
    ///
    /// Returns a fence error if the member is absent or has been replaced.
    pub fn heartbeat(&mut self, identity: &MemberIdentity, now_millis: u64) -> Result<()> {
        self.validate()?;
        let member = self.members.get_mut(identity.member_id()).ok_or_else(|| {
            CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            }
        })?;
        if member.incarnation != identity.incarnation() {
            return Err(CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            });
        }
        member.heartbeat_at_millis = member.heartbeat_at_millis.max(now_millis);
        Ok(())
    }

    /// Removes every expired member and advances one membership generation.
    ///
    /// # Errors
    ///
    /// Returns a registry error if the document is invalid or its membership
    /// generation would overflow.
    pub fn prune_stale(&mut self, now_millis: u64) -> Result<usize> {
        self.validate()?;
        let stale_after_millis = self.stale_after_millis;
        let removed = self
            .members
            .values()
            .filter(|member| is_stale(member, now_millis, stale_after_millis))
            .count();
        if removed == 0 {
            return Ok(0);
        }
        let next_generation = self.next_generation()?;
        self.members
            .retain(|_, member| !is_stale(member, now_millis, stale_after_millis));
        self.generation = next_generation;
        Ok(removed)
    }

    /// Removes the exact local incarnation during graceful shutdown.
    ///
    /// # Errors
    ///
    /// Returns a fence or generation-overflow error.
    pub fn remove(&mut self, identity: &MemberIdentity) -> Result<bool> {
        self.validate()?;
        let Some(member) = self.members.get(identity.member_id()) else {
            return Ok(false);
        };
        if member.incarnation != identity.incarnation() {
            return Err(CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            });
        }
        let next_generation = self.next_generation()?;
        self.members.remove(identity.member_id());
        self.generation = next_generation;
        Ok(true)
    }

    /// Derives the local member's deterministic assignment from this exact
    /// registry generation.
    ///
    /// # Errors
    ///
    /// Returns a fence, invalid-registry, or assignment error.
    pub fn snapshot(&self, identity: &MemberIdentity) -> Result<MembershipSnapshot> {
        self.validate()?;
        let own = self.members.get(identity.member_id()).ok_or_else(|| {
            CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            }
        })?;
        if own.incarnation != identity.incarnation() {
            return Err(CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            });
        }
        let mut members = self.members.values().cloned().collect::<Vec<_>>();
        members.sort_by(|left, right| {
            left.joined_at_millis
                .cmp(&right.joined_at_millis)
                .then_with(|| left.member_id.cmp(&right.member_id))
        });
        let member_number = members
            .iter()
            .position(|member| member.member_id == identity.member_id)
            .map(|index| index + 1)
            .ok_or_else(|| CouchbaseMembershipError::Fenced {
                member_id: identity.member_id().to_owned(),
            })?;
        let assignment = VBucketAssignment::balanced(
            self.generation,
            self.vbucket_count,
            member_number,
            members.len(),
        )
        .map_err(|error| CouchbaseMembershipError::Registry(error.to_string()))?;
        Ok(MembershipSnapshot {
            assignment,
            members: members
                .into_iter()
                .map(|member| MemberInfo {
                    member_id: member.member_id,
                    incarnation: member.incarnation,
                    joined_at_millis: member.joined_at_millis,
                    heartbeat_at_millis: member.heartbeat_at_millis,
                })
                .collect(),
        })
    }

    fn next_generation(&self) -> Result<u64> {
        self.generation.checked_add(1).ok_or_else(|| {
            CouchbaseMembershipError::Registry("membership generation overflow".into())
        })
    }
}

fn is_stale(member: &RegistryMember, now_millis: u64, stale_after_millis: u64) -> bool {
    now_millis.saturating_sub(member.heartbeat_at_millis) >= stale_after_millis
}

fn validate_vbucket_count(vbucket_count: usize) -> Result<()> {
    let max = usize::from(u16::MAX) + 1;
    if vbucket_count == 0 || vbucket_count > max {
        return Err(CouchbaseMembershipError::Configuration(format!(
            "vBucket count must be in 1..={max}"
        )));
    }
    Ok(())
}

fn validate_identifier(kind: &str, value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_ID_LEN
        || !value.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(CouchbaseMembershipError::Configuration(format!(
            "{kind} must contain 1..={MAX_ID_LEN} visible ASCII bytes"
        )));
    }
    Ok(())
}
