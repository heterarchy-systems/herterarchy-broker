use std::error::Error;
use std::fmt;
use std::num::NonZeroUsize;

const MIN_LEASE_DURATION_MS: u64 = 1_000;
const MAX_LEASE_DURATION_MS: u64 = 30 * 60 * 1_000;
const MAX_MAINTENANCE_BATCH: usize = 4_096;

/// Validation failure for bounded Broker policy values.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum PolicyError {
    /// Lease duration is outside the Python reference implementation's accepted range.
    LeaseDurationOutOfRange,
    /// A maintenance batch is zero or exceeds the fixed safety ceiling.
    MaintenanceBatchOutOfRange { field: &'static str },
    /// A hot-state capacity is zero.
    ZeroCapacity { field: &'static str },
}

impl fmt::Display for PolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::LeaseDurationOutOfRange => formatter
                .write_str("lease_duration_ms must be between 1000 and 1800000 milliseconds"),
            Self::MaintenanceBatchOutOfRange { field } => write!(
                formatter,
                "{field} must be between 1 and {MAX_MAINTENANCE_BATCH}"
            ),
            Self::ZeroCapacity { field } => write!(formatter, "{field} must be positive"),
        }
    }
}

impl Error for PolicyError {}

/// Validated Task lease duration in milliseconds.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct LeaseDurationMs(u64);

impl LeaseDurationMs {
    /// Validate a lease duration against the stable Broker request contract.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError::LeaseDurationOutOfRange`] outside `1000..=1800000` ms.
    pub const fn new(value: u64) -> Result<Self, PolicyError> {
        if value < MIN_LEASE_DURATION_MS || value > MAX_LEASE_DURATION_MS {
            return Err(PolicyError::LeaseDurationOutOfRange);
        }
        Ok(Self(value))
    }

    /// Return the validated duration in milliseconds.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

macro_rules! define_maintenance_limit {
    ($name:ident, $field:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(NonZeroUsize);

        impl $name {
            #[doc = concat!("Validate ", $field, ".")]
            ///
            /// # Errors
            ///
            /// Returns [`PolicyError::MaintenanceBatchOutOfRange`] outside `1..=4096`.
            pub fn new(value: usize) -> Result<Self, PolicyError> {
                if value == 0 || value > MAX_MAINTENANCE_BATCH {
                    return Err(PolicyError::MaintenanceBatchOutOfRange { field: $field });
                }
                let Some(value) = NonZeroUsize::new(value) else {
                    return Err(PolicyError::MaintenanceBatchOutOfRange { field: $field });
                };
                Ok(Self(value))
            }

            #[doc = concat!("Return validated ", $field, ".")]
            #[must_use]
            pub const fn get(self) -> usize {
                self.0.get()
            }
        }
    };
}

define_maintenance_limit!(
    ReapMemberLimit,
    "max_members",
    "Bounded stale-member reap limit."
);
define_maintenance_limit!(
    PruneTaskLimit,
    "max_tasks",
    "Bounded completed-Task pruning limit."
);

/// Validated hot-state capacity policy matching the Python reference defaults.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct BrokerCapacityPolicy {
    namespaces: usize,
    tasks_per_namespace: usize,
    groups_per_namespace: usize,
    members_per_group: usize,
}

impl Default for BrokerCapacityPolicy {
    fn default() -> Self {
        Self {
            namespaces: 64,
            tasks_per_namespace: 4_096,
            groups_per_namespace: 64,
            members_per_group: 256,
        }
    }
}

impl BrokerCapacityPolicy {
    /// Construct a custom validated hot-state capacity policy.
    ///
    /// # Errors
    ///
    /// Returns [`PolicyError::ZeroCapacity`] when any capacity is zero.
    pub fn new(
        max_namespaces: usize,
        max_tasks_per_namespace: usize,
        max_groups_per_namespace: usize,
        max_members_per_group: usize,
    ) -> Result<Self, PolicyError> {
        Ok(Self {
            namespaces: positive_capacity(max_namespaces, "max_namespaces")?,
            tasks_per_namespace: positive_capacity(
                max_tasks_per_namespace,
                "max_tasks_per_namespace",
            )?,
            groups_per_namespace: positive_capacity(
                max_groups_per_namespace,
                "max_groups_per_namespace",
            )?,
            members_per_group: positive_capacity(max_members_per_group, "max_members_per_group")?,
        })
    }

    #[must_use]
    pub const fn max_namespaces(self) -> usize {
        self.namespaces
    }

    #[must_use]
    pub const fn max_tasks_per_namespace(self) -> usize {
        self.tasks_per_namespace
    }

    #[must_use]
    pub const fn max_groups_per_namespace(self) -> usize {
        self.groups_per_namespace
    }

    #[must_use]
    pub const fn max_members_per_group(self) -> usize {
        self.members_per_group
    }
}

fn positive_capacity(value: usize, field: &'static str) -> Result<usize, PolicyError> {
    if value == 0 {
        return Err(PolicyError::ZeroCapacity { field });
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::{BrokerCapacityPolicy, LeaseDurationMs, PruneTaskLimit, ReapMemberLimit};

    #[test]
    fn capacity_defaults_match_python_reference() {
        let policy = BrokerCapacityPolicy::default();
        assert_eq!(policy.max_namespaces(), 64);
        assert_eq!(policy.max_tasks_per_namespace(), 4_096);
        assert_eq!(policy.max_groups_per_namespace(), 64);
        assert_eq!(policy.max_members_per_group(), 256);
    }

    #[test]
    fn lease_duration_bounds_match_python_reference() {
        assert!(LeaseDurationMs::new(999).is_err());
        assert_eq!(
            LeaseDurationMs::new(1_000).map(LeaseDurationMs::get),
            Ok(1_000)
        );
        assert_eq!(
            LeaseDurationMs::new(1_800_000).map(LeaseDurationMs::get),
            Ok(1_800_000)
        );
        assert!(LeaseDurationMs::new(1_800_001).is_err());
    }

    #[test]
    fn maintenance_batch_limits_match_python_reference() {
        assert!(PruneTaskLimit::new(0).is_err());
        assert_eq!(
            PruneTaskLimit::new(4_096).map(PruneTaskLimit::get),
            Ok(4_096)
        );
        assert!(PruneTaskLimit::new(4_097).is_err());
        assert!(ReapMemberLimit::new(0).is_err());
        assert_eq!(
            ReapMemberLimit::new(4_096).map(ReapMemberLimit::get),
            Ok(4_096)
        );
        assert!(ReapMemberLimit::new(4_097).is_err());
    }
}
