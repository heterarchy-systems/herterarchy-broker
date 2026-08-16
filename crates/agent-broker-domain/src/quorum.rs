use std::fmt;
use std::num::NonZeroUsize;

/// Error returned for an invalid Agent Broker cluster quorum configuration.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub enum QuorumPolicyError {
    /// A Broker cluster must contain at least one node.
    EmptyCluster,
}

impl fmt::Display for QuorumPolicyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Broker cluster node_count must be at least 1")
    }
}

impl std::error::Error for QuorumPolicyError {}

/// Majority quorum math shared by standalone and future replicated Broker modes.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct QuorumPolicy {
    node_count: NonZeroUsize,
}

impl QuorumPolicy {
    /// Construct quorum policy for `node_count` Broker nodes.
    ///
    /// # Errors
    ///
    /// Returns [`QuorumPolicyError::EmptyCluster`] when `node_count` is zero.
    pub fn new(node_count: usize) -> Result<Self, QuorumPolicyError> {
        NonZeroUsize::new(node_count)
            .map(|node_count| Self { node_count })
            .ok_or(QuorumPolicyError::EmptyCluster)
    }

    /// Return configured Broker node count.
    #[must_use]
    pub const fn node_count(self) -> usize {
        self.node_count.get()
    }

    /// Return majority quorum size: `floor(N / 2) + 1`.
    #[must_use]
    pub const fn majority(self) -> usize {
        (self.node_count() / 2) + 1
    }

    /// Return the number of Broker failures that can be tolerated while retaining quorum.
    #[must_use]
    pub const fn tolerated_failures(self) -> usize {
        self.node_count() - self.majority()
    }

    /// Return whether the configuration tolerates at least one Broker failure.
    #[must_use]
    pub const fn has_high_availability(self) -> bool {
        self.tolerated_failures() > 0
    }

    /// Return whether the cluster membership count is odd.
    #[must_use]
    pub const fn has_odd_membership(self) -> bool {
        self.node_count() % 2 == 1
    }
}

#[cfg(test)]
mod tests {
    use super::QuorumPolicy;

    #[test]
    fn one_node_matches_python_reference() {
        let policy = QuorumPolicy::new(1);
        assert_eq!(policy.map(QuorumPolicy::majority), Ok(1));
        assert_eq!(policy.map(QuorumPolicy::tolerated_failures), Ok(0));
        assert_eq!(policy.map(QuorumPolicy::has_high_availability), Ok(false));
        assert_eq!(policy.map(QuorumPolicy::has_odd_membership), Ok(true));
    }

    #[test]
    fn three_nodes_match_python_reference() {
        let policy = QuorumPolicy::new(3);
        assert_eq!(policy.map(QuorumPolicy::majority), Ok(2));
        assert_eq!(policy.map(QuorumPolicy::tolerated_failures), Ok(1));
        assert_eq!(policy.map(QuorumPolicy::has_high_availability), Ok(true));
        assert_eq!(policy.map(QuorumPolicy::has_odd_membership), Ok(true));
    }

    #[test]
    fn two_nodes_require_both_nodes_for_quorum() {
        let policy = QuorumPolicy::new(2);
        assert_eq!(policy.map(QuorumPolicy::majority), Ok(2));
        assert_eq!(policy.map(QuorumPolicy::tolerated_failures), Ok(0));
        assert_eq!(policy.map(QuorumPolicy::has_odd_membership), Ok(false));
    }

    #[test]
    fn zero_nodes_are_rejected() {
        assert!(QuorumPolicy::new(0).is_err());
    }
}
