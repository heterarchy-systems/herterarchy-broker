use std::fmt;
use std::num::NonZeroU64;

/// Error returned when a monotonic fencing counter cannot advance safely.
#[derive(Debug, Clone, Eq, PartialEq)]
pub enum FencingValueError {
    /// Broker terms are one-based and therefore reject zero.
    ZeroTerm,
    /// A bounded Rust counter reached `u64::MAX`; continuing would break monotonic fencing.
    Overflow { counter: &'static str },
}

impl fmt::Display for FencingValueError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroTerm => formatter.write_str("broker term must be at least 1"),
            Self::Overflow { counter } => write!(
                formatter,
                "{counter} overflowed and cannot advance without violating monotonic fencing"
            ),
        }
    }
}

impl std::error::Error for FencingValueError {}

/// Monotonic Broker leader/consensus term. Terms are always at least one.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Term(NonZeroU64);

impl Term {
    /// Initial standalone/cluster term used by the Python reference implementation.
    pub const INITIAL: Self = Self(NonZeroU64::MIN);

    /// Construct a Broker term.
    ///
    /// # Errors
    ///
    /// Returns [`FencingValueError::ZeroTerm`] when `value` is zero.
    pub fn new(value: u64) -> Result<Self, FencingValueError> {
        NonZeroU64::new(value)
            .map(Self)
            .ok_or(FencingValueError::ZeroTerm)
    }

    /// Return the raw term value for serialization or protocol boundaries.
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0.get()
    }

    /// Advance to the next term while preserving monotonicity.
    ///
    /// # Errors
    ///
    /// Returns [`FencingValueError::Overflow`] if the term cannot advance.
    pub fn next(self) -> Result<Self, FencingValueError> {
        self.get()
            .checked_add(1)
            .ok_or(FencingValueError::Overflow { counter: "term" })
            .and_then(Self::new)
    }
}

impl Default for Term {
    fn default() -> Self {
        Self::INITIAL
    }
}

macro_rules! define_counter {
    ($name:ident, $counter:literal, $doc:literal) => {
        #[doc = $doc]
        #[derive(Debug, Copy, Clone, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(u64);

        impl $name {
            #[doc = concat!("Construct a ", $counter, " value.")]
            #[must_use]
            pub const fn new(value: u64) -> Self {
                Self(value)
            }

            #[doc = concat!("Return the raw ", $counter, " value.")]
            #[must_use]
            pub const fn get(self) -> u64 {
                self.0
            }

            #[doc = concat!("Advance the ", $counter, " monotonically.")]
            ///
            /// # Errors
            ///
            /// Returns [`FencingValueError::Overflow`] if the counter cannot advance.
            pub fn next(self) -> Result<Self, FencingValueError> {
                self.0
                    .checked_add(1)
                    .map(Self)
                    .ok_or(FencingValueError::Overflow { counter: $counter })
            }
        }
    };
}

define_counter!(
    Generation,
    "generation",
    "Consumer Group membership generation used to fence stale members."
);
define_counter!(
    LeaseEpoch,
    "lease_epoch",
    "Task lease epoch used to fence previous lease holders after reassignment."
);
define_counter!(
    Revision,
    "revision",
    "Monotonic entity or Broker state revision used for optimistic ordering and replay."
);

#[cfg(test)]
mod tests {
    use super::{Generation, LeaseEpoch, Revision, Term};

    #[test]
    fn term_is_one_based_and_monotonic() {
        assert!(Term::new(0).is_err());
        let term = Term::new(1);
        assert_eq!(term.as_ref().map(|value| value.get()), Ok(1));
        assert_eq!(term.and_then(Term::next).map(Term::get), Ok(2));
    }

    #[test]
    fn generations_and_epochs_start_at_zero_like_python_reference() {
        assert_eq!(Generation::default().get(), 0);
        assert_eq!(LeaseEpoch::default().get(), 0);
        assert_eq!(Revision::default().get(), 0);
        assert_eq!(Generation::default().next().map(Generation::get), Ok(1));
    }
}
