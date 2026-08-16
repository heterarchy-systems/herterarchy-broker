use std::fmt;
use std::str::FromStr;

const MAX_IDENTIFIER_BYTES: usize = 128;

/// Error returned when a Broker domain identifier violates the stable identifier contract.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct IdentifierError {
    kind: &'static str,
    reason: &'static str,
}

impl fmt::Display for IdentifierError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "invalid {}: {}", self.kind, self.reason)
    }
}

impl std::error::Error for IdentifierError {}

impl IdentifierError {
    const fn new(kind: &'static str, reason: &'static str) -> Self {
        Self { kind, reason }
    }
}

fn validate_identifier(value: &str, kind: &'static str) -> Result<(), IdentifierError> {
    if value.is_empty() {
        return Err(IdentifierError::new(kind, "must not be empty"));
    }
    if value.len() > MAX_IDENTIFIER_BYTES {
        return Err(IdentifierError::new(
            kind,
            "must be at most 128 ASCII bytes",
        ));
    }

    let mut bytes = value.bytes();
    let Some(first) = bytes.next() else {
        return Err(IdentifierError::new(kind, "must not be empty"));
    };
    if !first.is_ascii_alphanumeric() {
        return Err(IdentifierError::new(
            kind,
            "must start with an ASCII alphanumeric character",
        ));
    }
    if !bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'.' | b':' | b'-'))
    {
        return Err(IdentifierError::new(
            kind,
            "contains unsupported characters",
        ));
    }
    Ok(())
}

macro_rules! define_identifier {
    ($name:ident, $kind:literal) => {
        #[doc = concat!("Validated Agent Broker ", $kind, " identifier.")]
        #[derive(Debug, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
        pub struct $name(String);

        impl $name {
            #[doc = concat!("Construct a validated ", $kind, " identifier.")]
            ///
            /// # Errors
            ///
            /// Returns [`IdentifierError`] when the identifier is empty, too long, or contains
            /// characters outside the stable ASCII identifier contract.
            pub fn new(value: impl Into<String>) -> Result<Self, IdentifierError> {
                let value = value.into();
                validate_identifier(&value, $kind)?;
                Ok(Self(value))
            }

            #[doc = concat!("Borrow this ", $kind, " identifier as text.")]
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str(&self.0)
            }
        }

        impl FromStr for $name {
            type Err = IdentifierError;

            fn from_str(value: &str) -> Result<Self, Self::Err> {
                Self::new(value)
            }
        }

        impl TryFrom<String> for $name {
            type Error = IdentifierError;

            fn try_from(value: String) -> Result<Self, Self::Error> {
                Self::new(value)
            }
        }
    };
}

define_identifier!(NamespaceId, "namespace_id");
define_identifier!(TaskId, "task_id");
define_identifier!(ConsumerGroupId, "group_id");
define_identifier!(MemberId, "member_id");
define_identifier!(LeaseId, "lease_id");

#[cfg(test)]
mod tests {
    use super::{ConsumerGroupId, NamespaceId, TaskId};

    #[test]
    fn identifiers_preserve_python_reference_contract() {
        let namespace = NamespaceId::new("project-a:default");
        assert_eq!(
            namespace.as_ref().map(NamespaceId::as_str),
            Ok("project-a:default")
        );

        let task = TaskId::new("task_01.alpha");
        assert_eq!(task.as_ref().map(TaskId::as_str), Ok("task_01.alpha"));
    }

    #[test]
    fn identifiers_reject_invalid_first_and_body_characters() {
        let invalid_first = ConsumerGroupId::new("-engineering");
        assert!(invalid_first.is_err());

        let invalid_body = ConsumerGroupId::new("engineering/team");
        assert!(invalid_body.is_err());
    }

    #[test]
    fn identifiers_enforce_reference_length_limit() {
        let valid = TaskId::new(format!("t{}", "a".repeat(127)));
        assert!(valid.is_ok());

        let invalid = TaskId::new(format!("t{}", "a".repeat(128)));
        assert!(invalid.is_err());
    }
}
