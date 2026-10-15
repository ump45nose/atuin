//! Atuin's GRPC daemon transport layer.
//!
//! The client talks to the daemon over GRPC. This module does all of that.

use crate::{PROTOCOL_VERSION, VERSION};

/// Trait for reply types that include the daemon version and protocol version.
pub trait VersionedReply {
    fn version(&self) -> &str;
    fn protocol(&self) -> u32;

    /// Check that this client can talk to the daemon that sent the reply: one on this build, or a
    /// newer one on this protocol.
    fn ensure_compatible(&self) -> Result<(), VersionMismatch> {
        let (version, protocol) = (self.version(), self.protocol());
        if version == VERSION && protocol == PROTOCOL_VERSION {
            return Ok(());
        }

        // Only an older daemon is replaced: with two atuin versions installed, replacing on any
        // mismatch has each one restart the daemon out from under the other on every command.
        let newer = matches!(
            (semver::Version::parse(version), semver::Version::parse(VERSION)),
            (Ok(daemon), Ok(ours)) if daemon > ours
        );

        match (newer, protocol == PROTOCOL_VERSION) {
            (true, true) => Ok(()),
            (true, false) => Err(VersionMismatch::Newer {
                version: version.to_owned(),
                protocol,
            }),
            (false, true) => Err(VersionMismatch::OutdatedVersion {
                version: version.to_owned(),
            }),
            (false, false) => Err(VersionMismatch::OutdatedProtocol { protocol }),
        }
    }
}

/// A daemon this client can't talk to, as [`VersionedReply::ensure_compatible`] finds it.
#[derive(Debug, PartialEq, Eq, thiserror::Error)]
pub enum VersionMismatch {
    /// An older daemon on this protocol, or one whose version doesn't parse.
    #[error("daemon is out of date: expected {VERSION}, got {version}")]
    OutdatedVersion {
        version: String,
    },
    /// A daemon on another protocol that isn't newer than this client.
    #[error("daemon protocol mismatch: expected {PROTOCOL_VERSION}, got {protocol}")]
    OutdatedProtocol {
        protocol: u32,
    },
    #[error(
        "daemon {version} is newer than this client ({VERSION}) and speaks protocol {protocol}, \
         expected {PROTOCOL_VERSION}; leaving it running"
    )]
    Newer {
        version: String,
        protocol: u32,
    },
}

impl VersionMismatch {
    /// Whether the daemon is older than this client, and so one a client may replace.
    #[must_use]
    pub const fn is_outdated(&self) -> bool {
        match self {
            Self::OutdatedVersion { .. } | Self::OutdatedProtocol { .. } => true,
            Self::Newer { .. } => false,
        }
    }
}

/// Mark a reply as versioned.
///
/// Versioned replies are protobuf messages that have a `version` field and a `protocol` field. This
/// will implement the [`VersionedReply`] trait for the given messages.
macro_rules! versioned_messages {
    ($($name:ident),* $(,)?) => {
        $(
            impl crate::grpc::VersionedReply for $name {
                fn version(&self) -> &str {
                    &self.version
                }

                fn protocol(&self) -> u32 {
                    self.protocol
                }
            }
        )*
    };
}

/// Mark a [`thiserror::Error`] type as an invalid argument error.
///
/// Invalid argument errors receive an [`Into`] implementation into [`tonic::Status`] which returns
/// the error as [`tonic::Status::invalid_argument`].
macro_rules! invalid_argument_errors {
    ($($err:ty),* $(,)?) => {
        $(
            impl From<$err> for tonic::Status {
                fn from(value: $err) -> Self {
                    Self::invalid_argument(value.to_string())
                }
            }
        )*
    };
}

/// Mark a [`thiserror::Error`] type as an internal error.
///
/// Invalid argument errors receive an [`Into`] implementation into [`tonic::Status`] which returns
/// the error as [`tonic::Status::internal`].
macro_rules! internal_errors {
    ($($err:ty),* $(,)?) => {
        $(
            impl From<$err> for tonic::Status {
                fn from(value: $err) -> Self {
                    Self::internal(value.to_string())
                }
            }
        )*
    };
}

pub mod ai;
pub mod common;
pub mod history;

pub use ai::session::Service as AiSessionService;
pub use history::Service as HistoryService;

#[cfg(test)]
mod tests {
    use rstest::rstest;

    use super::*;
    use crate::grpc::history::pb::StatusReply;

    #[rstest]
    #[case::matches(VERSION, PROTOCOL_VERSION, Ok(()))]
    #[case::older("0.0.0", PROTOCOL_VERSION, Err(VersionMismatch::OutdatedVersion { version: "0.0.0".to_owned() }))]
    #[case::older_protocol("0.0.0", 999, Err(VersionMismatch::OutdatedProtocol { protocol: 999 }))]
    #[case::same_version_other_protocol(VERSION, 999, Err(VersionMismatch::OutdatedProtocol { protocol: 999 }))]
    #[case::newer("999.0.0", PROTOCOL_VERSION, Ok(()))]
    #[case::newer_protocol("999.0.0", 999, Err(VersionMismatch::Newer { version: "999.0.0".to_owned(), protocol: 999 }))]
    #[case::unparsable("dev", PROTOCOL_VERSION, Err(VersionMismatch::OutdatedVersion { version: "dev".to_owned() }))]
    fn ensure_compatible_cases(
        #[case] version: &str,
        #[case] protocol: u32,
        #[case] expected: Result<(), VersionMismatch>,
    ) {
        let reply = StatusReply {
            version: version.to_owned(),
            protocol,
            ..StatusReply::default()
        };
        assert_eq!(reply.ensure_compatible(), expected);
    }
}
