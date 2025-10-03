"""
feeder_client.protocol: Protocol versioning and compatibility utilities for DeepFeeder.

Contains:
    - ENVELOPE_VERSION: Current envelope protocol version.
    - ensure_compat: Checks server protocol compatibility.
    - ProtocolVersionError, ProtocolVersionWarning: Exceptions for protocol mismatches.
"""

ENVELOPE_VERSION: int = 1

# compatibility policy
MIN_COMPATIBLE_SERVER_VERSION: int = 1
MAX_TESTED_SERVER_VERSION: int = 1

class ProtocolVersionError(RuntimeError):
    """Raised when the server protocol version is too old for this client."""

class ProtocolVersionWarning(UserWarning):
    """Warning for untested but possibly compatible server protocol versions."""

def ensure_compat(server_version: int) -> None:
    """
    Raise if the server_version is not compatible with this client.
    Warn if the server_version is newer than tested.
    """
    if server_version < MIN_COMPATIBLE_SERVER_VERSION:
        raise ProtocolVersionError(
            f"Server protocol {server_version} < min compatible {MIN_COMPATIBLE_SERVER_VERSION}"
        )
    if server_version > MAX_TESTED_SERVER_VERSION:
        import warnings
        warnings.warn(
            f"Server protocol {server_version} > max tested {MAX_TESTED_SERVER_VERSION}; "
            "attempting best-effort parse.",
            ProtocolVersionWarning,
            stacklevel=2,
        )
