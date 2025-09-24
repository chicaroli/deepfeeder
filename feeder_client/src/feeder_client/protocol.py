# feeder_client/protocol.py
ENVELOPE_VERSION: int = 1

# compatibility policy
MIN_COMPATIBLE_SERVER_VERSION: int = 1
MAX_TESTED_SERVER_VERSION: int = 1

class ProtocolVersionError(RuntimeError): ...
class ProtocolVersionWarning(UserWarning): ...

def ensure_compat(server_version: int) -> None:
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
