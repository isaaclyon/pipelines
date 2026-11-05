"""Check a client configuration file: ``analytics-client-config check PATH``.

Prints the enabled sources on success. On failure prints the error, which names
the file and field, and exits 1.
"""

import sys
from pathlib import Path

from analytics_client_config import ConfigError, parse


def main(argv: list[str] | None = None) -> int:
    args = sys.argv[1:] if argv is None else argv
    if len(args) != 2 or args[0] != "check":
        print("usage: analytics-client-config check PATH", file=sys.stderr)
        return 2
    path = Path(args[1])
    try:
        config = parse(path.read_text(), str(path))
    except OSError:
        print(f"{path}: client configuration is unreadable", file=sys.stderr)
        return 1
    except ConfigError as error:
        print(error, file=sys.stderr)
        return 1
    sources = ", ".join(config.sources.enabled) or "none"
    print(f"{path}: client {config.client} is valid; sources: {sources}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
