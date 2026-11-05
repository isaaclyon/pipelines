"""Classify a Git diff and check changed documentation without Docker dependencies."""

import json
import re
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit


def is_doc(name: str) -> bool:
    path = Path(name)
    return name in {
        "README.md",
        "REVIEW.md",
        "engineering-standards.md",
        "AGENTS.md",
    } or (
        path.suffix == ".md"
        and (
            name.startswith(("docs/", ".agents/instructions/"))
            or name == ".agents/AGENTS.md"
            or (
                path.name in {"README.md", "AGENTS.md"}
                and name.startswith(
                    (
                        "packages/",
                        "examples/",
                        "deploy/",
                        "scripts/",
                        ".pi/",
                        ".claude/",
                    )
                )
            )
        )
    )


PACKAGES = ("ingestion", "transformations", "metrics", "client-config")
# The sample client's checks, run on images built from the toolkit's
# transformations and metrics packages.
SAMPLE_CLIENT = "sample-client"
LANES = (*PACKAGES, SAMPLE_CLIENT)


def select_lanes(names: list[str]) -> tuple[list[str], bool]:
    selected: set[str] = set()
    integration = False
    if not names:
        return list(LANES), True
    for name in names:
        if is_doc(name):
            continue
        # Every Python package and image depends on the client configuration.
        if name.startswith("packages/client-config/") and not name.startswith(
            "packages/client-config/tests/"
        ):
            return list(LANES), True
        # The local stack runs the sample client, and the transformations
        # tests check its dbt project.
        if name.startswith("examples/sample-client/"):
            selected |= {"transformations", SAMPLE_CLIENT}
            integration = True
            continue
        for package in PACKAGES:
            if name.startswith(f"packages/{package}/"):
                selected.add(package)
                runtime = not name.startswith(f"packages/{package}/tests/")
                integration |= runtime
                if runtime and package in ("transformations", "metrics"):
                    selected.add(SAMPLE_CLIENT)
                break
        else:
            # Shared tooling, deployment, new packages, and unknown files fan out.
            return list(LANES), True
    return [lane for lane in LANES if lane in selected], integration


TRANSFORMATIONS = "packages/transformations/"
# dbt state comparison covers these project folders (ADR 0026).
DBT_NODE_FOLDERS = ("models/", "macros/", "tests/", "seeds/", "snapshots/", "analyses/")
# The check environment and the routing itself.
DBT_ENVIRONMENT = {
    "deploy/compose/Dockerfile.python",
    "deploy/compose/local.yaml",
    "deploy/compose/ci.yaml",
    "scripts/check-python.sh",
    "scripts/platform",
    "scripts/platform.d/check.sh",
    "scripts/ci_docs.py",
}
DBT_CHECK_ORDER = ("none", "changed", "all")


def dbt_checks(name: str) -> str:
    """Which dbt project tests a changed path needs: none, changed, or all."""
    if name in DBT_ENVIRONMENT:
        return "all"
    if is_doc(name) or not name.startswith(TRANSFORMATIONS):
        return "none"
    path = name.removeprefix(TRANSFORMATIONS)
    if path.startswith("dbt/"):
        nodes = path.removeprefix("dbt/").startswith(DBT_NODE_FOLDERS)
        return "changed" if nodes else "all"
    # Tests compare the Python modules they import with the base.
    if path.startswith(("tests/", "src/")) and not path.startswith("tests/fixtures/"):
        return "changed" if path.endswith(".py") else "all"
    return "all"


def select_dbt_checks(names: list[str]) -> str:
    if not names:
        return "all"
    return max((dbt_checks(name) for name in names), key=DBT_CHECK_ORDER.index)


def git_paths(*args: str) -> list[str]:
    result = subprocess.run(["git", *args], check=True, capture_output=True)
    return [name.decode() for name in result.stdout.split(b"\0") if name]


def changed_paths(base: str, head: str | None = None) -> list[str]:
    """Paths that differ from base: in head, or in the working tree."""
    # No rename detection: both sides must qualify before skipping runtime checks.
    if head is not None:
        return git_paths("diff", "--no-renames", "--name-only", "-z", base, head)
    return git_paths("diff", "--no-renames", "--name-only", "-z", base) + git_paths(
        "ls-files", "--others", "--exclude-standard", "-z"
    )


def check_docs(names: list[str]) -> None:
    tracked = subprocess.run(
        ["git", "ls-files", "-z"],
        check=True,
        capture_output=True,
    ).stdout.split(b"\0")
    for name in tracked:
        if name:
            path = Path(name.decode())
            if path.parts[0] == ".agent":
                raise ValueError(f"Use .agents/ instead of .agent/: {path}")
            if path.is_symlink() and not path.exists():
                raise ValueError(f"Broken symlink: {path}")
    for name in names:
        path = Path(name)
        if path.suffix != ".md" or not path.is_file():
            continue
        path = path.resolve()
        contents = re.sub(r"```.*?```", "", path.read_text(), flags=re.DOTALL)
        contents = re.sub(r"`[^`\n]*`", "", contents)
        # Local inline and reference links outside code; external URLs and
        # heading anchors are intentionally excluded from this deterministic,
        # offline check.
        links = list(re.finditer(r"\]\(<?([^\s)>]+)", contents))
        links += list(
            re.finditer(r"^\s*\[[^\]]+\]:\s*<?([^\s>]+)", contents, re.MULTILINE)
        )
        for match in links:
            link = match.group(1)
            parsed = urlsplit(link)
            if parsed.scheme or parsed.netloc or not parsed.path:
                continue
            if not (path.parent / unquote(parsed.path)).exists():
                raise ValueError(f"{name}: missing link target {link}")


def main() -> None:
    """Route BASE HEAD, BASE and the working tree, or (no base) everything."""
    arguments = sys.argv[1:]
    if len(arguments) > 2:
        print("Usage: ci_docs.py [BASE [HEAD]]", file=sys.stderr)
        sys.exit(2)
    names: list[str] = []
    if arguments:
        try:
            names = changed_paths(*arguments)
        except (subprocess.CalledProcessError, UnicodeError):
            pass
    # An unknown change runs every check, including every Markdown link.
    check_docs(names or git_paths("ls-files", "-z", "*.md"))
    print(
        f"docs_only={str(bool(names) and all(is_doc(name) for name in names)).lower()}"
    )
    packages, integration = select_lanes(names)
    print(f"packages={json.dumps(packages)}")
    print(f"integration={str(integration).lower()}")
    print(f"dbt_checks={select_dbt_checks(names)}")


if __name__ == "__main__":
    main()
