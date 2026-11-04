Rules for pi-warden in the Analytics Platform repository. Each `#` heading below is one rule, and the text under it is what
Jev judges a write or an edit against. The canonical file is `.agents/pi-warden.md`; the root `pi-warden.md` links to it.

# No hardcoded secrets
Source code must not contain passwords, API keys, tokens, or connection URLs with credentials.
Read them from the environment, a function parameter, or the config module. A test fixture with an obviously fake value
such as `test-token` is fine.

# Comments explain why, not what
A comment states a reason, a constraint, a workaround, or a non-obvious invariant. A comment
that restates what the next line plainly does is a violation.

# Errors are not swallowed
A `catch` block (in Python, an `except` block) must handle the error, report it, or re-raise it. An empty catch block,
one whose body is only `pass`, or one whose body is only a comment, is a violation.

# No partial implementations
Implement features fully. A comment that says "for now", "simplified", "later", "TODO", or "placeholder" is a
violation. So is a stub body: a function or method whose whole body is `pass`, `...`, `raise NotImplementedError`, or a
hardcoded placeholder return such as `return []` or `return None` beside a comment about future work.

# Do not run destructive commands that erase uncommitted work
`git reset --hard`, `git checkout -- .`, `git clean -fd`, and similar commands that discard
untracked or uncommitted changes are forbidden. These destroy work that has no backup. If a
clean tree is needed, create a worktree instead or ask the user.

# Secrets stay out of output
source: AGENTS.md § Working and validating changes
Keep secrets out of output. Code must not print, log, or echo a credential: `print(password)`, a log message that
interpolates a token or key, `echo $SNOWFLAKE_PASSWORD`, or `set -x` around commands that expand credentials are
violations. Log the setting's name or whether it is present, never its value.

# Live commands require explicit environment selection
source: AGENTS.md § Working and validating changes
paths: scripts/**, deploy/**
A command that runs live integration or deployment work against Snowflake, Shopify, or a remote host must take the target
environment explicitly. Falling back to a default live environment, such as `${ANALYTICS_ENVIRONMENT:-production}` or a
`default="dev"` for a deploy target, is a violation. Fail with a message when the environment is missing, as
`${ANALYTICS_ENV_FILE:?...}` does. A default that points at local example configuration is fine.

# Ordinary checks use offline fixtures
source: AGENTS.md § Working and validating changes
paths: packages/*/tests/**, scripts/test_*.py
Tests run offline against fixtures and fakes. A test that opens a real connection, such as `snowflake.connector.connect(`,
an HTTP request to a `myshopify.com` host, or reads real credentials such as `SNOWFLAKE_PASSWORD` from the environment,
is a violation.

# Packages import only public names from sibling packages
source: AGENTS.md § Working and validating changes
paths: packages/**
Use public APIs rather than sibling internals. An import of an underscore-prefixed module or name from another package,
such as `from analytics_transformations._bootstrap import ...` in ingestion code or `from analytics_ingestion.shopify
import _parse` in transformations code, is a violation. So is reaching into another package's files with a relative path
such as `../transformations/` or `../ingestion/`, or adding another package's `src` folder to `sys.path`.

# No shared workspace across packages
source: AGENTS.md § Working and validating changes
paths: pyproject.toml, package.json
Keep package dependencies, lockfiles, and environments independent. A `[tool.uv.workspace]` table, a `[tool.uv.sources]`
entry with `workspace = true`, or an npm `workspaces` field is a violation, because each joins packages into one shared
environment and lockfile.

# Agent configuration is edited in .agents/
source: AGENTS.md § Shared agent configuration
paths: .pi/**, .claude/**, .mcp.json
Edit all agentic items, including skills, MCP configuration, and agent instructions, in `.agents/`. Writing a skill, an
`AGENTS.md`, `mcp.json`, or other agent instructions as a regular file under `.pi/` or `.claude/` is a violation; those
paths are symlinks to the canonical files in `.agents/`. Runtime state written by tools, such as
`.pi/schedule-prompts.json`, is fine.

# Folder AGENTS.md guides live in .agents/instructions/
source: AGENTS.md § Shared agent configuration
paths: packages/**/AGENTS.md, deploy/**/AGENTS.md, docs/**/AGENTS.md, scripts/**/AGENTS.md
Scoped folder guides live in `.agents/instructions/`, mirroring their repository folder paths, with an `AGENTS.md`
symlink in each owning folder. Writing guide content to an `AGENTS.md` under `packages/`, `deploy/`, `docs/`, or
`scripts/` is a violation; edit the canonical target under `.agents/instructions/` instead.
