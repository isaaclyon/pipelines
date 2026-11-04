# Shared scripts agent instructions

Scope: `scripts/`. Inherit the root and parent-folder agent instructions.

Owns shared validation scripts and container lifecycle helpers.
Read the callers before changing a script and preserve per-package execution context.

## Platform commands

`scripts/platform <target> <command> [arguments]` is the single entry point for
local, CI, and deployment operations; `scripts/platform help` lists commands.

- Add each command as `scripts/platform.d/<command>.sh`. The file name is the
  command name. Start it with `# shellcheck shell=sh`, a `# summary:` line, and a
  `# targets:` line listing the targets it supports.
- The dispatcher resolves the target environment and rejects targets a command
  does not list. Keep target validation and environment resolution in the
  dispatcher; do not repeat them in command files.
- Command files are sourced by the dispatcher. Use the `compose` function so
  commands use the target's Compose files, env file, and project name. Arguments
  after the command name are available as `"$@"`.
- Add a command when it encodes ordering, safety rules, or target knowledge.
  Use `scripts/platform <target> compose ...` for plain Compose operations.
- Keep scripts POSIX `sh` and ShellCheck-clean; CI runs ShellCheck on `scripts/`.

## Host setup

`setup-host.sh` prepares a fresh Ubuntu VM for one environment and runs as root
on that VM, not through `scripts/platform`. Keep every step compare-then-change
so a second run changes nothing, and do not name a data source in it. Its test,
`test-setup-host.sh`, runs the real script in a container with stubs for apt,
systemd, ufw and swap; add a stub when the script gains a command the container
cannot run.

## Secret rendering

`render-secrets.sh` runs on the owner's workstation, not on the VM. It reads
`deploy/secrets/<environment>/` and must never print a value, validate everything
before changing the host, and replace files atomically. Its test,
`test-render-secrets.sh`, uses stub `op` and `ssh`; keep it free of real
credentials.
