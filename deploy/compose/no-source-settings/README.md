# No source settings

The local stack mounts this empty folder at `/run/secrets/sources` when
`SOURCE_SETTINGS_HOST_DIR` is unset. Any kit source that the client's
`client.yaml` lists then fails before it reads anything. Do not put settings
files here; keep them outside the checkout (see
[Live local loads](../README.md#live-local-loads-adr-0029)).
