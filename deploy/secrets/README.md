# Secret templates

Optional 1Password templates for the local stack's credential files. Nothing
here is required: you can write each file by hand instead. The templates
contain only 1Password references, so they are safe to commit.

`local/` holds templates for the source settings folder. Each template is named
after the file it renders, plus `.tpl`. Edit the vault, item and field names to
match your 1Password account, and replace any `<placeholder>` values, then
render it outside the checkout:

```sh
(umask 077; op inject -i deploy/secrets/local/impact_radius.json.tpl \
    -o ~/.config/analytics-platform/local/sources/impact_radius.json)
```

See [live local loads](../compose/README.md#live-local-loads-adr-0029) for where
each file goes and how the stack mounts it.
