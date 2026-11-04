# shellcheck shell=sh
# summary: Run dg in a Python package's check image: dg <ingestion|transformations> [arguments]
# targets: local ci

case "${1:-}" in
    ingestion|transformations) dg_package=$1; shift ;;
    *)
        echo "Usage: scripts/platform $ANALYTICS_ENVIRONMENT dg <ingestion|transformations> [dg arguments]" >&2
        exit 2
        ;;
esac
if [ "$#" -eq 0 ]; then
    set -- --help
fi

# Check images contain the installed package, so rebuild to pick up source edits.
compose --profile checks run --rm --no-deps --build --entrypoint dg "$dg_package-check" "$@"
