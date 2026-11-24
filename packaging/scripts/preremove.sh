#!/bin/sh
# preremove for the bitcoindex .deb/.rpm packages.
#
# On a real removal (not an upgrade) stop the unit so the binary is not held
# open. rpm passes "0" as the first argument on uninstall; deb passes
# "remove" or "purge". Anything else is an upgrade, where the service should
# keep running.
set -eu

action="${1:-}"

case "$action" in
    0|remove|purge)
        if command -v systemctl >/dev/null 2>&1; then
            systemctl stop bidx-serve.service >/dev/null 2>&1 || true
            systemctl disable bidx-serve.service >/dev/null 2>&1 || true
        fi
        ;;
    *)
        # upgrade: leave the running service alone
        ;;
esac

exit 0
