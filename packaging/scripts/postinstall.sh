#!/bin/sh
# postinstall for the bitcoindex .deb/.rpm packages.
#
# Deliberately minimal: the unit uses DynamicUser=yes, so there is no account
# to create, and StateDirectory= makes systemd own /var/lib/bidx. All we need
# is for systemd to notice a newly installed unit.
set -eu

if command -v systemctl >/dev/null 2>&1; then
    # Best-effort: in a container or chroot there is no running systemd, and
    # `systemctl daemon-reload` would fail. That must not fail the install.
    systemctl daemon-reload >/dev/null 2>&1 || true
fi

exit 0
