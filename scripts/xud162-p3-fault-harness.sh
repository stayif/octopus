#!/bin/sh
set -eu

root="$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"
cd "$root"
exec go test -count=1 -run '^TestXUD162DurableSettlementFaultWindows$' ./internal/relay
