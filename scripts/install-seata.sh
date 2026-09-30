#!/usr/bin/env bash
# Builds the Seata jars this project compiles against: the Sonata fork of Seata
# (branch integrate-sonata-in-xa-mode) plus the patches in seata-patches/, installed
# into ~/.m2 as 2.6.0-SNAPSHOT.
#
# The fork checkout is never modified: it is cloned into a temporary directory first.
#
#   scripts/install-seata.sh [path-or-url-of-the-sonata-fork]   (default: ../incubator-seata)
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
fork="${1:-$repo_root/../incubator-seata}"
branch="integrate-sonata-in-xa-mode"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git clone --quiet --branch "$branch" "$fork" "$work/seata"
cd "$work/seata"
git -c user.name=coshim -c user.email=coshim@localhost am --quiet "$repo_root"/seata-patches/*.patch

./mvnw -q -B install -DskipTests \
    -Dcheckstyle.skip -Dspotless.check.skip=true -Dspotless.apply.skip=true -Dlicense.skip=true -Drat.skip=true \
    -pl rm-datasource,tm,mock-server -am

echo "Installed patched Seata ($(git log --oneline -1)) into ~/.m2"
