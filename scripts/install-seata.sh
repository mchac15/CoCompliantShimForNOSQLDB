#!/usr/bin/env bash
# Builds the Seata jars this project compiles against: the Sonata fork of Seata
# (branch integrate-sonata-in-xa-mode) plus the patches in seata-patches/, installed
# into ~/.m2 as 2.6.0-SNAPSHOT.
#
# The fork checkout is never modified: it is cloned into a temporary directory first.
#
#   scripts/install-seata.sh [--with-acta] [path-or-url-of-the-sonata-fork]   (default: ../incubator-seata)
#
# --with-acta also builds the shaded seata-all and seata-spring-boot-starter, the artifacts the Acta
# benchmark server (../acta-server) depends on, so that Acta runs the same patched Seata as this
# project. Acta pins the same fork commit in its dependency/seata submodule; its own
# scripts/build-seata.sh would install the same version without our patches, so run this script
# after it, never before.
set -euo pipefail

repo_root="$(cd "$(dirname "$0")/.." && pwd)"
modules="rm-datasource,tm,mock-server"
if [[ "${1:-}" == "--with-acta" ]]; then
    modules="$modules,all,seata-spring-boot-starter"
    shift
fi
fork="${1:-$repo_root/../incubator-seata}"
branch="integrate-sonata-in-xa-mode"
work="$(mktemp -d)"
trap 'rm -rf "$work"' EXIT

git clone --quiet --branch "$branch" "$fork" "$work/seata"
cd "$work/seata"
git -c user.name=coshim -c user.email=coshim@localhost am --quiet "$repo_root"/seata-patches/*.patch

./mvnw -q -B install -DskipTests \
    -Dcheckstyle.skip -Dspotless.check.skip=true -Dspotless.apply.skip=true -Dlicense.skip=true -Drat.skip=true \
    -Djacoco.skip=true -Dpmd.skip=true -Dsource.skip=true \
    -pl "$modules" -am

echo "Installed patched Seata ($(git log --oneline -1)) into ~/.m2"
