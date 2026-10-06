#!/usr/bin/env bash
# Starts / stops the MySQL or PostgreSQL servers that Sonata's benchmark variants run on, in Docker,
# one server per participant (as one shim node per shim database), on the host network like Acta.
#
#   scripts/bench-dbs.sh start mysql|postgres N    # servers on PORT, PORT+1, ..., database "acta"
#   scripts/bench-dbs.sh stop  mysql|postgres      # removes them (and their data)
#   scripts/bench-dbs.sh urls  mysql|postgres N    # prints the JDBC URLs, comma separated
#   scripts/bench-dbs.sh createdb mysql|postgres I DB   # creates database DB on server I if missing
#                                                     # (the shim's SqlKvStore: one per shim database)
#
# Configuration: the defaults of Acta's deployment (../acta-server/scripts/deploy, ansible
# group_vars/all.example.yml), which is how Sonata is evaluated, with one change: no fsync, so that
# the databases, like the shim's in-memory store, do not pay for durability:
#   MySQL: innodb_flush_log_at_trx_commit=0, sync_binlog=0, innodb_flush_method=nosync
#   PG:    fsync=off, synchronous_commit=off, full_page_writes=off
# Memory sizes are scaled down to fit one machine (Acta's are 32G buffer pool / 32GB shared_buffers
# per server); set them back with the variables below on a large machine.
#
# Env: MYSQL_PORT (3306), PG_PORT (5432), MYSQL_IMAGE (mysql:8.4), POSTGRES_IMAGE (postgres:16),
#      MYSQL_BUFFER_POOL (2G), PG_SHARED_BUFFERS (2GB), PG_SHM_SIZE (4g).
set -euo pipefail

cmd="${1:-}"
kind="${2:-}"
count="${3:-1}"
mysql_port="${MYSQL_PORT:-3306}"
pg_port="${PG_PORT:-5432}"

usage() {
    echo "usage: $0 start|stop|urls mysql|postgres [N] | createdb mysql|postgres I DB" >&2
    exit 2
}

case "$kind" in
    mysql) base_port="$mysql_port" ;;
    postgres) base_port="$pg_port" ;;
    *) usage ;;
esac

name() {
    echo "coshim-bench-$kind-$1"
}

urls() {
    local list=""
    for ((i = 0; i < count; i++)); do
        list+="${list:+,}jdbc:$( [[ $kind == mysql ]] && echo mysql || echo postgresql )://127.0.0.1:$((base_port + i))/acta"
    done
    echo "$list"
}

start_mysql() {
    local i="$1" port=$((base_port + $1))
    docker run -d --rm --name "$(name "$i")" --network host \
        -e MYSQL_ALLOW_EMPTY_PASSWORD=yes -e MYSQL_DATABASE=acta \
        "${MYSQL_IMAGE:-mysql:8.4}" \
        --bind-address=127.0.0.1 --port="$port" --mysqlx=OFF \
        --max-connections=6000 \
        --innodb-lock-wait-timeout=60 \
        --innodb-use-native-aio=OFF \
        --innodb-buffer-pool-size="${MYSQL_BUFFER_POOL:-2G}" \
        --innodb-redo-log-capacity=4G \
        --innodb-flush-log-at-trx-commit=0 --sync-binlog=0 --innodb-flush-method=nosync > /dev/null
}

start_postgres() {
    local i="$1" port=$((base_port + $1))
    docker run -d --rm --name "$(name "$i")" --network host --shm-size="${PG_SHM_SIZE:-4g}" \
        -e POSTGRES_USER=acta -e POSTGRES_DB=acta -e POSTGRES_HOST_AUTH_METHOD=trust \
        "${POSTGRES_IMAGE:-postgres:16}" \
        postgres -c listen_addresses=127.0.0.1 -p "$port" \
        -c max_connections=6000 \
        -c lock_timeout=60s \
        -c deadlock_timeout=1s \
        -c max_prepared_transactions=1500 \
        -c max_locks_per_transaction=256 \
        -c max_pred_locks_per_transaction=512 \
        -c shared_buffers="${PG_SHARED_BUFFERS:-2GB}" \
        -c effective_cache_size=128GB \
        -c wal_buffers=64MB \
        -c max_wal_size=8GB \
        -c checkpoint_timeout=15min \
        -c checkpoint_completion_target=0.9 \
        -c fsync=off -c synchronous_commit=off -c full_page_writes=off > /dev/null
}

ready() {
    local i="$1" port=$((base_port + $1))
    if [[ $kind == mysql ]]; then
        # the image's init runs a socket-only server first: TCP answers once the real one is up
        docker exec "$(name "$i")" mysql -h127.0.0.1 -P"$port" -uroot -e 'select 1' acta > /dev/null 2>&1
    else
        docker exec "$(name "$i")" pg_isready -h 127.0.0.1 -p "$port" -U acta -d acta > /dev/null 2>&1
    fi
}

case "$cmd" in
    start)
        for ((i = 0; i < count; i++)); do
            if docker container inspect "$(name "$i")" > /dev/null 2>&1; then
                echo "$(name "$i") is already running" >&2
            elif [[ $kind == mysql ]]; then
                start_mysql "$i"
            else
                start_postgres "$i"
            fi
        done
        for ((i = 0; i < count; i++)); do
            for _ in $(seq 1 120); do
                ready "$i" && break
                if ! docker container inspect "$(name "$i")" > /dev/null 2>&1; then
                    echo "$(name "$i") exited; see: docker logs $(name "$i") (gone with --rm: rerun without it)" >&2
                    exit 1
                fi
                sleep 1
            done
            ready "$i" || { echo "$(name "$i") not ready after 120 s" >&2; exit 1; }
        done
        urls
        ;;
    stop)
        docker ps -a --format '{{.Names}}' | grep "^coshim-bench-$kind-" | xargs -r docker rm -fv > /dev/null   # -v: also their data volumes, or every run leaks GBs
        ;;
    urls)
        urls
        ;;
    createdb)
        db="${4:?database name}"
        port=$((base_port + count))
        if [[ $kind == mysql ]]; then
            docker exec "$(name "$count")" mysql -h127.0.0.1 -P"$port" -uroot -e "create database if not exists \`$db\`"
        else
            docker exec "$(name "$count")" psql -h 127.0.0.1 -p "$port" -U acta -d acta -tAc \
                "select 1 from pg_database where datname = '$db'" | grep -q 1 \
                || docker exec "$(name "$count")" psql -h 127.0.0.1 -p "$port" -U acta -d acta -qc "create database \"$db\""
        fi
        ;;
    *)
        usage
        ;;
esac
