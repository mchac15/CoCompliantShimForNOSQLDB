# CoCompliantShimForNOSQLDB

A speculative, Commitment-Ordering-compliant subtransaction shim for NoSQL key-value stores
(protocol: [`pseudo.txt`](pseudo.txt), report: [`main.tex`](main.tex)). It is exposed to
[Apache Seata](https://seata.apache.org/) as an **XA data source**, the same way MySQL and
PostgreSQL are in the Sonata fork. This lets us compare it with Sonata's MySQL/PG shim, and run
global transactions that mix both kinds of shim.

## Modules

| Module | Depends on | Contents |
| --- | --- | --- |
| `store-api` | nothing | `KvStore<K, V>`: the whole database behind `get` / `store`, plus `InMemoryKvStore`; `TableKey` (table, key): a shim manages several tables of its database |
| `store-sql` | `store-api` | `SqlKvStore`: a MySQL or PostgreSQL database as a `KvStore`. `get` is a `select`, `store` / `storeAll` an upsert (batched, one statement per table); each `TableKey` table is a SQL table `(k, v)`, created on first use, plain keys go to `kv`. Autocommit at READ COMMITTED: the shim does the concurrency control. Transient errors (deadlock, lost connection, ...) are retried. Tests against real databases run when `COSHIM_TEST_MYSQL_URL` / `COSHIM_TEST_PG_URL` are set |
| `shim-core` | `store-api` | `CoShim`: the requests a shim receives from client connections (start / get / put / end / prepare / commit / abort). It never runs client code; `SpeculativeCoShim` implements pseudo.txt, and with `speculative = false` is the non-speculative strict-2PL baseline (lock() waits for predecessors to *commit*, no cascades); `ShimStats` counts its commits and aborts by cause; `NoCcShim` is a pass-through shim (no concurrency control) used in tests |
| `shim-net` | `shim-core` | **Shim nodes**: `CoShimServer` hosts one or more databases, one `CoShim` each, and serves them over TCP to the participants only (shared-token handshake, optional client allowlist); `RemoteCoShim` is the client the RMs use, bound to one database; `CoShimNode` runs a node |
| `coshim-jdbc` | `shim-core` | The shim as a JDBC "driver": `CoShimDataSource` (`DataSource` + `XADataSource`, URL `jdbc:coshim://host:port/database`), `CoShimXAResource`, `KvSession` (`get(table, key)` / `put(table, key, value)` instead of SQL). No Seata dependency |
| `seata-xa` | `coshim-jdbc`, patched Seata | `DataSourceProxyCoShim`: the counterpart of `new DataSourceProxyXA(mysqlPool)` |

## How MySQL/PG are integrated in the Sonata fork (and what we copy)

The fork's commits on top of Seata `2.x` do **not** add MySQL/PG as data sources; upstream Seata
already has them:

1. The app wraps its pool: `new DataSourceProxyXA(dataSource)`. The JDBC URL becomes the
   resource id and the dbType.
2. `XAUtils` asks the MySQL/PG driver for its native `XAConnection` / `XAResource`.
3. `ConnectionProxyXA` drives that `XAResource`:
   - `setAutoCommit(false)`: `branchRegister`, then `xa start`;
   - the app runs SQL;
   - `close()`: `xa end`, then `xa prepare` (phase 1).
4. The TC's phase 2 goes through `ResourceManagerXA`: `xa commit`, or `xa rollback`.

Sonata (commit `559ee78eb`) is a hook inside this path. `ConnectionProxyXA.close()` does one
dummy write right before `xa prepare`:

- **MySQL (S2PL):** upsert a random key into `sonata_dummy`.
- **PG (SSI):** write a key that a prepared helper transaction has read.

The database's own concurrency control then yields CO. It is enabled with
`sonata.enableGlobalSerializability` and switched on per dbType. The other fork commits make
rollback robust against MySQL/PG XA quirks.

**Our shim plugs into the same path.** `XAResource` is the standard 2PC-participant interface,
and it maps onto pseudo.txt:

| XA call (from Seata's `ConnectionProxyXA` / `ResourceManagerXA`) | shim |
| --- | --- |
| `start(xid)` | `start`: txn registered, `started` |
| `KvSession.get/put` on the connection | `get` / `put` |
| `end(xid, TMSUCCESS)` | `end`: `started → executed` (failure → `XA_RBROLLBACK`) |
| `prepare(xid)` | `prepare`: YES → `XA_OK`, NO → `XA_RBROLLBACK` |
| `commit(xid)` | `commit` |
| `rollback(xid)` | `abort` |

So Seata runs the stock XA code, the same TC path (`XACore`), and no Seata global locks for both
Sonata and shim branches. Heavy vs light is purely what runs behind the `XAResource`, and one
global transaction can contain both.

### One shim per data source

A **data source is a node plus a database name**, and each has exactly one shim (its own lock
chains and transactions).

**Tables are optional.** A protocol key is a plain key (`kv.get(key)`) or, for stores that have
tables, a `(table, key)` pair (`kv.get(table, key)`); one shim covers all the tables of its
database. A store with no table abstraction goes behind `FlatKvStore`, which takes plain keys as
they are and rejects table keys.

A node can host a single database (standalone, or colocated with its store)
or several; that is a deployment choice, like one MySQL server hosting several logical databases.
MySQL/PG never go through these nodes: they remain their own servers, reached through their JDBC
drivers with Sonata's hook in the RM.

```
  application JVM (Seata TM + RM)                           shim node  host:7000
  DataSourceProxyCoShim("host:7000/shop") ─ ConnectionProxyXA ─ CoShimXAResource
                   └─ RemoteCoShim(database "shop") ── TCP, token ──▶ CoShimServer ─┬─ shim "shop" ─ KvStore (tables orders, stock, …)
                                                                                    └─ shim "billing" ─ KvStore
  Seata TC ◀──── branch register / phase 2 ────▶ RM (never talks to the node itself)
```

```sh
COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode --port 7000 --shim speculative \
    --databases shop,billing --lock-timeout-ms 1000 [--stats-interval-s 10] [--allow RM-host ...]
# --shim speculative | nonspeculative | nocc; commit/abort counters are logged every interval and at shutdown
# --store memory (default) | jdbc:mysql://host:3306/{database} | jdbc:postgresql://host:5432/{database}?user=u
#   each database of the node on its own SQL database ({database} = its name; optional with one database)
```

```java
RemoteCoShim<TableKey<String>, String> shim = new RemoteCoShim<>(
        new InetSocketAddress("localhost", 7000), "shop", System.getenv("COSHIM_TOKEN"),
        new TableKeyCodec<>(Codec.UTF8), Codec.UTF8).verify();
DataSource shop = new DataSourceProxyCoShim(
        new CoShimDataSource<>("localhost:7000/shop", shim));   // like new DataSourceProxyXA(mysqlPool)

// inside a global transaction (@GlobalTransactional, or GlobalTransactionContext + begin/commit):
try (Connection c = shop.getConnection()) {
    c.setAutoCommit(false);                                 // branchRegister + xa start
    KvSession<String, String> kv = KvSession.from(c);
    kv.put("orders", "order:1", kv.get("stock", "item:7"));  // two tables, one branch
    c.commit();
}                                                           // xa end + xa prepare (shim vote)
// phase 2 (xa commit / xa rollback) is driven by the TC, as for MySQL/PG
```

**Tombstones.** pseudo.txt originally assumed the coordinator sends nothing for a txn before its
start. Seata does not guarantee that: a TC rollback (e.g. a global timeout) can arrive before
`xa start`. The shim handles it itself. An `abort` for an id it does not know leaves a tombstone,
and `start` refuses a tombstoned id, atomically with the registration. Tombstones expire after a
TTL (at least the global transaction timeout). Because this lives on the shim, it also works
when the TC routes the rollback through a different application instance. `CoShimXAResource`
keeps no transaction state of its own.

Outside a global transaction a connection runs **local transactions** with standard JDBC
semantics (autocommit per request, or `commit()` / `rollback()`), like a MySQL/PG connection.
Inside a global transaction, `DataSourceProxyCoShim` refuses get/put until the connection is
enlisted with `setAutoCommit(false)`. Otherwise the requests would silently run outside the
global transaction, because `KvSession` bypasses Seata's statement templates, which enlist SQL
automatically.

### Deployment notes

- **Shim nodes** (`shim-net`). A node accepts many concurrent connections, one thread each, but
  only from the participants: a connection must present the node's shared token and name a
  database the node hosts, and can be restricted to an allowlist of RM hosts. The Seata TC never
  connects to it; as for MySQL/PG, the TC talks to the RMs and they talk to the node.
- **Resource id = node address + database** (`jdbc:coshim://host:port/database`), the same for
  every application instance, like a MySQL URL. The TC routes phase 2 by resource id and may pick
  any RM registered with it. That's fine, because every RM reaches the same shim, which holds the
  transaction state. Only an in-process shim (tests) needs a per-instance name.
- **Colocate the node with its store.** The shim reaches the store only through `KvStore`: reads
  that miss the write buffers, and the writes at commit. Put the node on the store's machine, or
  embed the store in the node's process when it is embeddable. The path is then application →
  node, one hop, like application → MySQL. A store on another machine adds a network round trip
  to those calls.
- **The shim must be the only writer of its store.** A key-value store has no concurrency control
  of its own, so a process writing to it directly bypasses the shim's locks: commits overwrite
  its writes blindly (lost updates) and reads are no longer protected. Let the store listen only
  to its node (localhost, or embedded). Applications that do not use Seata still go through the
  shim, with local transactions on the coshim data source. Commits reach the store as one batch
  (`KvStore.storeAll`).
- **Tombstone TTL is not definitive.** If a start arrives after its tombstone expired (a phase 1
  stalled longer than the TTL), the branch is accepted although the global transaction was
  rolled back.
- **The wire is not encrypted yet.** The token authenticates, but for untrusted networks, run the
  node behind TLS (or add it to `CoShimServer`).
- **Spring (`seata.data-source-proxy-mode=XA`).** The auto-proxy wraps every plain `DataSource`
  bean in a stock `DataSourceProxyXA`, which cannot create coshim XA connections ("xa not support
  dbType: coshim"). So do not expose a bare `CoShimDataSource` bean. Instead, expose a
  `DataSourceProxyCoShim` bean (declared return type `DataSource`; Seata logs a warning and
  routes calls to it). Alternatively, add `CoShimDataSource` to `seata.excludes-for-auto-proxying`
  and wrap it yourself.
- **Never `XA_RDONLY`.** The TC skips phase 2 for read-only branches, but a shim branch keeps its
  place in the lock chains until commit, so prepare always returns `XA_OK`.

## Seata patch

Seata only accepts data sources whose JDBC driver it knows. [`seata-patches/`](seata-patches)
holds one small, generic patch on top of the Sonata fork:

- `JdbcUtils`: for an unknown URL, fall back to the JDBC subprotocol as dbType (`coshim`) and to
  no `Driver`, instead of failing.
- `DataSourceProxyXA`: a `protected createXAConnection(physicalConn)` hook, defaulting to
  `XAUtils`, which `DataSourceProxyCoShim` overrides.

The Sonata code and the MySQL/PG behaviour are unchanged, and the fork's XA / `JdbcUtils` tests
still pass. The same patch applies to both the Sonata branch and upstream Seata `2.x`, so it could
also be proposed upstream.

## Building

```sh
scripts/install-seata.sh      # clones ../incubator-seata (Sonata fork), applies seata-patches/, installs 2.6.0-SNAPSHOT into ~/.m2
                              # --with-acta: also the shaded seata-all + starter used by ../acta-server
mvn verify                    # JDK 17+
```

The `seata-xa` tests drive Seata's real `DataSourceProxyXA` / `ConnectionProxyXA` /
`ResourceManagerXA`, with only the TC round trips stubbed (as in Seata's own `XAModeTest2`). An
application also needs Seata's `registry.conf` / `file.conf` (see `seata-xa/src/test/resources`)
and Druid on the classpath, like any Seata XA application.

## Benchmark: speculative vs non-speculative

`bench` runs Acta's Micro workload through the real path: a Seata TC decides every global
transaction, and the shims run in shim nodes (`CoShimNode`) reached over TCP. The benchmark JVM is
the application (Seata TM + RM, one `DataSourceProxyCoShim` per shim database), like one of Acta's
services:

```
 MicroBench (TM + RM) ── begin / commit / rollback ──▶ Seata TC :8091 ── phase 2 ──▶ RM
   └ DataSourceProxyCoShim("127.0.0.1:7000/s0") ─ RemoteCoShim ──TCP──▶ CoShimNode :7000 (--shim ...)
   └ DataSourceProxyCoShim("127.0.0.1:7001/s1") ─ RemoteCoShim ──TCP──▶ CoShimNode :7001
```

Per attempt: TM begin (with `--txn-timeout-ms`, enforced by the TC); per branch, an XA connection to
its shim (`setAutoCommit(false)` registers and starts the branch, the gets/puts go to the shim,
`commit()` ends and prepares it); then TM commit, or TM rollback after any failure (retried with
the same plan, as Acta does). On timeout the TC itself rolls the branches back on the shims. The
variant (speculative or not) and the lock timeout are options of the nodes, not of the benchmark.
With `--rmw true` (default) every write is `get + 1` and the run ends with an audit, read back
through the shims (sum of all values = committed txns × branches × writes).

**Start a TC** once (stock Apache Seata 2.6.0; the download steps are in `../acta-server`'s README,
which keeps it in `runtime/seata`), with a small heap and an existing log directory:

```sh
cd ../acta-server/runtime/seata && mkdir -p logs
JVM_XMX=1g JVM_XMS=1g LOG_HOME="$PWD/logs" \
  apache-seata-2.6.0-incubating-bin/seata-server/bin/seata-server.sh start -h 127.0.0.1 -p 8091 -m file
```

**Run** (starts and stops the shim nodes itself, one node per shim, for each variant):

```sh
scripts/bench-compare.sh                                   # the simple test, both variants, 2 shims
scripts/bench-compare.sh --reps 3 --skew 0.99 --threads 100
LOCK_TIMEOUT_MS=100 SHIMS=4 scripts/bench-compare.sh --parallel-branches true --txn-timeout-ms 500
```

Or by hand, e.g. against nodes on other machines:

```sh
COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode --port 7000 --shim speculative --databases s0
COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.bench.MicroBench --variant speculative \
    --shims host1:7000/s0,host2:7000/s1 --tc tc-host:8091
```

`--parallel-branches true` runs the branches of each global transaction at the same time instead of
in order. Transactions then take the shims in different orders, so two of them can wait on each
other's commit in `prepare` on different shims; the shim has no timeout there, only the TC's
`--txn-timeout-ms` breaks such a cycle (column `timed_out`).

Options: `--variant --shims --tc --threads --table-size --branches --reads --writes --skew --rmw
--warmup-s --measure-s --txn-timeout-ms --parallel-branches`; script environment: `SHIMS`,
`NODE_PORT`, `LOCK_TIMEOUT_MS`, `VARIANTS`, `CSV`. Every run is appended to
`bench-results/<timestamp>.csv`, with the nodes' logs (final commit/abort counters per cause) next
to it; the script prints a comparison table and exits 1 if a run warned or failed its audit.
`MicroBenchTest` runs both variants and both branch modes briefly through a TC and an in-JVM shim
node; those runs are skipped when no TC is reachable (`-Dbench.tc=host:port`, default
127.0.0.1:8091).

## Next steps

- Add a real key-value backend behind `KvStore`.
- Compare with Sonata on MySQL/PG through Acta (a `COSHIM` mode whose branches reach the shim nodes
  over TCP), with a mixed global transaction (a MySQL/Sonata branch and a coshim branch).
