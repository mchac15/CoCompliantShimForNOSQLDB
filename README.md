# CoCompliantShimForNOSQLDB

A speculative, Commitment-Ordering-compliant subtransaction shim for NoSQL key-value stores
(protocol: [`pseudo.txt`](pseudo.txt), report: [`main.tex`](main.tex)). It is exposed to
[Apache Seata](https://seata.apache.org/) as an **XA data source**, the same way MySQL and
PostgreSQL are in the Sonata fork. This lets us compare it with Sonata's MySQL/PG shim, and run
global transactions that mix both kinds of shim.

## Modules

| Module | Depends on | Contents |
| --- | --- | --- |
| `store-api` | nothing | `KvStore<K, V>`: the whole database behind `get` / `store`, plus `InMemoryKvStore` |
| `shim-core` | `store-api` | `CoShim`: the requests a shim receives from client connections (start / get / put / end / prepare / commit / abort). It never runs client code; `SpeculativeCoShim` is a **TODO stub** to be implemented from pseudo.txt; `NoCcShim` is a pass-through shim (no concurrency control) used as the benchmark baseline and in tests |
| `shim-net` | `shim-core` | **Each shim is a separate node**: `CoShimServer` serves one `CoShim` over TCP to the participants only (shared-token handshake, optional client allowlist); `RemoteCoShim` is the client the RMs use; `CoShimNode` runs a node |
| `coshim-jdbc` | `shim-core` | The shim as a JDBC "driver": `CoShimDataSource` (`DataSource` + `XADataSource`, URL `jdbc:coshim://<name>`), `CoShimXAResource`, `KvSession` (get/put instead of SQL). No Seata dependency |
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

```
  application JVM (Seata TM + RM)                          shim node (one per store)
  DataSourceProxyCoShim ─ ConnectionProxyXA ─ CoShimXAResource
                                     └─ RemoteCoShim ── TCP, token ──▶ CoShimServer ─ SpeculativeCoShim ─ KvStore
  Seata TC ◀──── branch register / phase 2 ────▶ RM (never talks to the node itself)
```

```sh
COSHIM_TOKEN=secret java -cp ... ch.epfl.coshim.net.CoShimNode 7000 nocc [allowed RM hosts...]
```

```java
RemoteCoShim<String, String> shim = new RemoteCoShim<>(
        new InetSocketAddress("localhost", 7000), System.getenv("COSHIM_TOKEN"), Codec.UTF8, Codec.UTF8).verify();
DataSource orders = new DataSourceProxyCoShim(
        new CoShimDataSource<>("localhost:7000/orders-kv", shim));   // like new DataSourceProxyXA(mysqlPool)

// inside a global transaction (@GlobalTransactional, or GlobalTransactionContext + begin/commit):
try (Connection c = orders.getConnection()) {
    c.setAutoCommit(false);                                 // branchRegister + xa start
    KvSession<String, String> session = KvSession.from(c);
    session.put("order:1", session.get("stock:7"));
    c.commit();
}                                                           // xa end + xa prepare (shim vote)
// phase 2 (xa commit / xa rollback) is driven by the TC, as for MySQL/PG
```

`CoShimXAResource` also guards one race that pseudo.txt's coordinator assumption rules out but
Seata does not: a TC rollback arriving before `xa start`, or while phase 1 runs. See
`BranchStates`; it keeps short-lived tombstones at the XA layer, not in the shim protocol. It is a
first mitigation (TODO).

Outside a global transaction a connection runs **local transactions** with standard JDBC
semantics (autocommit per request, or `commit()` / `rollback()`), like a MySQL/PG connection.
Inside a global transaction, `DataSourceProxyCoShim` refuses get/put until the connection is
enlisted with `setAutoCommit(false)`. Otherwise the requests would silently run outside the
global transaction, because `KvSession` bypasses Seata's statement templates, which enlist SQL
automatically.

### Deployment notes

- **Each shim is a separate node** (`shim-net`). It accepts many concurrent connections, one
  thread each, but only from the participants: a connection must present the node's shared
  token, and can be restricted to an allowlist of RM hosts. The Seata TC never connects to it;
  as for MySQL/PG, the TC talks to the RMs and they talk to the node.
- **Resource id = the node's address** (`jdbc:coshim://host:port/store`), the same for every
  application instance, like a MySQL URL. The TC routes phase 2 by resource id and may pick any
  RM registered with it. That's fine, because every RM reaches the same node, which holds the
  transaction state. Only an in-process shim (tests) needs a per-instance name.
- **Early-rollback guard is still per RM** (`BranchStates`). With several application instances,
  a TC rollback racing phase 1 may reach another instance. Moving the guard onto the node is the
  TODO on `BranchStates`.
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
mvn verify                    # JDK 17+
```

The `seata-xa` tests drive Seata's real `DataSourceProxyXA` / `ConnectionProxyXA` /
`ResourceManagerXA`, with only the TC round trips stubbed (as in Seata's own `XAModeTest2`). An
application also needs Seata's `registry.conf` / `file.conf` (see `seata-xa/src/test/resources`)
and Druid on the classpath, like any Seata XA application.

## Next steps

- Implement `SpeculativeCoShim` from pseudo.txt.
- Add a real key-value backend behind `KvStore`.
- Run end to end against `seata-mock-server` or a real TC, with a mixed global transaction (a
  MySQL/Sonata branch and a coshim branch). Then build a benchmark comparing stock XA, Sonata,
  `NoCcShim` (plumbing only) and `SpeculativeCoShim`.
