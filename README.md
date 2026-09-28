# CoCompliantShimForNOSQLDB

A speculative, Commitment-Ordering-compliant subtransaction shim for NoSQL key-value stores
(protocol: [`pseudo.txt`](pseudo.txt), report: [`main.tex`](main.tex)), plugged into
[Apache Seata](https://seata.apache.org/) as an XA participant. That lets us compare it with
Sonata's MySQL/PostgreSQL shim, and run global transactions that mix both kinds of shim.

## Modules

| Module | Depends on | Contents |
| --- | --- | --- |
| `store-api` | nothing | `KvStore<K, V>`: the whole database behind `get` / `store`, plus `InMemoryKvStore` |
| `shim-core` | `store-api` | `CoShim` (execute / prepare / commit / abort), `TxnCode`, `TxnContext`; `SpeculativeCoShim` is a **TODO stub** to be implemented from `pseudo.txt` |
| `seata-adapter` | `shim-core`, Seata jars | Runs a `CoShim` as a Seata XA branch |

`shim-core` and `store-api` know nothing about Seata. The adapter only uses Seata's public API
and its SPI. No Seata source is copied or modified.

## How the shim becomes an XA branch

Seata's XA mode is JDBC-bound: `ResourceManagerXA` only accepts `AbstractDataSourceProxyXA`
resources. So the shim is not wrapped as a `javax.sql.DataSource`. It is added as a new kind of
XA resource instead:

- **`ShimResource`**: a Seata `Resource` with `BranchType.XA` and id `coshim://<name>`.
- **`RoutingResourceManagerXA`**: extends `ResourceManagerXA` and replaces it through
  `META-INF/services` (`@LoadLevel(order = 100)`; Seata keeps one RM per branch type, the last
  loaded wins).
  - Shim resources get phase 2 mapped onto the shim: branchCommit calls `commit`, branchRollback
    calls `abort`.
  - Every other resource (stock XA and Sonata data sources) goes to the stock code unchanged.
- **`ShimBranchExecutor.run(resource, code)`**: phase 1, mirroring `ConnectionProxyXA`:
  1. register an XA branch with the TC, with no lock keys and the shim txn id as applicationData;
  2. `execute`;
  3. `prepare` (blocking vote). On FAILED or NO it reports `PhaseOne_Failed` and throws.

Two properties follow:

- **Same TC path as Sonata.** The TC sees ordinary XA branches (`XACore`) and takes no global
  locks for them, so the shim's own concurrency control is the only one being measured, and one
  global transaction can contain both shim and Sonata branches.
- **The phases line up with pseudo.txt.** Seata XA prepares locally in phase 1, before the TM's
  global commit, and the global-transaction timeout plays the coordinator's vote timeout
  (assumption 4). A TC rollback that arrives before the shim has registered the txn is caught
  by `ShimResource`'s phase-1 tracking, so no prepared txn is left holding locks.

```java
KvStore<String, String> store = new InMemoryKvStore<>();
ShimResource<String, String> orders =
        new ShimResource<>("orders-kv", new SpeculativeCoShim<>(store, Duration.ofMillis(200)));
CoShimSeata.register(orders);                       // after RMClient.init(appId, txServiceGroup)

GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();
tx.begin(60_000, "place-order");
try {
    new ShimBranchExecutor().run(orders, ctx -> ctx.put("order:1", ctx.get("stock:7")));
    // ... more shim branches, or Sonata/MySQL XA work on a DataSourceProxyXA ...
    tx.commit();
} catch (Exception e) {
    tx.rollback();
}
```

## Building

The adapter compiles against the Sonata fork of Seata (`2.6.0-SNAPSHOT`, not on Maven Central).
Install it into `~/.m2` once, and again whenever the fork changes:

```sh
cd ../incubator-seata            # branch integrate-sonata-in-xa-mode
./mvnw install -DskipTests -pl rm-datasource,tm,mock-server -am
cd -
mvn verify                       # JDK 17+
```

The adapter tests do not need a running TC: branch registration and reports are stubbed, as in
Seata's own `XAModeTest2`. An application still needs Seata's `registry.conf` / `file.conf`
(see `seata-adapter/src/test/resources`).

## Next steps

- Implement `SpeculativeCoShim` from `pseudo.txt`.
- Add a real key-value backend behind `KvStore`.
- Add an end-to-end run against `seata-mock-server` or a real TC, then a benchmark harness
  comparing stock XA, Sonata, the shim, and mixed deployments.
