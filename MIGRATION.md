# kotobase-peer capability migration

`kotobase-peer` is feature frozen. It remains buildable at existing Git SHAs
while capability repositories are published and consumers advance their pins.
Keeping the compatibility source temporarily avoids making a local west
checkout a hidden runtime requirement.

## Ownership

| Existing namespace or area | Destination |
|---|---|
| `kotobase-peer.core` | `kotobase-engine-prolly` behind `kotobase-engine-contract`; public calls through `kotobase` |
| `kotobase-peer.merkle-lsm` | `merkle-lsm` and `kotobase-engine-lsm` |
| `kotobase-peer.cache` | `block-cache` and host composition |
| `kotobase-peer.materialized-view` | `kotobase.projection` |
| `kotobase-peer.atomic-publication` | `kotobase.projection.publication` |
| `kotobase-peer.statistics` | `kotobase.projection.statistics` |
| `kotobase-peer.datalog-materialization` | `kotobase.projection.datalog` with injected query/transaction functions |
| `kotobase-peer.availability` | `kotobase.federation` |
| `kotobase-peer.retention` | `kotobase.maintenance.retention` |
| `kotobase-peer.database-restore` | `kotobase.maintenance.restore` |
| `kotobase-peer.resumable-execution` | `kotobase.maintenance.resumable` |
| `kotobase-peer.policy` | `kotobase-server` / common authorization semantics |
| `kotobase-peer.transactor` | `kotobase-server` |
| S3/R2 block, ref, and object operations | `kotobase-storage-s3` |
| Worker scheduling, backup, restore, and GC execution | `net-kotobase` consuming portable maintenance state |

## Compatibility sequence

1. Publish each destination and run its extracted tests independently.
2. Add destination dependencies to this repository at reviewed Git SHAs.
3. Replace extracted namespaces here with thin delegating facades; do not
   change their signatures in the compatibility cycle.
4. Advance `kotobase-server`, `net-kotobase`, browser workers, and SDK pins.
5. Verify there are no runtime consumers of the old coordinates.
6. Rename the residual repository to `kotobase-peer-compat` for one release
   cycle, then archive it.

The compatibility repository must not depend on unpublished sibling
`local/root` paths. Such a change would appear green in the west checkout but
make the released Git coordinate unusable elsewhere.
