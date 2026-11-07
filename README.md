# LSM Storage Engine in C#

A from-scratch log-structured merge-tree (LSM) key-value storage engine in C# on .NET 8 — write-ahead log with CRC-32 integrity, sorted in-memory memtable, immutable SSTables with sparse indexes and bloom filters, and a choice of full-merge (tiered) or leveled compaction. Byte-array keys and values, per-key TTL, range deletes, custom key comparators, an atomic batch write, snapshot scans, and durability guarantees that are stated precisely and tested by simulating crashes.

The library has **zero package dependencies** — the CRC-32, the bloom filter, and every file format are implemented by hand.

## Why this project

A storage engine is where backend engineering stops being forgiving:

- **Durability** — a write you acknowledged must survive the process dying at the worst possible moment, and the boundary between "survives" and "may be lost" must be exact, not vibes
- **Crash recovery** — telling a torn final write (normal, truncate it) apart from corruption in the middle of the log (never acceptable, fail loudly)
- **Data structures** — sorted runs, sparse indexes, bloom filters, k-way merges
- **File formats** — byte-level layouts that a future reader (and a crash) must interpret unambiguously

## Features

**Core operations** — `Put`, `Get`, `Delete`, `Put` with a per-key TTL and `Expire`, `DeleteRange` (one range tombstone for a whole span, as durable as a point delete), atomic `WriteBatch` (a batch is a single WAL record: all of it survives a crash or none of it does), `Flush`, `Compact`

**Reads** — point lookups (memtable → frozen memtable while a flush is in flight → SSTables newest-first, bloom filter consulted before the index, range-tombstone coverage answered from a dedicated ranges section) and a forward `Scan` iterator over a snapshot view, with optional half-open key ranges. Expired values are invisible to reads the moment their deadline passes and never resurrect older copies

**Storage machinery**
- Append-only WAL; every record framed with its length and CRC-32
- Background flush: at a configurable size threshold the memtable is sealed and written to an immutable SSTable by a worker thread while reads and writes continue against the new memtable and WAL generation
- SSTables with sparse indexes and per-table bloom filters (format v2; previous-generation LSMDB001 tables still load and merge)
- Two compaction strategies, selectable at open: **tiered** full-merge at a configurable table-count threshold (newest version wins, tombstones purged), and **leveled** — size-ratio levels, key-range-partitioned above level 0, tombstones dropped only when a merge provably contains every copy they shadow. Both stream end to end, so peak merge memory is proportional to the key count, never to the number of bytes merged; on the suite's reference workload leveled writes ~0.54× the SSTable bytes of tiered (measured, see DESIGN.md)
- Custom key comparators: the ordering is pluggable, its identity is recorded in the manifest, and opening a directory under a different identity is refused loudly instead of silently misreading every sorted structure
- Atomic manifest (temp file + fsync + rename), format v2 — a live-WAL-generation list plus a pending-flush slot — as the single source of truth for live files; format v1 directories upgrade in place; orphans from interrupted operations are cleaned on open
- One open database per directory, enforced by an OS file lock; concurrent readers inside the process

**Durability, honestly** — the fsync policy is a real trade-off, not a footnote:

| Policy | Process crash | OS crash / power loss |
|---|---|---|
| `Always` (default) | nothing acknowledged is lost | nothing acknowledged is lost |
| `Interval` | nothing acknowledged is lost | up to one fsync interval of acknowledged writes may be lost |

The full guarantee statement, the recovery algorithm, and the file formats are in [DESIGN.md](DESIGN.md).

## Usage

```csharp
using System.Text;
using StorageEngine;

using var db = LsmDb.Open(new StorageOptions
{
    DirectoryPath = "app-data",
    FsyncPolicy = FsyncPolicy.Always,   // the default
    MemTableSizeBytes = 4 * 1024 * 1024 // flush threshold
});

db.Put(Encoding.UTF8.GetBytes("user:42"), Encoding.UTF8.GetBytes("eslam"));

var batch = new WriteBatch()
    .Put(Encoding.UTF8.GetBytes("user:43"), Encoding.UTF8.GetBytes("amina"))
    .Delete(Encoding.UTF8.GetBytes("user:41"));
db.Write(batch); // atomic: both operations or neither

var value = db.Get(Encoding.UTF8.GetBytes("user:42"));

foreach (var (key, val) in db.Scan()) // snapshot, ascending key order
    Console.WriteLine($"{Encoding.UTF8.GetString(key)} = {Encoding.UTF8.GetString(val)}");

// Per-key TTL: the deadline is persisted with the value and survives reopen.
db.Put(Encoding.UTF8.GetBytes("session:9"), Encoding.UTF8.GetBytes("token"),
       TimeSpan.FromMinutes(30));
db.Expire(Encoding.UTF8.GetBytes("user:42"), TimeSpan.FromDays(90));

// One durable tombstone for the whole half-open span [start, end):
db.DeleteRange(Encoding.UTF8.GetBytes("draft:"), Encoding.UTF8.GetBytes("draft;"));
```

## Benchmarks

Measured with the included `bench` project (single thread, 100-byte values, random point reads, data read back from flushed + compacted SSTables where stated) on a small virtualized sandbox machine — treat the absolute numbers as indicative of shape, not as hardware truth:

| Scenario | Result |
|---|---|
| Writes, fsync = always (20,000 keys) | ~31,500 writes/sec |
| Writes, fsync = interval 100 ms (100,000 keys) | ~68,600 writes/sec |
| Point reads @ 10,000 keys (50,000 reads) | ~101,000 reads/sec |
| Point reads @ 100,000 keys (50,000 reads) | ~66,600 reads/sec |
| Full scan @ 100,000 keys | ~431,000 entries/sec |
| Streaming compaction: 81 tables, 75.8 MB → one 60.9 MB table | ~0.3–0.4 sec, peak working-set growth 5–9 MB |

The compaction line is the hardening pass's boundedness evidence: the merge streams, so its memory stays in single-digit megabytes while the data it merges is ~76 MB — the test suite asserts growth under 32 MB on the same shape (a merge that materialized its output would need roughly the surviving ~61 MB). The suite also carries a scale check: 300,000 keys through ~450,000 mixed writes, verified byte-for-byte against an independent model after compaction and again after reopen.

Run them yourself:

```bash
dotnet run --project bench/StorageEngine.Bench -c Release
```

## Tests

117 tests, including the ones that matter most — **crash simulation**: write, abandon the instance without any clean shutdown (a test hook drops every handle the way process death does), reopen, and verify every acknowledged write is present and no partial record is visible. Plus torn-tail tests (garbage appended to the WAL, a CRC-damaged final record, and a record cut mid-payload all recover cleanly and are truncated), a mid-log corruption test (one flipped byte with valid records after it → loud `StorageCorruptionException`), tests that rebuild the exact on-disk states a crash leaves mid-flush and mid-compaction (orphaned tables are deleted unread; retired compaction inputs are cleaned on reopen), compaction correctness checked byte-for-byte against a model, write semantics across layers (duplicate keys inside one batch, delete-then-reinsert across flushes, compaction against a live memtable, values far larger than the memtable), scan snapshot and range edges, argument validation, and bloom-filter sanity (no false negatives ever; false-positive rate under a stated ceiling, re-checked at 100,000 keys). The hardening passes added the scale proofs: a **boundedness test** that compacts ~76 MB of tables while a sampler thread watches the process working set (the streaming merge must stay under a 32 MB growth limit — see Benchmarks), and a **300,000-key scale test** (mixed puts, deletes, rewrites, and resurrections across ~18 tables) verified byte-for-byte against an independent model after a full compaction and again after reopen. The completion pass added the **manifest v2 crash-state matrix** — hand-built on-disk states for every window of the background flush (after rotation, mid-write, after the rename, after the commit, WAL deletion pending; a pending table is discarded unread and the generations replay) — and the **background flush evidence**: reads and writes continuing against the new generation mid-flight, crashes abandoned at the rotation and at the rename with exact recovery, `Dispose` rendezvousing with the worker, an injected flush failure suspending writes while reads continue (and `Flush()` retrying the identical job to a heal), and a seeded random-crash loop (12 seeds) plus concurrent writers landing every acknowledged write against a model. The completion wave added: **TTL** proven against a deadline-aware model under a manually advanced fake clock (no wall-clock sleeps) — lazy expiry, expiry shadowing older table values without resurrection, TTL set-then-overwritten, expire-on-missing, deadlines surviving crash/reopen; **custom comparators** — reverse and case-insensitive orderings across flush, compaction, and reopen, plus loud refusal when the manifest's recorded comparer identity doesn't match the open options; **range deletes** against a seeded model, incl. overlapping ranges, range-then-put and put-then-range in one batch, and a crash between the WAL append and the flush; **leveled compaction** — per-level non-overlap verified from the table files, the tombstone retention/drop rule proven both directions with hand-edited manifest states, the manifest-swap crash window, and write amplification asserted against the tiered strategy on one workload through the engine's bytes-written counter; and **format compatibility** — a table built byte-for-byte to the original LSMDB001 spec, read back and merged with new-format tables.

```bash
dotnet test -c Release
```

## Limitations

Single writer process per directory; no transactions beyond batch atomicity; no column families (grouping lives in key prefixes). The default compaction is a full merge, so its write amplification (not its memory) grows with data size — the leveled strategy is the opt-in answer, measured in [DESIGN.md](DESIGN.md). Directory fsyncs after renames are not issued (file fsyncs are): on journaling filesystems — NTFS, APFS, and ext4/XFS/Btrfs in their default modes — created and renamed entries are durable as journaled metadata, while a non-journaling POSIX filesystem leaves a theoretical power-loss window around renames. See the *Guarantees* and *Deviations* sections of [DESIGN.md](DESIGN.md) for the precise per-platform statement.

## License

MIT — see [LICENSE](LICENSE).
