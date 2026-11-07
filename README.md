# LSM Storage Engine in C#

A from-scratch log-structured merge-tree (LSM) key-value storage engine in C# on .NET 8 — write-ahead log with CRC-32 integrity, sorted in-memory memtable, immutable SSTables with sparse indexes and bloom filters, and full-merge compaction. Byte-array keys and values, an atomic batch write, snapshot scans, and durability guarantees that are stated precisely and tested by simulating crashes.

The library has **zero package dependencies** — the CRC-32, the bloom filter, and every file format are implemented by hand.

## Why this project

A storage engine is where backend engineering stops being forgiving:

- **Durability** — a write you acknowledged must survive the process dying at the worst possible moment, and the boundary between "survives" and "may be lost" must be exact, not vibes
- **Crash recovery** — telling a torn final write (normal, truncate it) apart from corruption in the middle of the log (never acceptable, fail loudly)
- **Data structures** — sorted runs, sparse indexes, bloom filters, k-way merges
- **File formats** — byte-level layouts that a future reader (and a crash) must interpret unambiguously

## Features

**Core operations** — `Put`, `Get`, `Delete`, atomic `WriteBatch` (a batch is a single WAL record: all of it survives a crash or none of it does), `Flush`, `Compact`

**Reads** — point lookups (memtable → SSTables newest-first, bloom filter consulted before the index) and a forward `Scan` iterator over a snapshot view, with optional half-open key ranges

**Storage machinery**
- Append-only WAL; every record framed with its length and CRC-32
- Memtable flush to immutable SSTables at a configurable size threshold
- SSTables with sparse indexes and per-table bloom filters
- Full-merge compaction at a configurable table-count threshold: newest version wins, tombstones purged — streamed end to end, so peak merge memory is proportional to the key count, never to the number of bytes merged
- Atomic manifest (temp file + fsync + rename) as the single source of truth for live files; orphans from interrupted operations are cleaned on open
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

61 tests, including the ones that matter most — **crash simulation**: write, abandon the instance without any clean shutdown (a test hook drops every handle the way process death does), reopen, and verify every acknowledged write is present and no partial record is visible. Plus torn-tail tests (garbage appended to the WAL, a CRC-damaged final record, and a record cut mid-payload all recover cleanly and are truncated), a mid-log corruption test (one flipped byte with valid records after it → loud `StorageCorruptionException`), tests that rebuild the exact on-disk states a crash leaves mid-flush and mid-compaction (orphaned tables are deleted unread; retired compaction inputs are cleaned on reopen), compaction correctness checked byte-for-byte against a model, write semantics across layers (duplicate keys inside one batch, delete-then-reinsert across flushes, compaction against a live memtable, values far larger than the memtable), scan snapshot and range edges, argument validation, and bloom-filter sanity (no false negatives ever; false-positive rate under a stated ceiling). The hardening pass added the two scale proofs: a **boundedness test** that compacts ~76 MB of tables while a sampler thread watches the process working set (the streaming merge must stay under a 32 MB growth limit — see Benchmarks), and a **300,000-key scale test** (mixed puts, deletes, rewrites, and resurrections across ~18 tables) verified byte-for-byte against an independent model after a full compaction and again after reopen.

```bash
dotnet test -c Release
```

## Limitations

Single writer process per directory; no transactions beyond batch atomicity; no TTL or custom comparators; compaction is a full merge, so its write amplification (not its memory) grows with data size. Directory fsyncs after renames are not issued (file fsyncs are): on journaling filesystems — NTFS, APFS, and ext4/XFS/Btrfs in their default modes — created and renamed entries are durable as journaled metadata, while a non-journaling POSIX filesystem leaves a theoretical power-loss window around renames. See the *Guarantees* and *Deviations* sections of [DESIGN.md](DESIGN.md) for the precise per-platform statement.

## License

MIT — see [LICENSE](LICENSE).
