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
- Full-merge compaction at a configurable table-count threshold: newest version wins, tombstones purged
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

Run them yourself:

```bash
dotnet run --project bench/StorageEngine.Bench -c Release
```

## Tests

32 tests, including the ones that matter most — **crash simulation**: write, abandon the instance without any clean shutdown (a test hook drops every handle the way process death does), reopen, and verify every acknowledged write is present and no partial record is visible. Plus a torn-tail test (garbage appended to the WAL recovers cleanly and is truncated), a mid-log corruption test (one flipped byte with valid records after it → loud `StorageCorruptionException`), compaction correctness checked byte-for-byte against a model, and bloom-filter sanity (no false negatives ever; false-positive rate under a stated ceiling).

```bash
dotnet test -c Release
```

## Limitations

Single writer process per directory; no transactions beyond batch atomicity; no TTL or custom comparators; compaction buffers its merged output in memory. Directory fsyncs after renames are not issued (file fsyncs are) — see the *Guarantees* and *Deviations* sections of [DESIGN.md](DESIGN.md) for exactly what that means.

## License

MIT — see [LICENSE](LICENSE).
