# Design

An LSM-tree (log-structured merge-tree) key-value storage engine in C#/.NET 8,
with a hand-rolled CRC-32 and zero package dependencies. This document describes
the architecture, the exact on-disk formats, the crash-recovery algorithm, and —
precisely — what is and is not guaranteed.

## Architecture

```
                Put / Delete / Write(batch)
                          │
                          ▼
              ┌───────────────────────┐        fsync policy
              │  WAL  wal-<gen>.log   │───────► durable before the
              │  append-only, CRC-32  │         write is acknowledged
              └───────────┬───────────┘
                          ▼
              ┌───────────────────────┐
              │       Memtable        │  sorted, in memory
              │  (red-black tree)     │
              └───────────┬───────────┘
              size ≥ threshold (flush)
                          ▼
              ┌───────────────────────┐
              │  SSTable  sst-*.lsm   │  immutable, sorted, sparse index
              │  + bloom filter       │  + bloom filter
              └───────────┬───────────┘
              table count ≥ threshold
                          ▼
              ┌───────────────────────┐
              │      Compaction       │  full merge: newest wins,
              │                       │  tombstones purged
              └───────────────────────┘

  MANIFEST — the atomic source of truth for which files are live.
  LOCK     — an OS file lock: one open database per directory, period.
```

**Write path.** Encode the operation (a batch becomes one record) → append to
the WAL and flush per policy → apply to the memtable → flush the memtable if it
crossed its size threshold.

**Read path.** Memtable → SSTables newest-first. An SSTable read checks the
key range, then the bloom filter, then binary-searches the sparse index and
scans forward at most `IndexSampleInterval` entries.

**Memtable choice.** A `SortedDictionary` (red-black tree) with a byte-wise
comparer, rather than a skip list. The engine has a single writer and guards
the memtable with the database lock, so the skip list's one advantage —
lock-free concurrent mutation — buys nothing here. The tree gives deterministic
O(log n) operations, in-order enumeration for flushes and scans for free, and
no probabilistic edge cases. Entry size accounting is approximate: key + value
bytes + a fixed 48-byte per-entry overhead estimate.

## File formats

All integers are little-endian. Keys and values are raw byte arrays; ordering
is unsigned lexicographic byte order.

### WAL — `wal-<generation>.log`

A sequence of record frames:

```
[payloadLength int32][crc32 uint32 of payload][payload]
```

Payload layouts:

```
Put:    [0x01][keyLen int32][key][valueLen int32][value]
Delete: [0x02][keyLen int32][key]
Batch:  [0x03][opCount int32] then per op:
          [opType byte 0x01|0x02][keyLen int32][key]
          (+ [valueLen int32][value] for Put ops)
```

A batch is a *single* record, so the frame CRC makes it atomic: recovery
applies the whole batch or none of it.

### SSTable — `sst-<id>.lsm`

```
Data section — entries in ascending key order:
  [keyLen int32][key][flags byte: 0 = value, 1 = tombstone]
  [valueLen int32][value]                      (omitted for tombstones)
Index section — sparse index:
  [keyLen int32][key][dataOffset int64]        one per sampled entry
                                               (entry 0 always; the last entry always)
Bloom section:
  [hashCount int32][bit bytes]                 ~10 bits/key by default
Footer — fixed 32 bytes at end of file:
  [indexOffset int64][bloomOffset int64][bloomLength int32]
  [entryCount int32][magic 8 bytes "LSMDB001"]
```

The bloom filter derives its k probe positions by double hashing over CRC-32
and FNV-1a of the key. It can only err towards "maybe present" — never towards
"absent" for a key that was written.

### MANIFEST

A small text file (`next-sst-id`, `wal-gen`, one `sst <id>` line per live
table), replaced atomically: write `MANIFEST.tmp`, fsync, rename over the old
file. A crash therefore leaves the old or the new manifest, never a torn one.
Files present on disk but not named by the manifest are orphans of interrupted
operations and are deleted on open.

## Flush protocol

Flushing is synchronous, under the write lock (see *Deviations* below). The
order of steps is chosen so every crash window recovers correctly:

1. Write the memtable's entries to `sst-<id>.tmp`, fsync, rename to
   `sst-<id>.lsm`. (Not yet in the manifest: a crash here leaves an orphan
   file; the WAL still holds every record.)
2. Create the *next* WAL generation file.
3. Update the manifest: add the table, advance the WAL generation, fsync via
   the atomic rename. (A crash before this completes is step 1's case; after
   it, the table is live and the old WAL is an orphan.)
4. Switch the live WAL to the new generation; delete the old WAL file.

At no point can a record exist only in a file the manifest disowns.

## Recovery

On open, after loading the manifest and the tables, the live WAL generation is
replayed record by record into a fresh memtable, using each frame's CRC-32.
Two kinds of damage are possible at the tail, and they are treated oppositely:

* **Torn tail** — a crash mid-append leaves a partial final record, or a final
  record whose CRC fails, with nothing valid after it. Replay stops at the
  last valid record and the file is truncated there. This is normal operation,
  not an error.
* **Mid-log corruption** — a record fails (bad CRC or implausible header) *and
  valid records exist after it*. Sequential appends cannot produce that, so it
  means fully-written data was damaged afterwards. Opening throws
  `StorageCorruptionException`. The engine never silently skips damaged data.

The two are distinguished by a resync scan: from just after the first bad
record, look for any offset that begins a chain of CRC-valid records ending
exactly at end-of-file. If such a chain exists, the damage is mid-log. (The
scan is O(file²) in the worst case — acceptable for a WAL, which is bounded by
the memtable threshold plus one batch.)

## Compaction

Trigger: after a flush, when the live table count reaches
`CompactionTableThreshold` (also callable directly via `Compact()`).

Policy: size-tiered in spirit — tables accumulate as a tier of flush outputs,
and the merge is a **full merge of every live table** into one, rather than a
leveled design's incremental merges. That trades write amplification (every
compaction rewrites all data) for a much simpler correctness story, which is
the right trade at this scale.

The merge **streams end to end**: a k-way merge holds one entry per input
table, and each surviving entry is written straight into the output table
through a buffered writer before the next is read. Nothing materializes the
merged data in memory. Peak merge memory is one entry per input table, plus
the output writer's sampled index (one key per `IndexSampleInterval` entries)
and its bloom filter — proportional to the *key count*, never to the number of
bytes the tables hold. (The one place a count is needed before the data is
seen is the bloom filter's size; it is sized from the inputs' combined entry
count, a strict upper bound on the merged count, since duplicates and
tombstones only shrink it. A merged table may therefore carry a somewhat
larger filter than an exact count would give — never a smaller one.)

During the merge:

* for duplicate keys, the newest table's version wins;
* overwritten entries are dropped;
* **tombstones are purged.** This is safe *because* the merge spans every
  table: there is no older copy of any key beneath the merge for a tombstone
  to shadow. (The memtable is newer than every table and is not part of the
  merge.) In a leveled design, tombstones may only be dropped at the last
  level; here every merge is a last-level merge.

Evidence for the streaming bound, measured in the hardening run (shared
sandbox VM, Release build, warm page cache — indicative, like the README
benchmarks): 81 input tables totalling 75.8 MB merged into a single 60.9 MB
table in ~0.3–0.4 s, with peak process working-set growth of 5–9 MB sampled
every 2 ms across the merge. The suite's boundedness test replays that shape
and asserts growth under 32 MB — under half the surviving data size, which
is what a materializing merge would have to hold. Correctness at scale is
evidenced separately: 300,000 keys through ~450,000 mixed writes (fill,
delete-every-7th, rewrite-every-3rd, resurrect-half-the-deleted) across ~18
tables, verified byte-for-byte against an independent in-test model after
compaction, and again after a close/reopen cycle.

Crash safety: the merged table is written to a temp file, fsynced, renamed,
and only then swapped into the manifest. A crash before the swap leaves the
inputs live and the output an orphan; after the swap, the inputs are orphans.
Input files are deleted lazily (at `Dispose`, or by the next open's orphan
cleanup) and retired readers stay usable until then, so a snapshot scan taken
before a compaction keeps reading its consistent view.

## Concurrency

* One process per directory, enforced by an OS file lock (`LOCK`).
* Inside the process: a reader/writer lock. Any number of concurrent readers;
  one writer at a time. Writes are serialized — there is no lock-free path.
* Scans are snapshots: the source list (a memtable entry copy + the table
  readers) is captured under the read lock at `Scan()` call time, and the
  merge runs lock-free over immutable data afterwards. Later writes are
  invisible to the scan; later deletes do not hide its rows.

## Guarantees — stated precisely

A write is *acknowledged* when `Put`/`Delete`/`Write` returns.

**`FsyncPolicy.Always`** (the default)

| Event | What survives |
|---|---|
| Process crash | Every acknowledged write. |
| OS crash / power loss | Every acknowledged write. Each record was fsynced before the call returned. |
| Crash *during* a write call | That write is present whole or not at all (frame CRC); every earlier acknowledged write is present. A batch is never partially visible. |

**`FsyncPolicy.Interval`**

| Event | What survives |
|---|---|
| Process crash | Every acknowledged write (records are handed to the OS synchronously; the OS page cache survives the process). |
| OS crash / power loss | Everything up to the last completed interval fsync. Writes acknowledged since then may be lost — up to one interval's worth. |

**Always true, under either policy**

* No acknowledged write is ever silently corrupted or half-applied: damage is
  either a truncated torn tail (data after the last valid record was never
  acknowledged) or a loud `StorageCorruptionException` at open.
* A clean `Dispose()` fsyncs, so a dispose/reopen cycle loses nothing under
  either policy.
* Deletes are writes: a tombstone is as durable as the value it removes.
* One platform-qualified edge stands next to this list, stated in full under
  *Deviations*: fsync makes a record's **bytes** durable; the **directory
  entry** naming its file follows the filesystem's metadata rules. On NTFS,
  on APFS, and on Linux journaling filesystems in their default modes,
  created and renamed entries are durable as journaled metadata, and the
  tables above hold exactly as written. On a non-journaling POSIX
  filesystem, a power loss can in theory revert a create/rename from a
  flush window — the one gap between this statement and full POSIX-grade
  durability, named rather than hidden.

## Deviations from a textbook LSM, on purpose

* **No immutable-memtable queue (background flush — evaluated, not adopted).**
  Textbook designs rotate the memtable to an immutable one and flush it on a
  background thread so writes never stall. Here the flush happens
  synchronously under the write lock. With a 4 MB default memtable the stall
  is a few milliseconds, and in exchange an entire class of races (flush
  failing in the background, reads spanning three memtable generations, WAL
  generation bookkeeping across rotations) does not exist. Read order is
  simply: memtable, then tables.

  This was re-evaluated in the hardening pass, and the evaluation is recorded
  here because it is a decision, not an oversight. Rotation forces the live
  WAL to switch to a new generation *immediately* — new writes cannot wait
  for the background flush — while the rotated generation's records are not
  yet durable anywhere but that generation's WAL file. So for the whole
  duration of a background flush, **two** WAL generations are live, and a
  crash must replay both, in order. The manifest names exactly one live
  generation, and the flush protocol's crash windows (above) are all stated
  against that fact: with a single-generation manifest, recovery from a
  mid-rotation crash would treat one of the two live generations as an
  orphan and delete it — losing acknowledged writes, or resurrecting flushed
  ones. A safe background flush therefore needs a manifest format change
  first (a generation list plus a pending-flush table slot) and a new
  recovery matrix for the states it creates. Two further obstacles stand
  behind that one: a background flush can fail (disk full, I/O error) with
  no caller to throw to — every write it covers was already acknowledged
  against the WAL — so the engine would need a poisoned-state semantic the
  current contract does not have; and clean shutdown and the crash test hook
  would both have to rendezvous with the flush thread, so a flush completing
  concurrently could not swap tables or save the manifest into an instance
  that is already logically dead. The benefit on offer — removing a
  few-millisecond stall at the default memtable size — does not pay for
  re-opening the recovery proof. The prerequisite for any future attempt is
  named: the manifest format change, with its own crash-state tests written
  before the rotation itself.
* **Full-merge compaction** instead of leveled compaction (see above).
* **Directory fsyncs are not issued** after renames or file creations; file
  fsyncs are. The distinction matters: fsyncing a record makes its *bytes*
  durable, while the *name* — the directory entry a WAL file is created
  under, an SSTable is renamed to, the manifest is renamed over — becomes
  durable under POSIX only when the containing directory is itself fsynced.
  .NET exposes no managed API for that: `FileStream` and `File.OpenHandle`
  refuse directory paths outright (verified empirically on .NET 8, Linux —
  both throw `UnauthorizedAccessException`), and Windows has no equivalent
  call in the managed surface either. The only route would be native interop
  (libc `open`/`fsync` on Unix; `CreateFile` with backup-semantics plus
  `FlushFileBuffers` on Windows) — per-platform native code in a library
  whose single dependency rule is "none". The gap is therefore closed by
  statement instead of by code, precisely:

  *What it can reach:* only the instants where a name is created or replaced
  — a WAL file appearing at open or at a flush's generation switch, an
  SSTable's rename at flush or compaction, the MANIFEST rename that follows.
  If the OS kept the paired deletion (of the previous WAL generation) but
  lost the rename, that flush's acknowledged writes could go with it: the
  newest manifest reads as absent, the new table is an orphan, the old log
  is gone. Record-level fsync operates below the file-name abstraction and
  cannot see this.

  *What it cannot reach:* the bytes of any file whose name is already
  durable. A database that has sat through one completed flush cycle has all
  its names settled, and every guarantee in the tables above then holds in
  full.

  *Per platform:* on Windows/NTFS the exposure is effectively absent —
  NTFS journals metadata, so a create or rename that has returned survives
  power loss; that is precisely why no directory-sync call exists for .NET
  to expose. macOS/APFS journals metadata the same way. On Linux, ext4 in
  its default `ordered` mode, XFS, and Btrfs all journal metadata, making
  the same outcome de-facto reliable — but POSIX does not *guarantee* it
  without the directory fsync, and a non-journaling filesystem (or a
  journal-disabled mount) is genuinely exposed. An embedder who needs the
  guarantee on such a filesystem today has one lever: place the database
  directory on a journaling filesystem, which is the default on mainstream
  Linux distributions.

## Limitations

* Byte-array keys and values only; no comparators, no column families, no TTL.
* No transactions beyond single-batch atomicity (no read-your-writes isolation
  levels, no multi-batch rollback).
* Single writer process; no network interface — this is an embeddable library.
* Compaction streams (see above), but it is still a *full* merge: every
  compaction rewrites every live byte, so write amplification — not memory —
  is the cost that grows with data size. A leveled design is the named next
  step if that ever matters more than the simple correctness story.
* Values are returned as copies from `Get`; `Put` takes ownership of the
  arrays it is given (mutating them afterwards is a caller bug).
