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

### MANIFEST — format v2

A small text file, replaced atomically: write `MANIFEST.tmp`, fsync, rename
over the old file. A crash therefore leaves the old or the new manifest, never
a torn one. Files present on disk but not named by the manifest are orphans of
interrupted operations and are deleted on open.

```
# LSM storage engine manifest. ...
format 2
next-sst-id 7
wal-gen 3              one line per live WAL generation, oldest first;
wal-gen 5              the last one is the append target
pending-flush 3 6      optional, at most one: generation 3 is being
                       flushed into table 6 (see the flush protocol)
sst 1                  one line per live table
sst 2
```

A **generation list**, rather than a single live generation, because a
rotation seals one generation and starts the next *before* the sealed one is
flushed: for the duration of a background flush, two generations hold live
records, and recovery must replay both, in order. A generation leaves the
list only at the commit that lands its records in a manifest table.

The **pending-flush slot** names the one flush in flight: the sealed
generation being written out and the table id reserved for its output. The
generation stays in the list and the id stays out of `sst` lines until the
commit, so every crash state the slot can describe has one unambiguous
recovery (below).

Format v1 (the original layout — no `format` line, exactly one `wal-gen`,
no pending slot) still loads: it is a one-element generation list. The first
save after such an open rewrites the file as v2 with identical meaning —
v1 directories upgrade in place. Cross-field invariants the writer maintains
(generations strictly increasing; the pending generation listed, sealed —
never the append target — and its table not yet adopted) are validated on
load; a manifest violating them was not produced by a crash of this engine
and opening refuses with `StorageCorruptionException`.

## Flush protocol

Flushing is a **background** operation in three phases. Rotation is instant
and happens under the write lock; the table write runs on a worker thread;
the commit takes the write lock briefly.

**Rotation** (triggered when the memtable crosses its size threshold, or by
`Flush()`):

1. Create the *next* WAL generation's file and fsync it — before the
   manifest names it.
2. Save the manifest: the new generation joins the list as the append
   target, the sealed generation stays listed and gains the pending-flush
   slot with a reserved table id, and `next-sst-id` advances past it. (If
   this save fails, the rotation rolls back in memory and the caller sees
   the exception; nothing rotated.)
3. Rotate in memory: the live memtable freezes as the *pending memtable*,
   a fresh memtable and the new WAL take over, and the worker starts.
   Writes never wait for the table write.

**Worker** (no locks): stream the frozen entries to `sst-<id>.tmp`, fsync,
rename to `sst-<id>.lsm`.

**Commit** (write lock, briefly):

4. Save the manifest: adopt the table, clear the pending slot, and retire
   every generation the flushed memtable covered — the sealed one, plus any
   older generations recovery had replayed into the same memtable.
5. Delete the retired generations' WAL files, open the table for reads,
   drop the pending memtable, and run the table-count compaction check.
6. If a flush was requested during the flight, or the live memtable
   already crossed its threshold again, chain the next rotation *before*
   reporting the pipeline idle — a caller waiting for the flush to settle
   never observes a false idle.

**Crash windows** — every state a crash can leave, and its recovery:

| Window | On-disk state | Recovery |
|---|---|---|
| Before the rotation save | Old manifest; the new WAL file exists empty, unlisted | File is an orphan, deleted; the old generation replays. Nothing rotated, nothing lost. |
| After rotation, before the table write | Pending slot set; both generations listed; no table file | Pending flush discarded: both generations replay; the slot is cleared and the resolution saved. |
| Mid-write | Pending slot set; `sst-<id>.tmp` partial | Temp file deleted as an orphan; generations replay. |
| After the rename, before the commit save | Pending slot set; `sst-<id>.lsm` complete but unlisted | Table deleted **unread** as an orphan; generations replay. (Recovery never adopts a table the manifest did not adopt — the WAL is the record.) |
| After the commit save, before WAL deletion | Table adopted; slot cleared; retired WAL files still present, now unlisted | Files deleted unread as orphans; the table serves the data. |

At no point can a record exist only in a file the manifest disowns, and at
every instant an acknowledged write is durable in exactly one place: the
current generation's WAL, a sealed generation's WAL, or its table.

**Flush failure.** If the worker's write or the commit throws (disk full,
I/O error), nothing durable changes: the generation stays listed, the slot
stays set, its WAL is untouched, and the frozen memtable stays in the read
path. The failure is recorded and the semantics are: **reads keep working;
writes throw** an `InvalidOperationException` naming the original failure,
until `Flush()` retries the same job (same generation, same reserved table
id) to completion, or the instance is disposed — a reopen recovers every
acknowledged write from the WALs either way. A failed generation is never
silently dropped, and a retry never renumbers anything.

## Recovery

On open, after loading the manifest and the tables, **every live WAL
generation is replayed, oldest first**, into a fresh memtable, using each
frame's CRC-32. (A generation's records are never also in a manifest table —
it leaves the list only at the commit that adopts its table — so replay
cannot double-apply a flush. A pending flush is resolved by the discard
rule in the table above, and the resolution is saved.) A WAL file the
manifest lists but disk does not have is loud `StorageCorruptionException`:
like a missing table, it held the only durable copy of acknowledged writes,
and opening must refuse rather than replay nothing and call the loss a
fresh database.

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
* Read order is: live memtable, then the pending (frozen) memtable while a
  flush is in flight, then tables newest-first. Reads never wait for the
  flush thread.
* The flush pipeline has explicit rendezvous points instead of lock-step
  coupling: `Flush()` returns only when the flush it started or requested —
  including a rotation chained from an in-flight commit — has committed;
  `Compact()` and the `TableCount` getter first wait for the pipeline to
  settle; `Dispose()` waits for the in-flight job to commit before tearing
  down. Waiting never happens while holding the database lock, and the
  worker needs only the write lock briefly at commit, so none of these can
  deadlock it. If the memtable crosses its threshold while a flush is in
  flight, it simply keeps growing until that flush's commit chains the
  next rotation — backpressure is growth bounded by flush speed, never a
  blocked writer.
* Scans are snapshots: the source list (memtable entry copies + the table
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
* Across a flush — synchronous observation or background flight — an
  acknowledged write is durable in exactly one place at every instant: the
  live WAL generation, one sealed generation's WAL, or its table. A failed
  background flush changes none of that (see *Flush failure* above): it can
  suspend writes, never lose them.
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

* **Background flush — adopted, on the prerequisite the earlier evaluation
  named.** The first hardening pass evaluated the textbook design (rotate
  the memtable, flush it on a background thread) and rejected it *at that
  time*, for one structural reason: rotation makes two WAL generations
  live at once, and the single-generation manifest could not express that
  — a mid-rotation crash would have orphaned a live generation. The
  evaluation named its prerequisite exactly: a manifest format change (a
  generation list plus a pending-flush slot) with its own crash-state
  tests written before the rotation itself. That is the order this pass
  followed: format v2 landed first with a hand-built crash-state matrix
  for every state the new fields can describe; the rotation was then built
  on top, and each of the evaluation's remaining obstacles got a stated
  answer — background failure has defined semantics (reads continue,
  writes suspend loudly, `Flush()` retries the identical job; see the
  flush protocol), and `Dispose`/`SimulateCrash` rendezvous with or
  abandon the worker explicitly, with checkpoints that keep a surviving
  worker from touching the directory after a crash. The queue depth is
  one: a single pending slot, matching the single slot in the format. The
  synchronous flush's one real virtue — the state space stayed tiny — is
  what the v2 crash matrix had to re-prove at the larger size; the suite
  counts are in the README's Tests section.
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
