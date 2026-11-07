# Design

An LSM-tree (log-structured merge-tree) key-value storage engine in C#/.NET 8,
with a hand-rolled CRC-32 and zero package dependencies. This document describes
the architecture, the exact on-disk formats, the crash-recovery algorithm, and —
precisely — what is and is not guaranteed.

## Architecture

```
          Put / Delete / DeleteRange / Write(batch)
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
              │  + bloom filter       │  + bloom filter + range index
              └───────────┬───────────┘
              strategy trigger (see Compaction)
                          ▼
              ┌───────────────────────┐
              │      Compaction       │  tiered: full merge, or
              │                       │  leveled: incremental, by level
              └───────────────────────┘

  MANIFEST — the atomic source of truth for which files are live.
  LOCK     — an OS file lock: one open database per directory, period.
```

**Write path.** Encode the operation (a batch becomes one record) → append to
the WAL and flush per policy → apply to the memtable → flush the memtable if it
crossed its size threshold.

**Read path.** Memtable → SSTables newest-first, where "newest" is level
ascending, then table id descending (under the tiered strategy every table is
level 0, so this is simply newest table first). A lookup resolves the first
layer that has a verdict for the key — a value, a point tombstone, or a range
tombstone covering it — and a value whose TTL deadline has passed counts as
absent at its own layer and shadows everything beneath it. An SSTable read
checks the key range, then the bloom filter, then binary-searches the sparse
index and scans forward at most `IndexSampleInterval` entries; range coverage
is answered from a dedicated ranges section, not by scanning data.

**Memtable choice.** A `SortedDictionary` (red-black tree) ordered by the
database's key comparer (byte-wise by default), rather than a skip list. The
engine has a single writer and guards the memtable with the database lock, so
the skip list's one advantage — lock-free concurrent mutation — buys nothing
here. The tree gives deterministic O(log n) operations, in-order enumeration
for flushes and scans for free, and no probabilistic edge cases. Entry size
accounting is approximate: key + value bytes + a fixed 48-byte per-entry
overhead estimate.

## File formats

All integers are little-endian. Keys and values are raw byte arrays; ordering
is the database's key comparer — unsigned lexicographic byte order unless a
custom comparer was supplied at creation (see *Ordering, expiry, and range
deletes*).

### WAL — `wal-<generation>.log`

A sequence of record frames:

```
[payloadLength int32][crc32 uint32 of payload][payload]
```

Payload layouts:

```
Put:           [0x01][keyLen int32][key][valueLen int32][value]
Delete:        [0x02][keyLen int32][key]
Batch:         [0x03][opCount int32] then per op (below)
PutWithExpiry: [0x04][keyLen int32][key][expiresAtMs int64][valueLen int32][value]
RangeDelete:   [0x05][startLen int32][start][endLen int32][end]
```

A batch's ops use the same per-op layouts, prefixed by their type byte
(0x01, 0x02, 0x04, or 0x05); a batch put-with-TTL is encoded as 0x04 with the
*absolute* deadline — the engine converts the caller's relative TTL against
its clock when the batch is written, so replay never depends on when it runs.
Deadlines are unix milliseconds. The 0x01/0x02 layouts are byte-identical to
the original format: a WAL written by the previous generation decodes exactly
as it did then.

A batch is a *single* record, so the frame CRC makes it atomic: recovery
applies the whole batch or none of it.

### SSTable — `sst-<id>.lsm` (format v2)

```
Data section — entries in ascending key order (a range tombstone sorts at
its start key, after a point entry with the same key):
  [keyLen int32][key][flags byte: 0 = value, 1 = tombstone,
                                2 = value with expiry, 3 = range tombstone]
  [expiresAtMs int64]                          (flag 2 only)
  [valueLen int32][value]                      (flags 0 and 2)
  [endLen int32][end]                          (flag 3 only: the range end)
Index section — sparse index:
  [keyLen int32][key][dataOffset int64]        one per sampled entry
                                               (entry 0 always; the last entry always)
Bloom section:
  [hashCount int32][bit bytes]                 ~10 bits/key by default
Ranges section — the table's range tombstones, normalized to disjoint,
start-sorted intervals (overlaps merged by the writer):
  [count int32] then per interval: [startLen int32][start][endLen int32][end]
Footer — fixed 40 bytes at end of file:
  [indexOffset int64][bloomOffset int64][bloomLength int32]
  [entryCount int32][rangeOffset int64][magic 8 bytes "LSMDB002"]
```

The ranges section exists so a point lookup can answer "does a range
tombstone here cover this key?" in O(log n) without scanning the data
section; the flag-3 entries in the data section are what scans and merges
consume, and the two are written from the same source list.

The bloom filter derives its k probe positions by double hashing over CRC-32
and FNV-1a of the key. It can only err towards "maybe present" — never towards
"absent" for a key that was written. It is keyed on exact key bytes, so it is
consulted only under the built-in byte ordering: a custom comparer may equate
distinct byte strings (a case-insensitive ordering does), and under one, a
bloom miss would not prove absence — the index walk alone decides there.

**Format v1 tables still load.** The previous generation's layout (flags 0/1
only, no ranges section, fixed 32-byte footer, magic "LSMDB001") is read by
the same reader — a directory can hold both generations, and merges consume
v1 tables through the same entry stream as v2. Nothing is rewritten eagerly;
a v1 table leaves the directory when a merge retires it, like any other
table. The test suite builds a v1 file byte-for-byte to the old spec and
reads it back.

### MANIFEST — format v2

A small text file, replaced atomically: write `MANIFEST.tmp`, fsync, rename
over the old file. A crash therefore leaves the old or the new manifest, never
a torn one. Files present on disk but not named by the manifest are orphans of
interrupted operations and are deleted on open.

```
# LSM storage engine manifest. ...
format 2
comparator bytewise    the recorded key-comparer identity (see below)
next-sst-id 7
wal-gen 3              one line per live WAL generation, oldest first;
wal-gen 5              the last one is the append target
pending-flush 3 6      optional, at most one: generation 3 is being
                       flushed into table 6 (see the flush protocol)
sst 1 0                one line per live table: id, then compaction level
sst 2 1                (the level field may be absent in older files = 0)
```

Two grammar extensions ride on the v2 format without bumping it, because
both degrade safely in the direction that matters. The `sst` level field is
absent from manifests written before levels existed and parses as level 0 —
exactly right, since the tiered strategy treats every table as level 0
anyway. The `comparator` line records the name the database's key comparer
was opened with (`bytewise` for the built-in ordering, including every
directory created before comparers existed — a missing line means
`bytewise`). Opening a directory whose recorded name differs from the name
supplied in `StorageOptions` is refused with `InvalidOperationException`
before anything is read or written: every sorted structure on disk is laid
out under the recorded ordering, and interpreting it under another would
silently corrupt reads. The refusal compares *names*, deliberately — the
engine cannot verify that two comparer objects with the same name order
identically, so the name is the contract the embedder owns.

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

## Ordering, expiry, and range deletes

**Custom comparators.** `StorageOptions.KeyComparer` replaces the key
ordering everywhere — memtable, tables, merges, scans, range operations —
and `KeyComparerName` gives that ordering an identity, recorded in the
manifest (the open-time refusal on a mismatch is described under *MANIFEST*).
The built-in default is unsigned lexicographic byte order, name `bytewise`.
Two consequences are worth stating. First, equality is the comparer's: under
a case-insensitive comparer, `User:42` and `USER:42` are the same key for
puts, gets, and deletes alike. Second, the per-table bloom filter is keyed on
exact bytes and is therefore consulted only under the built-in ordering (see
*SSTable*) — correctness under a custom comparer never depends on the
comparer's relationship to byte equality.

**Per-key TTL.** `Put(key, value, ttl)` and `Expire(key, ttl)` attach an
absolute deadline — unix milliseconds, computed from the database clock at
write time — that is persisted with the value through the WAL (record 0x04),
the memtable, and the tables (entry flag 2), and so survives crash and
reopen with the original deadline. The clock is `StorageOptions.Clock`,
defaulting to the system clock; tests substitute a manually advanced clock,
so no test ever sleeps. Semantics:

* Expiry is **lazy and exact**: a `Get` or scan at or after the deadline
  sees the key as absent. No background sweeper exists; an expired value's
  bytes are reclaimed when its layer is next written (flush or merge).
* An expired newest version **shadows** older copies exactly like a
  tombstone does — expiry never resurrects an older value.
* A plain `Put` clears any deadline; a later `Put` with a TTL replaces it.
  `Expire` on a missing or already-expired key returns `false` and writes
  nothing. `Delete` on an expired value likewise writes nothing.
* At flush, a value already expired converts to a point tombstone (the
  shadow must survive; the bytes need not). At a bottom merge — a tiered
  full merge, or a leveled merge whose drop test passes — expired values
  and their shadows are dropped entirely.

**Range deletes.** `DeleteRange(start, end)` writes one range tombstone for
the half-open span `[start, end)` (in comparer order; an empty span is a
no-op that writes nothing, a reversed span is an argument error). It is a
first-class write with the same guarantees as a point tombstone: one WAL
record (0x05, or one op inside a batch, applied in batch order), durable
before the call returns, flowing memtable → flush → compaction. Inside the
memtable it also removes covered point entries physically — they can never
be observed again — while the tombstone itself is kept to shadow older
layers. In a table it is stored twice from one source: as data entries for
scans and merges, and as normalized intervals in the ranges section for
point lookups. During a merge, a range tombstone from a newer input
suppresses covered point entries from older inputs; entries from its own
input are never suppressed by it (application order within one layer was
already resolved when that layer was written). A put inside a deleted span,
at any later time and in any newer layer, is unaffected — the tombstone
shadows only what is older than it.

## Compaction

Two strategies, selected at open by `StorageOptions.CompactionStrategy`.
Both consume and produce the same table format through the same streaming
k-way merge; they differ in *which* tables a merge takes as inputs and in
when a tombstone may be dropped. A directory may switch strategies between
opens — levels are manifest metadata, and reads order tables level-then-id
under either strategy.

### Tiered (the default)

Trigger: after a flush, when the live table count reaches
`CompactionTableThreshold` (also callable directly via `Compact()`).

Policy: size-tiered in spirit — tables accumulate as a tier of flush outputs,
and the merge is a **full merge of every live table** into one. That trades
write amplification (every compaction rewrites all data) for a much simpler
correctness story, which is the right trade at small scale — and the reason
it stays the default.

During the merge:

* for duplicate keys, the newest table's version wins;
* overwritten entries are dropped;
* **tombstones are purged.** This is safe *because* the merge spans every
  table: there is no older copy of any key beneath the merge for a tombstone
  to shadow. (The memtable is newer than every table and is not part of the
  merge.) Every tiered merge is a bottom merge: point tombstones, range
  tombstones, and expired values all leave the database here.

### Leveled

Flush outputs land in level 0. A *settle* pass — run at every flush commit
and by `Compact()` — repeats merges until nothing is owed:

* **L0 drain:** when level 0 holds at least `CompactionTableThreshold`
  tables, all of them merge into level 1, together with every level-1 table
  whose span they touch.
* **Level step:** the first level L ≥ 1 whose total size exceeds its budget
  — `LeveledBaseLevelSizeBytes × LeveledSizeRatio^(L−1)` — contributes its
  smallest-starting-key table, plus every level-(L+1) table whose span it
  touches, to a merge into level L+1.

Because every merge into a level includes all overlapping tables already
there, **each level above 0 is key-range partitioned**: its tables never
overlap, so a point read consults at most one table per level. Outputs are
split at `LeveledTargetTableSizeBytes`. The manifest swap that adopts a
merge's outputs is a single save, with the same crash windows as the tiered
merge (outputs are orphans before it, inputs are orphans after it).

**Tombstone discipline** is the leveled difference, and it is a proof
obligation per merge, not a level number: a point tombstone (or an expired
value, which behaves as one) for key K may be dropped only if no live table
*outside the merge's inputs* has a span that could contain K; a range
tombstone for `[s, e)` may be dropped only if no outside table's span
intersects it. Anything not provably droppable is carried into the output
(an expired value is carried as a point tombstone — same shadow, no value
bytes). Resurrection is therefore impossible by construction: a tombstone
outlives every copy it could shadow, and disappears only in a merge that
contains them all. The suite proves both directions — retention with an
older copy pinned in a non-input table below, and the drop once a merge
includes that table.

**Measured amplification**, on the suite's fixed workload (400 keys,
~210-byte values, ~85 KB live set; initial load, then 12 rounds each
rewriting a tenth of the keys, with a flush and a `Compact()` per round;
counted by the engine's own SSTable-bytes-written counter, same machine and
build as the README benchmarks):

| Strategy | SSTable bytes written | vs live set |
|---|---|---|
| Tiered (full merge per round) | 1,373,116 | ~16× |
| Leveled (base 16 KB, ratio 4, target 16 KB) | 744,822 | ~9× |

Leveled writes roughly half the bytes here — the expected direction, since
a round's small update batch no longer drags the whole live set through a
merge — at the cost of the partitioning and tombstone bookkeeping above,
and of more live tables at rest (8 vs 1 at the end of this workload).

### The merge itself (both strategies)

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
* Deletes are writes: a tombstone is as durable as the value it removes —
  and a range delete is one tombstone write with the same standing, not a
  per-key loop.
* Expiry is not a durability event: a TTL deadline is an absolute timestamp
  persisted with the value, so a crash and a long downtime change nothing
  about when a key expires — a value whose deadline passed while the
  process was down is simply absent on the next read.
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
* **Full-merge compaction is the default**, with leveled compaction as an
  opt-in strategy rather than the only design (see *Compaction*). The
  textbook (LevelDB-style) answer makes leveled the design and pays its
  complexity always; here the simple correctness story — every merge is a
  bottom merge, tombstones purge unconditionally — stays the default, and
  the leveled machinery (partitioned levels, per-merge tombstone proof
  obligations) is carried only by embedders who select it for the write
  amplification. The two share the merge, the formats, and the crash
  protocol; the measured difference on the reference workload is in the
  Compaction section.
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

* Byte-array keys and values only; no column families — keys that need
  grouping carry their group in the key bytes (a prefix), as in the systems
  this design follows. This is the one capability gap the design records
  without a scheduled direction: nothing in the formats forbids it, and no
  part of the roadmap depends on it.
* No transactions beyond single-batch atomicity (no read-your-writes isolation
  levels, no multi-batch rollback).
* Single writer process; no network interface — this is an embeddable library.
* The default (tiered) compaction is still a *full* merge: every compaction
  rewrites every live byte, so write amplification — not memory — is the
  cost that grows with data size. The leveled strategy exists for exactly
  that cost (see *Compaction* for the measured comparison); it is a
  strategy choice at open, not a free upgrade — it keeps more tables at
  rest and carries tombstones longer by design.
* Expiry is lazy: an expired value's bytes are reclaimed only when its
  layer is next written, so disk usage can temporarily include dead values
  (reads never see them).
* Values are returned as copies from `Get`; `Put` takes ownership of the
  arrays it is given (mutating them afterwards is a caller bug).
