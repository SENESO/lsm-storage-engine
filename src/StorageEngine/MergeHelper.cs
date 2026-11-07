namespace StorageEngine;

/// <summary>
/// K-way merge over sorted entry sources ordered newest-first, under the
/// database's key comparer. For duplicate point keys the copy from the newest
/// source wins and older copies are suppressed; point tombstones are yielded
/// like any other entry (they must suppress older values) and filtered by the
/// consumer.
///
/// Range tombstones stream at their start key and are always yielded (the
/// consumer decides whether they may be dropped). From the moment one is
/// yielded it is <i>active</i>: a point entry from a strictly older source
/// whose key falls in [start, end) is suppressed — the range deleted it at a
/// newer recency. Entries stream in nondecreasing key order, so a tombstone
/// is always active before any point entry it can cover is reached; a point
/// entry from the <i>same</i> source as a tombstone is never suppressed by
/// it (within one source, a surviving point entry was written after the
/// tombstones that cover it — the source's stored state is already the
/// collapsed result of applying its operations in order).
/// </summary>
internal static class MergeHelper
{
    public static IEnumerable<StoreEntry> MergeByRecency(
        IReadOnlyList<IEnumerable<StoreEntry>> sourcesNewestFirst,
        IComparer<byte[]> comparer)
    {
        var enumerators = new List<IEnumerator<StoreEntry>>();
        var queue = new PriorityQueue<(IEnumerator<StoreEntry> It, int Rank), (byte[] Key, int Rank)>(
            new PriorityComparer(comparer));
        // Range tombstones yielded so far, as (end, source rank). Ends are
        // pruned lazily: keys stream in ascending order, so an interval whose
        // end has been passed can never cover a later point entry.
        var activeRanges = new List<(byte[] End, int Rank)>();
        try
        {
            for (var rank = 0; rank < sourcesNewestFirst.Count; rank++)
            {
                var it = sourcesNewestFirst[rank].GetEnumerator();
                enumerators.Add(it);
                if (it.MoveNext())
                    queue.Enqueue((it, rank), (it.Current.Key, rank));
            }

            byte[]? lastPointKey = null;
            while (queue.Count > 0)
            {
                var (it, rank) = queue.Dequeue();
                var current = it.Current;
                if (it.MoveNext())
                    queue.Enqueue((it, rank), (it.Current.Key, rank));

                if (current.Kind == EntryKind.RangeTombstone)
                {
                    activeRanges.Add((current.RangeEnd!, rank));
                    yield return current;
                    continue;
                }

                if (lastPointKey is not null && comparer.Compare(current.Key, lastPointKey) == 0)
                    continue; // an older copy of a key already decided

                lastPointKey = current.Key;
                var suppressed = false;
                for (var i = activeRanges.Count - 1; i >= 0; i--)
                {
                    if (comparer.Compare(activeRanges[i].End, current.Key) <= 0)
                    {
                        activeRanges.RemoveAt(i); // passed: covers nothing further
                        continue;
                    }
                    if (activeRanges[i].Rank < rank)
                        suppressed = true;
                }
                if (!suppressed)
                    yield return current;
            }
        }
        finally
        {
            foreach (var it in enumerators)
                it.Dispose();
        }
    }

    /// <summary>Orders by key ascending under the database comparer, then by source rank ascending (newest source first).</summary>
    private sealed class PriorityComparer : IComparer<(byte[] Key, int Rank)>
    {
        private readonly IComparer<byte[]> _comparer;

        public PriorityComparer(IComparer<byte[]> comparer) => _comparer = comparer;

        public int Compare((byte[] Key, int Rank) x, (byte[] Key, int Rank) y)
        {
            var byKey = _comparer.Compare(x.Key, y.Key);
            return byKey != 0 ? byKey : x.Rank.CompareTo(y.Rank);
        }
    }
}
