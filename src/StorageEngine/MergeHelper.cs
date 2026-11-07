namespace StorageEngine;

/// <summary>
/// K-way merge over sorted entry sources ordered newest-first. For duplicate
/// keys the copy from the newest source wins and older copies are suppressed;
/// tombstones are yielded like any other entry (they must suppress older
/// values) and filtered out by the consumer.
/// </summary>
internal static class MergeHelper
{
    public static IEnumerable<KeyValuePair<byte[], byte[]?>> MergeByRecency(
        IReadOnlyList<IEnumerable<KeyValuePair<byte[], byte[]?>>> sourcesNewestFirst)
    {
        var enumerators = new List<IEnumerator<KeyValuePair<byte[], byte[]?>>>();
        var queue = new PriorityQueue<
            (IEnumerator<KeyValuePair<byte[], byte[]?>> It, int Rank),
            (byte[] Key, int Rank)>(PriorityComparer.Instance);
        try
        {
            for (var rank = 0; rank < sourcesNewestFirst.Count; rank++)
            {
                var it = sourcesNewestFirst[rank].GetEnumerator();
                enumerators.Add(it);
                if (it.MoveNext())
                    queue.Enqueue((it, rank), (it.Current.Key, rank));
            }

            byte[]? lastYieldedKey = null;
            while (queue.Count > 0)
            {
                var (it, rank) = queue.Dequeue();
                var current = it.Current;
                var isNewestForKey = lastYieldedKey is null ||
                                     ByteArrayComparer.Instance.Compare(current.Key, lastYieldedKey) != 0;
                if (isNewestForKey)
                {
                    lastYieldedKey = current.Key;
                    yield return current;
                }
                if (it.MoveNext())
                    queue.Enqueue((it, rank), (it.Current.Key, rank));
            }
        }
        finally
        {
            foreach (var it in enumerators)
                it.Dispose();
        }
    }

    /// <summary>Orders by key ascending, then by source rank ascending (newest source first).</summary>
    private sealed class PriorityComparer : IComparer<(byte[] Key, int Rank)>
    {
        public static readonly PriorityComparer Instance = new();

        public int Compare((byte[] Key, int Rank) x, (byte[] Key, int Rank) y)
        {
            var byKey = ByteArrayComparer.Instance.Compare(x.Key, y.Key);
            return byKey != 0 ? byKey : x.Rank.CompareTo(y.Rank);
        }
    }
}
