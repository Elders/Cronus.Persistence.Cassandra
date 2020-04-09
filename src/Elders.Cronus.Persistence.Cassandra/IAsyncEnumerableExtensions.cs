using System.Collections.Generic;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal static class IAsyncEnumerableExtensions
    {
        public static async ValueTask<IEnumerable<T>> Wait<T>(this IAsyncEnumerable<T> source)
        {
            var list = new List<T>();

            await foreach (var item in source.ConfigureAwait(false))
            {
                list.Add(item);
            }

            return list;
        }
    }
}
