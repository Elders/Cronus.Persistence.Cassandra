using System;
using System.Collections.Concurrent;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class TablePerBoundedContext : ICassandraEventStoreTableNameStrategy
    {
        private readonly ConcurrentDictionary<string, string> eventsTableName = new ConcurrentDictionary<string, string>();

        public string GetEventsTableName(string boundedContext)
        {
            string tableName;
            if (!eventsTableName.TryGetValue(boundedContext, out tableName))
            {
                tableName = String.Format("{0}Events", boundedContext).ToLower();
                eventsTableName.TryAdd(boundedContext, tableName);
            }
            return tableName;
        }
    }
}
