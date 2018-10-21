using System;
using System.Collections.Concurrent;
using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class TablePerBoundedContext : ICassandraEventStoreTableNameStrategy
    {
        private readonly ConcurrentDictionary<string, string> eventsTableName = new ConcurrentDictionary<string, string>();

        public string GetEventsTableName(AggregateCommit aggregateCommit)
        {
            var boundedContext = aggregateCommit.BoundedContext;
            return GetEventsTableName(boundedContext);
        }

        public string[] GetAllTableNames(string boundedContext)
        {
            return (new System.Collections.Generic.List<string>() { GetEventsTableName(boundedContext) }).ToArray();
        }

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
