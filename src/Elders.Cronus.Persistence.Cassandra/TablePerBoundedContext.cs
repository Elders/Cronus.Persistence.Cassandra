using System;
using System.Collections.Concurrent;
using System.Reflection;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class TablePerBoundedContext : ICassandraEventStoreTableNameStrategy
    {
        private readonly ConcurrentDictionary<string, string> eventsTableName = new ConcurrentDictionary<string, string>();
        private readonly Assembly aggregatesAssemblies;

        public TablePerBoundedContext(Assembly aggregatesAssemblies)
        {
            this.aggregatesAssemblies = aggregatesAssemblies;
        }

        public string GetEventsTableName(AggregateCommit aggregateCommit)
        {
            // mynkow if(Environment.GetEnvironmentVariable("ForceCronusChecks"))
            // if (boundedContext)

            var boundedContext = aggregateCommit.BoundedContext;
            return GetEventsTableName(boundedContext);
        }

        public string[] GetAllTableNames()
        {
            {
                var boundedContext = aggregatesAssemblies.GetBoundedContext().BoundedContextName;
                return (new System.Collections.Generic.List<string>() { GetEventsTableName(boundedContext) }).ToArray();
            }
        }

        public string GetEventsTableName(string boundedContext)
        {
            string tableName;
            if (!eventsTableName.TryGetValue(boundedContext, out tableName))
            {
                tableName = String.Format("{0}Events", boundedContext).ToLowerInvariant();
                eventsTableName.TryAdd(boundedContext, tableName);
            }
            return tableName;
        }
    }
}