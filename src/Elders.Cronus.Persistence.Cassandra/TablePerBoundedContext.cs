using System;
using System.Collections.Concurrent;
using System.Reflection;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class TablePerBoundedContext : ICassandraEventStoreTableNameStrategy
    {
        private readonly ConcurrentDictionary<string, string> eventsTableName = new ConcurrentDictionary<string, string>();
        private readonly string boundedContextName;

        public TablePerBoundedContext(IConfiguration configuration)
        {
            boundedContextName = configuration["cronus_boundedcontext"];
        }

        public TablePerBoundedContext(Assembly aggregatesAssemblies)
        {
            this.boundedContextName = aggregatesAssemblies.GetBoundedContext().BoundedContextName;
        }

        public TablePerBoundedContext(string boundedContextName)
        {
            this.boundedContextName = boundedContextName;
        }

        public string GetEventsTableName(AggregateCommit aggregateCommit)
        {
            var boundedContext = aggregateCommit.BoundedContext;
            return GetEventsTableName(boundedContext);
        }

        public string[] GetAllTableNames()
        {
            return (new System.Collections.Generic.List<string>() { GetEventsTableName(boundedContextName) }).ToArray();
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
