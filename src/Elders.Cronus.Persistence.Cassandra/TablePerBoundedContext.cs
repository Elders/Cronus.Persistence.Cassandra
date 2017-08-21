﻿using System;
using System.Collections.Concurrent;
using System.Reflection;
using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class TablePerBoundedContext : ICassandraEventStoreTableNameStrategy
    {
        private readonly ConcurrentDictionary<string, string> eventsTableName = new ConcurrentDictionary<string, string>();
        private readonly string boundedContextName;

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
            // mynkow if(Environment.GetEnvironmentVariable("ForceCronusChecks"))
            // if (boundedContext)

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
                tableName = String.Format("{0}Events", boundedContext).ToLowerInvariant();
                eventsTableName.TryAdd(boundedContext, tableName);
            }
            return tableName;
        }
    }
}
