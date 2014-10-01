using System;
using System.Collections.Concurrent;
using System.Reflection;
using Elders.Cronus.DomainModeling;

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

        /// <summary>
        /// Gets the events table name for the specified aggregate root.
        /// </summary>
        /// <param name="aggregateRoot">The aggregate root which knows about its Bounded Context. Usually the Bounded Context is specified in AssemblyAttribute.</param>
        /// <returns></returns>
        public string GetEventsTableName<AR>() where AR : DomainModeling.IAggregateRoot
        {
            // mynkow if(Environment.GetEnvironmentVariable("ForceCronusChecks"))
            // if (boundedContext)

            var boundedContext = typeof(AR).Assembly.GetBoundedContext().BoundedContextName;
            return GetTableName(boundedContext);
        }

        public string GetEventsTableName<AR>(AR aggregate) where AR : DomainModeling.IAggregateRoot
        {
            // mynkow if(Environment.GetEnvironmentVariable("ForceCronusChecks"))
            // if (boundedContext)

            var boundedContext = aggregate.GetType().Assembly.GetBoundedContext().BoundedContextName;
            return GetTableName(boundedContext);
        }

        public string[] GetAllTableNames()
        {
            //foreach (var assembly in aggregatesAssemblies)
            {
                var boundedContext = aggregatesAssemblies.GetBoundedContext().BoundedContextName;
                return (new System.Collections.Generic.List<string>() { GetTableName(boundedContext) }).ToArray();
            }
        }

        private string GetTableName(string boundedContext)
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