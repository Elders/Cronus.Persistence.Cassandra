using System;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class TablePerBoundedContext : ITableNamingStrategy
    {
        private readonly BoundedContext boundedContext;

        public TablePerBoundedContext(BoundedContext boundedContext)
        {
            this.boundedContext = boundedContext;
        }

        public string GetName()
        {
            return $"{boundedContext.Name}Events".ToLower();
        }
    }
}
