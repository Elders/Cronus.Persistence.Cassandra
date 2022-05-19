using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class TablePerBoundedContextNew : ITableNamingStrategy
    {
        private readonly BoundedContext boundedContext;

        public TablePerBoundedContextNew(IOptionsMonitor<BoundedContext> boundedContext)
        {
            this.boundedContext = boundedContext.CurrentValue;
        }

        public string GetName()
        {
            return $"{boundedContext.Name}Events_preview".ToLower();
        }
    }
}
