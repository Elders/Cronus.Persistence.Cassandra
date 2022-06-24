using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra
{
    /*    public sealed class TablePerBoundedContext : ITableNamingStrategy
        {
            private readonly BoundedContext boundedContext;

            public TablePerBoundedContext(IOptionsMonitor<BoundedContext> boundedContext)
            {
                this.boundedContext = boundedContext.CurrentValue;
            }

            public string GetName()
            {
                return $"{boundedContext.Name}Events".ToLower();
            }
        }*/
}
