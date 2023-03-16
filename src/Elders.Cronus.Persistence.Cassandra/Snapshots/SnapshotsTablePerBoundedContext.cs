using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Snapshots
{
    public sealed class SnapshotsTablePerBoundedContext : ISnapshotsTableNamingStrategy
    {
        private readonly BoundedContext boundedContext;

        public SnapshotsTablePerBoundedContext(IOptionsMonitor<BoundedContext> boundedContext)
        {
            this.boundedContext = boundedContext.CurrentValue;
        }

        public string GetName()
        {
            return $"{boundedContext.Name}Snapshots".ToLower();
        }
    }
}
