using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigratorCassandraProvider : CassandraProvider
    {
        public MigratorCassandraProvider(IOptionsMonitor<MigrationCassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer, ILock @lock)
            : base(optionsMonitor, keyspaceNamingStrategy, replicationStrategy, initializer, @lock)
        {

        }
    }
}
