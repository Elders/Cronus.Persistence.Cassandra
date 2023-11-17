using Cassandra;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigratorCassandraProvider : CassandraProvider
    {
        public MigratorCassandraProvider(IOptionsMonitor<MigrationCassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
            : base(optionsMonitor, keyspaceNamingStrategy, replicationStrategy, logger, initializer) { }
    }
}
