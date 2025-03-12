using Cassandra;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigratorCassandraProvider : CassandraProvider
    {
        public MigratorCassandraProvider(ICronusContextAccessor cronusContextAccessor, IOptionsMonitor<MigrationCassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
            : base(cronusContextAccessor, optionsMonitor, keyspaceNamingStrategy, replicationStrategy, logger, initializer) { }
    }
}
