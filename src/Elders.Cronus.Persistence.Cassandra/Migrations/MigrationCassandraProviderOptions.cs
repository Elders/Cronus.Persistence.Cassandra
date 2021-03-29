using Cassandra;
using Elders.Cronus.Migrations;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigrationCassandraProviderOptions : CassandraProviderOptions { }

    public class MigrationCassandraProviderOptionsProvider : CronusOptionsProviderBase<MigrationCassandraProviderOptions>
    {
        public const string SettingKey = "cronus:migration:source:cassandra";

        public MigrationCassandraProviderOptionsProvider(IConfiguration configuration) : base(configuration) { }

        public override void Configure(MigrationCassandraProviderOptions options)
        {
            configuration.GetSection(SettingKey).Bind(options);
        }
    }

    public class MigratorCassandraProvider : CassandraProvider
    {
        public MigratorCassandraProvider(IOptionsMonitor<MigrationCassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer = null)
            : base(optionsMonitor, keyspaceNamingStrategy, replicationStrategy, initializer)
        {

        }
    }

    public class MigratorCassandraReplaySettings : CassandraEventStoreSettings
    {
        public MigratorCassandraReplaySettings(MigratorCassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer) : base(cassandraProvider, tableNameStrategy, serializer)
        {

        }
    }


    public class CassandraMigratorEventStorePlayer : CassandraEventStore<MigratorCassandraReplaySettings>, IMigrationEventStorePlayer
    {
        public CassandraMigratorEventStorePlayer(MigratorCassandraReplaySettings settings) : base(settings)
        {

        }
    }
}
