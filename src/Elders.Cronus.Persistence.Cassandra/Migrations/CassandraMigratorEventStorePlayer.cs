using Elders.Cronus.Migrations;
using Elders.Cronus.Persistence.Cassandra.Preview;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class CassandraMigratorEventStorePlayer : CassandraEventStore<MigratorCassandraReplaySettings>, IMigrationEventStorePlayer
    {
        public CassandraMigratorEventStorePlayer(MigratorCassandraReplaySettings settings, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger) : base(settings, indexByEventTypeStore, logger)
        {

        }
    }
}
