using Elders.Cronus.Migrations;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class CassandraMigratorEventStorePlayer : CassandraEventStore<MigratorCassandraReplaySettings>, IMigrationEventStorePlayer
    {
        public CassandraMigratorEventStorePlayer(MigratorCassandraReplaySettings settings, ILogger<CassandraEventStore> logger) : base(settings, logger)
        {

        }
    }
}
