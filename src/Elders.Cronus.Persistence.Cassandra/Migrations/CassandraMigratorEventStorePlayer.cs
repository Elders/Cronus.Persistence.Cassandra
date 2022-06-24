using Elders.Cronus.Migrations;
using Elders.Cronus.Persistence.Cassandra.Preview;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class CassandraMigratorEventStorePlayer : CassandraEventStoreNew<MigratorCassandraReplaySettings>, IMigrationEventStorePlayer
    {
        public CassandraMigratorEventStorePlayer(MigratorCassandraReplaySettings settings, ILogger<CassandraEventStoreNew> logger) : base(settings, logger)
        {

        }
    }
}
