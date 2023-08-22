namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class MigratorCassandraReplaySettings : CassandraEventStoreSettings
    {
        public MigratorCassandraReplaySettings(MigratorCassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer)
            : base(cassandraProvider, tableNameStrategy, serializer)
        {

        }
    }
}
