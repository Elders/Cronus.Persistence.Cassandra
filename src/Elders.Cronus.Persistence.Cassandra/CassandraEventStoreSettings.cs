namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSettings : ICassandraEventStoreSettings
    {
        public CassandraEventStoreSettings(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer)
        {
            CassandraProvider = cassandraProvider;
            TableNameStrategy = tableNameStrategy;
            Serializer = serializer;
        }

        public ICassandraProvider CassandraProvider { get; }
        public ITableNamingStrategy TableNameStrategy { get; }
        public ISerializer Serializer { get; }
    }
}
