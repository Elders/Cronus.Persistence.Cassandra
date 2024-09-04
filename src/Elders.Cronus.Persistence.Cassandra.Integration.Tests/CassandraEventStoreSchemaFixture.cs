namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraEventStoreSchemaFixture
{
    private readonly CassandraFixture cassandraFixture;

    public CassandraEventStoreSchemaFixture(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    public CassandraEventStoreSchema GetEventStoreSchema(ITableNamingStrategy namingStrategy)
    {
        return new CassandraEventStoreSchema(cassandraFixture, namingStrategy);
    }
}
