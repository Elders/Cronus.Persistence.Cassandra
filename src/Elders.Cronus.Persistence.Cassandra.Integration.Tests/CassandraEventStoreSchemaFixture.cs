using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraEventStoreSchemaFixture
{
    private readonly CassandraFixture cassandraFixture;

    public CassandraEventStoreSchemaFixture(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    public CassandraEventStoreSchema GetEventStoreSchema(ITableNamingStrategy namingStrategy, ICassandraReplicationStrategy replicationStrategy)
    {
        return new CassandraEventStoreSchema(cassandraFixture, namingStrategy, replicationStrategy);
    }
}
