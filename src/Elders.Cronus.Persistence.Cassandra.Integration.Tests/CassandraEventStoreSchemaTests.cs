namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraEventStoreSchemaTests : IClassFixture<CassandraEventStoreSchemaFixture>
{
    private readonly CassandraEventStoreSchemaFixture cassandraEventStoreSchemaFixture;
    private readonly CassandraFixture cassandraFixture;

    public CassandraEventStoreSchemaTests(CassandraEventStoreSchemaFixture cassandraEventStoreSchemaFixture, CassandraFixture cassandraFixture)
    {
        this.cassandraEventStoreSchemaFixture = cassandraEventStoreSchemaFixture;
        this.cassandraFixture = cassandraFixture;
    }

    [Fact]
    public async Task CreateStorageAsync()
    {
        await cassandraEventStoreSchemaFixture.Schema.CreateStorageAsync();
        var cluster = await cassandraFixture.GetClusterAsync();
        var tables = cluster.Metadata.GetTables("test_containers");

        Assert.Contains(cassandraEventStoreSchemaFixture.NamingStrategy.GetName(), tables);
        Assert.Contains("index_by_eventtype", tables);
        Assert.Contains("index_status", tables);
        Assert.Contains("message_counter", tables);
    }
}

public class CassandraEventStoreSchemaFixture
{
    public CassandraEventStoreSchemaFixture(CassandraFixture cassandraFixture)
    {
        NamingStrategy = new NoTableNamingStrategy();
        Schema = new CassandraEventStoreSchema(cassandraFixture, NamingStrategy);
    }

    public CassandraEventStoreSchema Schema { get; }
    public ITableNamingStrategy NamingStrategy { get; }
}
