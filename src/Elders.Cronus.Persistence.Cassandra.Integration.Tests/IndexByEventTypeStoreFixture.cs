namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class IndexByEventTypeStoreFixture
{
    public IndexByEventTypeStoreFixture(CassandraFixture cassandraFixture, NullLoggerFixture nullLoggerFixture)
    {
        Index = new IndexByEventTypeStore(cassandraFixture, nullLoggerFixture.CreateLogger<IndexByEventTypeStore>());
    }

    public IndexByEventTypeStore Index { get; }
}
