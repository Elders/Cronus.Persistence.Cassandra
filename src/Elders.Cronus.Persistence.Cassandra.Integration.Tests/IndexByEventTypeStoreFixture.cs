using Microsoft.Extensions.Logging.Abstractions;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class IndexByEventTypeStoreFixture
{
    public IndexByEventTypeStoreFixture(CassandraFixture cassandraFixture)
    {
        Index = new IndexByEventTypeStore(cassandraFixture, NullLogger<IndexByEventTypeStore>.Instance);
    }

    public IndexByEventTypeStore Index { get; }
}
