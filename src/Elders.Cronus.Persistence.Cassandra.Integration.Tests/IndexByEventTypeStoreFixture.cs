using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class IndexByEventTypeStoreFixture
{
    public IndexByEventTypeStoreFixture(ICronusContextAccessor cronusContextAccessor, CassandraFixture cassandraFixture)
    {
        Index = new IndexByEventTypeStore(cronusContextAccessor, cassandraFixture, NullLogger<IndexByEventTypeStore>.Instance);
    }

    public IndexByEventTypeStore Index { get; }
}
