using System.Security.Cryptography;
using Cassandra;
using Elders.Cronus.EventStore.Index;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[EnsureEventStore]
public class IndexByEventTypeStoreTests :
    IClassFixture<IndexByEventTypeStoreFixture>,
    IClassFixture<CassandraEventStoreSchemaFixture>
{
    private readonly IIndexStore index;
    private readonly CassandraFixture cassandraFixture;
    private readonly CassandraEventStoreSchemaFixture schemaFixture;

    public IndexByEventTypeStoreTests(
        IndexByEventTypeStoreFixture indexByEventTypeStoreFixture,
        CassandraFixture cassandraFixture,
        CassandraEventStoreSchemaFixture schemaFixture)
    {
        index = indexByEventTypeStoreFixture.Index;
        this.cassandraFixture = cassandraFixture;
        this.schemaFixture = schemaFixture;
    }

    [Fact]
    public async Task ApendAsync()
    {
        var indexRecordFixture = new IndexRecordFixture();
        var record = indexRecordFixture.IndexRecord;
        await index.ApendAsync(record);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement("SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=? AND pid=?;", record.Id, indexRecordFixture.PartitionId);
        var rows = await session.ExecuteAsync(statement);

        var row = Assert.Single(rows);
        Assert.Equal(record.AggregateRootId.Span, row.GetValue<byte[]>("aid"));
        Assert.Equal(record.Revision, row.GetValue<int>("rev"));
        Assert.Equal(record.Position, row.GetValue<int>("pos"));
        Assert.Equal(record.TimeStamp, row.GetValue<long>("ts"));
    }

    //[Fact]
    //public void GetAsync() // This dowsn't work because of the partition id
    //{
    //    var record = indexRecordFixture.IndexRecord;
    //    var row = Assert.Single(index.GetAsync(record.Id));
    //    Assert.Equal(record.AggregateRootId, row.AggregateRootId);
    //    Assert.Equal(record.Revision, row.Revision);
    //    Assert.Equal(record.Position, row.Position);
    //    Assert.Equal(record.TimeStamp, row.TimeStamp);
    //}

    //[Fact]
    //public async Task GetCountAsync() // This dowsn't work because of the partition id
    //{
    //    var count = await index.GetCountAsync(indexRecordFixture.IndexRecord.Id);
    //    Assert.Equal(1, count);
    //}

    [Fact]
    public async Task DeleteAsync()
    {
        var indexRecordFixture = new IndexRecordFixture();
        await index.ApendAsync(indexRecordFixture.IndexRecord);
        Exception exception = null;
        try
        {
            await index.DeleteAsync(indexRecordFixture.IndexRecord);
        }
        catch (Exception ex)
        {
            exception = ex;
        }

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement("SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=? AND pid=?;", indexRecordFixture.IndexRecord.Id, indexRecordFixture.PartitionId);
        var rows = await session.ExecuteAsync(statement);

        Assert.Null(exception);
        Assert.Empty(rows);
    }
}

public class IndexRecordFixture
{
    public IndexRecordFixture()
    {
        var id = Guid.NewGuid().ToString();
        Memory<byte> arId = new byte[64];
        RandomNumberGenerator.Fill(arId.Span);
        var revision = 1;
        var position = 0;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();

        IndexRecord = new IndexRecord(id, arId, revision, position, timestamp);
        PartitionId = IndexByEventTypeStore.CalculatePartition(timestamp);
    }

    public IndexRecord IndexRecord { get; }
    public int PartitionId { get; }
}
