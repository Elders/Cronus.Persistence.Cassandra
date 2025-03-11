using System.Security.Cryptography;
using System.Text;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging.Abstractions;
using Newtonsoft.Json;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[EnsureEventStore]
public class EnumerateEventStore : IClassFixture<CassandraEventStoreFixture>
{
    private readonly CassandraEventStoreFixture cassandraEventStoreFixture;
    private readonly BlobIdFixture blobIdFixture;

    public EnumerateEventStore(CassandraEventStoreFixture cassandraEventStoreFixture)
    {
        this.cassandraEventStoreFixture = cassandraEventStoreFixture;
        blobIdFixture = new BlobIdFixture();
    }

    [Fact]
    public async Task EnumerateEventStore_AllEvents()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);

        var rawEvents = new List<AggregateEventRaw>();
        AggregateStream aggregateStream = null;
        var finished = false;
        var progressNotifications = 0;
        await cassandraEventStoreFixture.EventStore.EnumerateEventStore(new PlayerOperator
        {
            OnLoadAsync = er =>
            {
                rawEvents.Add(er);
                return Task.CompletedTask;
            },
            OnAggregateStreamLoadedAsync = stream =>
            {
                aggregateStream = stream;
                return Task.CompletedTask;
            },
            OnFinish = () =>
            {
                finished = true;
                return Task.CompletedTask;
            },
            NotifyProgressAsync = po =>
            {
                progressNotifications++;
                return Task.CompletedTask;
            }
        }, new PlayerOptions(), TestContext.Current.CancellationToken);

        Assert.True(finished);
        Assert.Equal(1, progressNotifications);

        var rawEvent = Assert.Single(rawEvents);
        Assert.Equal(arId, rawEvent.AggregateRootId);
        Assert.Equal(revision, rawEvent.Revision);
        Assert.Equal(0, rawEvent.Position);
        Assert.Equal(timestamp, rawEvent.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), rawEvent.Data);

        Assert.NotNull(aggregateStream);
        var rawCommit = Assert.Single(aggregateStream.Commits);
        Assert.Equal(timestamp, rawCommit.Timestamp.ToFileTime());
        var rawEventFromStream = Assert.Single(rawCommit.Events);
        Assert.Equal(arId, rawEventFromStream.AggregateRootId);
        Assert.Equal(revision, rawEventFromStream.Revision);
        Assert.Equal(0, rawEventFromStream.Position);
        Assert.Equal(timestamp, rawEventFromStream.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), rawEventFromStream.Data);
    }
}

[EnsureEventStore]
public class CassandraEventStoreTests : IClassFixture<CassandraEventStoreFixture>
{
    private readonly CassandraFixture cassandraFixture;
    private readonly BlobIdFixture blobIdFixture;
    private readonly CassandraEventStoreFixture cassandraEventStoreFixture;

    public CassandraEventStoreTests(
        CassandraFixture cassandraFixture,
        CassandraEventStoreFixture cassandraEventStoreFixture)
    {
        this.cassandraFixture = cassandraFixture;
        this.cassandraEventStoreFixture = cassandraEventStoreFixture;
        blobIdFixture = new BlobIdFixture();
    }

    [Fact]
    public async Task AppendAggregateCommitAsync()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var eventData = cassandraEventStoreFixture.Serializer.SerializeToBytes(@event);
        var publicEvent = new PublicTestEvent(DateTimeOffset.UtcNow);
        var publicEventData = cassandraEventStoreFixture.Serializer.SerializeToBytes(publicEvent);
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [publicEvent], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement($"SELECT rev,pos,ts,data FROM {cassandraEventStoreFixture.TableNaming.GetName()} WHERE id = ?;", arId);
        var rows = await session.ExecuteAsync(statement);
        var rowsList = rows.ToList();

        Assert.NotEmpty(rowsList);
        Assert.Collection(rowsList,
            r =>
            {
                Assert.Equal(revision, r.GetValue<int>("rev"));
                Assert.Equal(0, r.GetValue<int>("pos"));
                Assert.Equal(timestamp, r.GetValue<long>("ts"));
                Assert.Equal(eventData, r.GetValue<byte[]>("data"));
            },
            r =>
            {
                Assert.Equal(revision, r.GetValue<int>("rev"));
                Assert.Equal(5, r.GetValue<int>("pos"));
                Assert.Equal(timestamp, r.GetValue<long>("ts"));
                Assert.Equal(publicEventData, r.GetValue<byte[]>("data"));
            });
    }

    [Fact]
    public async Task AppendAggregateEventRawAsync()
    {
        var arId = blobIdFixture.New();
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var eventData = cassandraEventStoreFixture.Serializer.SerializeToBytes(@event);
        var revision = 2;
        var position = 0;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var eventRaw = new AggregateEventRaw(arId, eventData, revision, position, timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(eventRaw);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement($"SELECT rev,pos,ts,data FROM {cassandraEventStoreFixture.TableNaming.GetName()} WHERE id = ?;", arId);
        var rows = await session.ExecuteAsync(statement);

        var row = Assert.Single(rows);
        Assert.Equal(revision, row.GetValue<int>("rev"));
        Assert.Equal(position, row.GetValue<int>("pos"));
        Assert.Equal(timestamp, row.GetValue<long>("ts"));
        Assert.Equal(eventData, row.GetValue<byte[]>("data"));
    }

    [Fact]
    public async Task LoadAsync()
    {
        var id = new TestId(blobIdFixture.New());

        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var publicEvent = new PublicTestEvent(DateTimeOffset.UtcNow);
        var timestamp = DateTime.UtcNow;
        var commit1 = new AggregateCommit(id.RawId, revision, [@event], [publicEvent], timestamp.ToFileTimeUtc());

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit1);

        revision++;
        @event = new TestEvent(@event.Timestamp.AddSeconds(1));
        timestamp = timestamp.AddSeconds(1);
        var commit2 = new AggregateCommit(id.RawId, revision, [@event], [], timestamp.ToFileTimeUtc());

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit2);

        var stream = await cassandraEventStoreFixture.EventStore.LoadAsync(id);

        Assert.NotNull(stream);
        Assert.Equal(2, stream.Commits.Count());

        var first = stream.Commits.ElementAt(0);
        Assert.Equal(1, first.Revision);
        Assert.Equal(id.RawId, first.AggregateRootId);
        Assert.Single(first.Events);
        Assert.Single(first.PublicEvents);

        var second = stream.Commits.ElementAt(1);
        Assert.Equal(2, second.Revision);
        Assert.Equal(id.RawId, second.AggregateRootId);
        Assert.Single(second.Events);
        Assert.Empty(second.PublicEvents);
    }

    [Fact]
    public async Task LoadAggregateEventRaw()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var publicEvent = new PublicTestEvent(DateTimeOffset.UtcNow);
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [publicEvent], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);
        var indexRecord = new IndexRecord("dosn't matter", arId, revision, 0, timestamp);

        var eventRaw = await cassandraEventStoreFixture.EventStore.LoadAggregateEventRaw(indexRecord);

        Assert.NotNull(eventRaw);
        Assert.Equal(1, eventRaw.Revision);
        Assert.Equal(0, eventRaw.Position);
        Assert.Equal(timestamp, eventRaw.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), eventRaw.Data);
    }

    [Fact]
    public async Task LoadEventWithRebuildProjectionAsync()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var publicEvent = new PublicTestEvent(DateTimeOffset.UtcNow);
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [publicEvent], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);
        var indexRecord = new IndexRecord("dosn't matter", arId, revision, 0, timestamp);

        var loadedEvent = await cassandraEventStoreFixture.EventStore.LoadEventWithRebuildProjectionAsync(indexRecord);

        Assert.NotNull(loadedEvent);
        Assert.IsType<TestEvent>(loadedEvent);
        Assert.Equal(@event.Timestamp, loadedEvent.Timestamp);
    }

    [Fact]
    public async Task LoadWithPagingAsync()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);

        var result = await cassandraEventStoreFixture.EventStore.LoadWithPagingAsync(new TestId(arId), PagingOptions.Empty());

        Assert.NotNull(result);
        var eventRaw = Assert.Single(result.RawEvents);
        Assert.Equal(1, eventRaw.Revision);
        Assert.Equal(0, eventRaw.Position);
        Assert.Equal(timestamp, eventRaw.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), eventRaw.Data);
    }

    [Fact]
    public async Task EnumerateEventStoreForEventType()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var eventTypeId = @event.GetType().Name;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);
        var accessor = new CronusContextAccessorMock
        {
            CronusContext = new CronusContext("tests", new NullServiceProviderMock())
        };
        var index = new IndexByEventTypeStoreFixture(accessor, cassandraFixture).Index;
        await index.ApendAsync(new IndexRecord(eventTypeId, arId, revision, 0, timestamp));

        var rawEvents = new List<AggregateEventRaw>();
        AggregateStream aggregateStream = null;
        var finished = false;
        var progressNotifications = 0;
        await cassandraEventStoreFixture.EventStore.EnumerateEventStore(new PlayerOperator
        {
            OnLoadAsync = er =>
            {
                rawEvents.Add(er);
                return Task.CompletedTask;
            },
            OnAggregateStreamLoadedAsync = stream =>
            {
                aggregateStream = stream;
                return Task.CompletedTask;
            },
            OnFinish = () =>
            {
                finished = true;
                return Task.CompletedTask;
            },
            NotifyProgressAsync = po =>
            {
                progressNotifications++;
                return Task.CompletedTask;
            }
        }, new PlayerOptions { EventTypeId = eventTypeId }, TestContext.Current.CancellationToken);

        Assert.True(finished);
        Assert.Equal(1, progressNotifications);

        var rawEvent = Assert.Single(rawEvents);
        Assert.Equal(arId, rawEvent.AggregateRootId);
        Assert.Equal(revision, rawEvent.Revision);
        Assert.Equal(0, rawEvent.Position);
        Assert.Equal(timestamp, rawEvent.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), rawEvent.Data);

        Assert.NotNull(aggregateStream);
        var rawCommit = Assert.Single(aggregateStream.Commits);
        Assert.Equal(timestamp, rawCommit.Timestamp.ToFileTime());
        var rawEventFromStream = Assert.Single(rawCommit.Events);
        Assert.Equal(arId, rawEventFromStream.AggregateRootId);
        Assert.Equal(revision, rawEventFromStream.Revision);
        Assert.Equal(0, rawEventFromStream.Position);
        Assert.Equal(timestamp, rawEventFromStream.Timestamp);
        Assert.Equal(cassandraEventStoreFixture.Serializer.SerializeToBytes(@event), rawEventFromStream.Data);
    }

    [Fact]
    public async Task DeleteAsync()
    {
        var arId = blobIdFixture.New();
        var revision = 1;
        var @event = new TestEvent(DateTimeOffset.UtcNow);
        var eventTypeId = @event.GetType().Name;
        var timestamp = DateTime.UtcNow.ToFileTimeUtc();
        var commit = new AggregateCommit(arId, revision, [@event], [], timestamp);

        await cassandraEventStoreFixture.EventStore.AppendAsync(commit);

        var eventRaw = new AggregateEventRaw(commit.AggregateRootId, null, revision, 0, timestamp);
        var deleted = await cassandraEventStoreFixture.EventStore.DeleteAsync(eventRaw);

        var session = await cassandraFixture.GetSessionAsync();
        var statement = new SimpleStatement($"SELECT rev,pos,ts,data FROM {cassandraEventStoreFixture.TableNaming.GetName()} WHERE id = ?;", arId);
        var rows = await session.ExecuteAsync(statement);

        Assert.True(deleted);
        Assert.Empty(rows);
    }
}

class SerializerMock : ISerializer
{
    public SerializerMock()
    {
        settings = new JsonSerializerSettings
        {
            DateTimeZoneHandling = DateTimeZoneHandling.Utc,
            DateFormatHandling = DateFormatHandling.IsoDateFormat,
            TypeNameHandling = TypeNameHandling.Objects,
            TypeNameAssemblyFormatHandling = TypeNameAssemblyFormatHandling.Simple,
            Formatting = Formatting.None,
            ConstructorHandling = ConstructorHandling.AllowNonPublicDefaultConstructor
        };
    }

    private readonly JsonSerializerSettings settings;

    public T DeserializeFromBytes<T>(byte[] bytes)
    {
        var json = Encoding.UTF8.GetString(bytes);
        return JsonConvert.DeserializeObject<T>(json, settings);
    }

    public byte[] SerializeToBytes<T>(T message)
    {
        var json = JsonConvert.SerializeObject(message, settings);
        return Encoding.UTF8.GetBytes(json);
    }

    public string SerializeToString<T>(T message)
    {
        return JsonConvert.SerializeObject(message, settings);
    }
}

sealed class TestEvent : IEvent
{
    public TestEvent(DateTimeOffset timestamp)
    {
        Timestamp = timestamp;
    }

    public DateTimeOffset Timestamp { get; }
}

sealed class PublicTestEvent : IPublicEvent
{
    public PublicTestEvent(DateTimeOffset timestamp)
    {
        Timestamp = timestamp;
    }

    public string Tenant => "tests";

    public DateTimeOffset Timestamp { get; }
}

sealed class TestId : IBlobId
{
    public TestId(ReadOnlyMemory<byte> rawId)
    {
        RawId = rawId;
    }

    public ReadOnlyMemory<byte> RawId { get; }
}

public class BlobIdFixture
{
    public BlobIdFixture()
    {
        RawId = New();
    }

    public ReadOnlyMemory<byte> RawId { get; }

    public ReadOnlyMemory<byte> New()
    {
        Memory<byte> memory = new byte[64];
        RandomNumberGenerator.Fill(memory.Span);
        return memory;
    }
}

public class CassandraEventStoreFixture
{
    public CassandraEventStoreFixture(CassandraFixture cassandraFixture)
    {
        TableNaming = new NoTableNamingStrategy();
        Serializer = new SerializerMock();
        var accessor = new CronusContextAccessorMock
        {
            CronusContext = new CronusContext("tests", new NullServiceProviderMock())
        };
        var index = new IndexByEventTypeStore(accessor, cassandraFixture, NullLogger<IndexByEventTypeStore>.Instance);
        EventStore = new CassandraEventStore(accessor, cassandraFixture, TableNaming, Serializer, index, NullLogger<CassandraEventStore>.Instance);
    }

    public CassandraEventStore EventStore { get; }

    public ISerializer Serializer { get; }

    public ITableNamingStrategy TableNaming { get; }
}
