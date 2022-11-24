using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Testing;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class CassandraEventStore<TSettings> : CassandraEventStore, IEventStorePlayer<TSettings>
        where TSettings : class, ICassandraEventStoreSettings
    {
        public CassandraEventStore(TSettings settings, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger)
            : base(settings.CassandraProvider, settings.TableNameStrategy, settings.Serializer, indexByEventTypeStore)
        {
        }
    }

    public class CassandraEventStore : IEventStore, IEventStorePlayer
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStore));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT rev,pos,ts,data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,rev,pos,ts,data) VALUES (?,?,?,?,?);";
        private const string LoadAggregateCommitsQueryTemplate = @"SELECT id,rev,pos,ts,data FROM {0};";

        private const string LoadAggregateEventsRebuildQueryTemplate = @"SELECT data FROM {0} WHERE id = ? AND rev = ? AND pos = ?;";
        private const string LoadEventQueryTemplate = @"SELECT data,ts FROM {0} WHERE id = ? AND rev = ? AND pos = ?;";

        private readonly ISerializer serializer;
        private readonly IndexByEventTypeStore indexByEventTypeStore;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;
        private PreparedStatement replayStatement;
        private PreparedStatement replayWithoutDataStatement;
        private PreparedStatement loadAggregateCommitsMetaStatement;

        public CassandraEventStore(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer, IndexByEventTypeStore indexByEventTypeStore)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            this.indexByEventTypeStore = indexByEventTypeStore ?? throw new ArgumentNullException(nameof(indexByEventTypeStore));
        }

        public async Task AppendAsync(AggregateCommit aggregateCommit)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement writeStatement = await GetWriteStatementAsync(session).ConfigureAwait(false);

            var pos = -1;
            foreach (var @event in aggregateCommit.Events)
            {
                byte[] data = SerializeEvent(@event);

                try
                {
                    BoundStatement boundStatement = writeStatement.Bind(aggregateCommit.AggregateRootId, aggregateCommit.Revision, ++pos, aggregateCommit.Timestamp, data);

                    await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
                }
                catch (WriteTimeoutException ex)
                {
                    logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
                }
            }

            pos += AggregateCommitBlock.PublicEventsOffset;

            foreach (var @event in aggregateCommit.PublicEvents)
            {
                byte[] data = SerializeEvent(@event);

                try
                {
                    BoundStatement boundStatement = writeStatement.Bind(aggregateCommit.AggregateRootId, aggregateCommit.Revision, pos++, aggregateCommit.Timestamp, data);
                    await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
                }
                catch (WriteTimeoutException ex)
                {
                    logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
                }
            }
        }

        public async Task AppendAsync(AggregateCommitRaw aggregateCommitRaw)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetWriteStatementAsync(session).ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Revision, aggregateCommitRaw.Position, aggregateCommitRaw.Data);

                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit");
            }
        }

        public async Task<EventStream> LoadAsync(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = await LoadAggregateCommitsAsync(aggregateId).ConfigureAwait(false);

            return new EventStream(aggregateCommits);
        }

        private async Task<List<AggregateCommit>> LoadAggregateCommitsAsync(IBlobId id)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement bs = await GetReadStatementAsync(session).ConfigureAwait(false);
            BoundStatement boundStatement = bs.Bind(id.RawId);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);

            var block = new AggregateCommitBlock(id);
            foreach (var row in result.GetRows())
            {
                int revision = row.GetValue<int>("rev");
                int position = row.GetValue<int>("pos");
                long timestamp = row.GetValue<long>("ts");
                byte[] data = row.GetValue<byte[]>("data");

                using (var stream = new MemoryStream(data))
                {
                    IMessage messageData = (IMessage)serializer.Deserialize(stream);
                    block.AppendBlock(revision, position, messageData, timestamp);
                }
            }

            return block.Complete();
        }

        public async Task<LoadAggregateCommitsResult> LoadAggregateCommitsAsync(ReplayOptions replayOptions)
        {
            throw new NotImplementedException();
        }

        public async Task<LoadAggregateCommitsResult> LoadAggregateCommitsAsync(string paginationToken, int pageSize = 5000)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadAggregateCommitsResult() { PaginationToken = paginationToken };

            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            PreparedStatement statement = await GetReplayStatementAsync().ConfigureAwait(false);
            IStatement queryStatement = statement.Bind().SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);

            IBlobId firstElementId = null;
            AggregateCommitBlock block = null;
            AggregateCommitBlock.CassandraRawId currentId = null;
            foreach (Row row in result.GetRows())
            {
                byte[] loadedId = row.GetValue<byte[]>("id");

                if (currentId is null)
                {
                    currentId = new AggregateCommitBlock.CassandraRawId(loadedId);
                    block = new AggregateCommitBlock(currentId);
                }

                if (ByteArrayHelper.Compare(currentId.RawId, loadedId) == false)
                {
                    aggregateCommits.AddRange(block.Complete());

                    currentId = new AggregateCommitBlock.CassandraRawId(loadedId);
                    block = new AggregateCommitBlock(currentId);
                }

                int revision = row.GetValue<int>("rev");
                int position = row.GetValue<int>("pos");
                long timestamp = row.GetValue<long>("ts");
                byte[] data = row.GetValue<byte[]>("data");

                if (firstElementId != null && ByteArrayHelper.Compare(firstElementId.RawId, loadedId))
                {
                    continue;
                }

                if (firstElementId is null && revision > 1 && position > 0)
                {
                    firstElementId = currentId;
                    var firstIdAggregates = await LoadAggregateCommitsAsync(firstElementId).ConfigureAwait(false);
                    aggregateCommits.AddRange(firstIdAggregates);
                    continue;
                }

                using (var stream = new MemoryStream(data))
                {
                    try
                    {
                        var @event = (IMessage)serializer.Deserialize(stream);
                        try
                        {
                            block.AppendBlock(revision, position, @event, timestamp);
                        }
                        catch (Exception)
                        {
                        }

                    }
                    catch (Exception ex)
                    {
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }
                }
            }

            var finalAggregateCommits = await LoadAggregateCommitsAsync(currentId).ConfigureAwait(false);

            if (result.IsFullyFetched == false)
            {
                logger.Warn(() => "Not implemented logic. => if (result.IsFullyFetched == false)");
            }

            aggregateCommits.AddRange(finalAggregateCommits);

            return new LoadAggregateCommitsResult()
            {
                Commits = aggregateCommits,
                PaginationToken = PagingInfo.From(result).ToString()
            };
        }

        public async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync(int batchSize)
        {
            PreparedStatement statement = await GetReplayStatementAsync().ConfigureAwait(false);
            IStatement queryStatement = statement.Bind().SetPageSize(batchSize);
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);

            AggregateCommitBlock block = null;
            AggregateCommitBlock.CassandraRawId currentId = null;
            foreach (var row in result.GetRows())
            {
                byte[] loadedId = row.GetValue<byte[]>("id");

                if (currentId is null)
                {
                    currentId = new AggregateCommitBlock.CassandraRawId(loadedId);
                    block = new AggregateCommitBlock(currentId);
                }
                else if (ByteArrayHelper.Compare(currentId.RawId, loadedId) == false)
                {
                    foreach (var arCommit in block.Complete())
                    {
                        yield return arCommit;
                    }

                    currentId = new AggregateCommitBlock.CassandraRawId(loadedId);
                    block = new AggregateCommitBlock(currentId);
                }

                int revision = row.GetValue<int>("rev");
                int position = row.GetValue<int>("pos");
                long timestamp = row.GetValue<long>("ts");
                byte[] data = row.GetValue<byte[]>("data");

                using (MemoryStream stream = new MemoryStream(data))
                {
                    try
                    {
                        IMessage eventMessage = (IMessage)serializer.Deserialize(stream);
                        block.AppendBlock(revision, position, eventMessage, timestamp);
                    }
                    catch (Exception ex)
                    {
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }
                }
            }

            foreach (var arCommit in block.Complete())
            {
                yield return arCommit;
            }
        }

        public async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync()
        {
            var statement = await GetReplayStatementAsync().ConfigureAwait(false);
            var queryStatement = statement.Bind();
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            AggregateCommitBlock block = null;

            foreach (var row in result.GetRows())
            {
                var id = new AggregateCommitBlock.CassandraRawId(row.GetValue<byte[]>("id"));
                var revision = row.GetValue<int>("rev");
                var position = row.GetValue<int>("pos");
                long timestamp = row.GetValue<long>("ts");
                var data = row.GetValue<byte[]>("data");

                if (block is null)
                    block = new AggregateCommitBlock(id);

                AggregateCommit commit = null;
                using (var stream = new MemoryStream(data))
                {
                    bool isBlockCompleted = false;

                    // TODO: What if we have missing blocks?
                    try
                    {
                        var @event = (IMessage)serializer.Deserialize(stream);
                        block.AppendBlock(revision, position, @event, timestamp);
                        if (isBlockCompleted)
                        {
                            block = null;
                        }
                    }
                    catch (Exception ex)
                    {
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }

                    if (commit is not null)
                        yield return commit;
                }
            }
        }

        public async IAsyncEnumerable<AggregateCommitRaw> LoadAggregateCommitsRawAsync(int batchSize = 5000)
        {
            var statement = await GetReplayStatementAsync().ConfigureAwait(false);
            var queryStatement = statement.Bind().SetPageSize(batchSize);
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                byte[] id = row.GetValue<byte[]>("id");
                byte[] data = row.GetValue<byte[]>("data");
                var position = row.GetValue<int>("pos");
                int revision = row.GetValue<int>("rev");
                long timestamp = row.GetValue<long>("ts");

                using (var stream = new MemoryStream(data))
                {
                    AggregateCommitRaw commitRaw = new AggregateCommitRaw(id, data, revision, position, timestamp);

                    yield return commitRaw;
                }
            }
        }

        public async IAsyncEnumerable<AggregateCommitRaw> LoadAggregateCommitsRawAsync()
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var queryStatement = (await GetReplayStatementAsync().ConfigureAwait(false)).Bind();
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                byte[] id = row.GetValue<byte[]>("id");
                byte[] data = row.GetValue<byte[]>("data");
                var position = row.GetValue<int>("pos");
                int revision = row.GetValue<int>("rev");
                long timestamp = row.GetValue<long>("ts");

                using (var stream = new MemoryStream(data))
                {
                    AggregateCommitRaw commitRaw = new AggregateCommitRaw(id, data, revision, position, timestamp);

                    yield return commitRaw;
                }
            }
        }

        private async Task<PreparedStatement> PrepareLoadEventQueryStatementAsync(ISession session)
        {
            if (loadAggregateCommitsMetaStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                loadAggregateCommitsMetaStatement = await session.PrepareAsync(string.Format(LoadEventQueryTemplate, tableName)).ConfigureAwait(false);
                loadAggregateCommitsMetaStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return loadAggregateCommitsMetaStatement;
        }

        public async Task<IEvent> LoadEventWithRebuildProjectionAsync(IndexRecord indexRecord)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetRebuildDataStatementAsync(session).ConfigureAwait(false);

            BoundStatement boundStatement = statement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            var row = result.GetRows().Single();
            byte[] data = row.GetValue<byte[]>("data");

            using (var stream = new MemoryStream(data))
            {
                return (IEvent)serializer.Deserialize(stream);
            }
        }

        private PagingInfo GetPagingInfo(string paginationToken)
        {
            PagingInfo pagingInfo = new PagingInfo();
            if (string.IsNullOrEmpty(paginationToken) == false)
            {
                string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
                pagingInfo = JsonSerializer.Deserialize<PagingInfo>(paginationJson);
            }
            return pagingInfo;
        }

        private byte[] SerializeEvent(IEvent @event)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, @event);
                return stream.ToArray();
            }
        }

        private byte[] SerializeEvent(IPublicEvent @event)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, @event);
                return stream.ToArray();
            }
        }

        private async Task<PreparedStatement> GetWriteStatementAsync(ISession session)
        {
            if (writeStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                writeStatement = await session.PrepareAsync(string.Format(InsertEventsQueryTemplate, tableName)).ConfigureAwait(false);
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private async Task<PreparedStatement> GetReadStatementAsync(ISession session)
        {
            if (readStatement is null)
            {

                string tableName = tableNameStrategy.GetName();
                readStatement = await session.PrepareAsync(string.Format(LoadAggregateEventsQueryTemplate, tableName)).ConfigureAwait(false);
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }

        private async Task<PreparedStatement> GetReplayStatementAsync()
        {
            if (replayStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                string tableName = tableNameStrategy.GetName();
                replayStatement = await session.PrepareAsync(string.Format(LoadAggregateCommitsQueryTemplate, tableName)).ConfigureAwait(false);
                replayStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayStatement;
        }

        private async Task<PreparedStatement> GetRebuildDataStatementAsync(ISession session)
        {
            if (replayWithoutDataStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayWithoutDataStatement = await session.PrepareAsync(string.Format(LoadAggregateEventsRebuildQueryTemplate, tableName)).ConfigureAwait(false);
                replayWithoutDataStatement.SetConsistencyLevel(ConsistencyLevel.LocalOne);
            }

            return replayWithoutDataStatement;
        }

        public async IAsyncEnumerable<IPublicEvent> LoadPublicEventsAsync(ReplayOptions replayOptions, Action<ReplayOptions> notifyProgress = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement queryStatement = await PrepareLoadEventQueryStatementAsync(session).ConfigureAwait(false);

            long after = replayOptions.After.Value.ToFileTime();
            long before = replayOptions.Before.Value.ToFileTime();

            // var indexRecords = indexByEventTypeStore.GetRecordsAsync(replayOptions.EventTypeId, after, before, replayOptions.PaginationToken, replayOptions.BatchSize, null, cancellationToken).ConfigureAwait(false);

            var indexRecords = indexByEventTypeStore.GetRecordsAsync(replayOptions.EventTypeId, after, before, replayOptions.PaginationToken, replayOptions.BatchSize, (pagingInfo) =>
            {
                if (notifyProgress is not null)
                {
                    var gg = replayOptions.WithPaginationToken(pagingInfo.ToString());
                    notifyProgress(gg);
                }
            }, cancellationToken).ConfigureAwait(false);

            await foreach (IndexRecord indexRecord in indexRecords)
            {
                counters++;
                BoundStatement query = queryStatement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);
                RowSet rowSet = await session.ExecuteAsync(query).ConfigureAwait(false);
                Row row = rowSet.SingleOrDefault();
                if (row is not null)
                {
                    byte[] data = row.GetValue<byte[]>("data");
                    using (var stream = new MemoryStream(data))
                    {
                        if (serializer.Deserialize(stream) is IPublicEvent publicEvent)
                            yield return publicEvent;
                    }
                }
            }
        }
        static int counters = 1;
    }

    internal class AggregateCommitBlock
    {
        private readonly IBlobId id;
        private int revision;
        private long timestamp;
        public const int PublicEventsOffset = 5;

        public AggregateCommitBlock(IBlobId id)
        {
            this.id = id;
            revision = 1;
            Events = new List<IEvent>();
            PublicEvents = new List<IPublicEvent>();
            aggregateCommits = new List<AggregateCommit>();
        }

        private int GetNextExpectedEventPosition() => Events.Count;

        private int GetNextExpectedPublicEventPosition() => Events.Count + PublicEventsOffset + PublicEvents.Count;

        private List<IEvent> Events { get; set; }

        private List<IPublicEvent> PublicEvents { get; set; }

        List<AggregateCommit> aggregateCommits;

        internal void AppendBlock(int revision, int position, IMessage data, long timestamp)
        {
            if (this.timestamp == 0)
                this.timestamp = timestamp;

            if (this.revision == revision)
            {
                AttachDataAtPosition(data, position);
            }
            else if (this.revision < revision)
            {
                var aggregateCommit = new AggregateCommit(id.RawId, this.revision, Events.ToList(), PublicEvents.ToList(), this.timestamp);
                aggregateCommits.Add(aggregateCommit);

                Events.Clear();
                PublicEvents.Clear();

                this.revision = revision;
                this.timestamp = timestamp;
                AttachDataAtPosition(data, position);
            }
        }

        private void AttachDataAtPosition(IMessage data, int position)
        {
            if (GetNextExpectedEventPosition() == position) // If the event we want to attach is IEvent (not public) and it is the first one 
                Events.Add((IEvent)data);
            else if (GetNextExpectedPublicEventPosition() >= position)
                PublicEvents.Add((IPublicEvent)data);
            else
                throw new NotSupportedException("How?!?!?");
        }

        public List<AggregateCommit> Complete()
        {
            if (Events.Any())
            {
                // Appends the everything we have in memory to the final result
                var aggregateCommit = new AggregateCommit(id.RawId, this.revision, Events.ToList(), PublicEvents.ToList(), this.timestamp);
                aggregateCommits.Add(aggregateCommit);
            }

            return aggregateCommits;
        }


        internal class CassandraRawId : IBlobId
        {
            public CassandraRawId(byte[] rawId)
            {
                RawId = rawId;
            }

            public byte[] RawId { get; private set; }
        }
    }

}
