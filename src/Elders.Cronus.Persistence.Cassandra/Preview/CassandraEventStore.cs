using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class CassandraEventStore<TSettings> : CassandraEventStore, IEventStorePlayer<TSettings>
        where TSettings : class, ICassandraEventStoreSettings
    {
        public CassandraEventStore(TSettings settings, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger)
            : base(settings.CassandraProvider, settings.TableNameStrategy, settings.Serializer, indexByEventTypeStore, logger)
        {
        }
    }

    public class CassandraEventStore : IEventStore, IEventStorePlayer
    {
        private const string LoadAggregateEventsQueryTemplate = @"SELECT rev,pos,ts,data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,rev,pos,ts,data) VALUES (?,?,?,?,?);";
        private const string LoadEventsQueryTemplate = @"SELECT id,rev,pos,ts,data FROM {0};";
        private const string LoadAggregateEventsWithinSpecifiedRevisionsQueryTemplate = @"SELECT rev,pos,ts,data FROM {0} WHERE id = ? order by rev desc, pos desc";

        private const string LoadAggregateEventsRebuildQueryTemplate = @"SELECT data FROM {0} WHERE id = ? AND rev = ? AND pos = ?;";
        private const string LoadEventQueryTemplate = @"SELECT data,ts FROM {0} WHERE id = ? AND rev = ? AND pos = ?;";

        public const string DeleteEventQueryTemplate = @"DELETE FROM {0} WHERE id = ? and rev=? and pos=?;";

        private readonly ISerializer serializer;
        private readonly IndexByEventTypeStore indexByEventTypeStore;
        private readonly ILogger<CassandraEventStore> logger;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;
        private PreparedStatement replayStatement;
        private PreparedStatement replayWithoutDataStatement;
        private PreparedStatement loadAggregateCommitsMetaStatement;
        private PreparedStatement deleteStatement;
        private PreparedStatement readWithPagingByRevisionStatement;

        public CassandraEventStore(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            this.indexByEventTypeStore = indexByEventTypeStore ?? throw new ArgumentNullException(nameof(indexByEventTypeStore));
            this.logger = logger;
        }

        public async Task AppendAsync(AggregateCommit aggregateCommit)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement writeStatement = await GetWriteStatementAsync(session).ConfigureAwait(false);
                BatchStatement batch = new BatchStatement();
                batch.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
                batch.SetIdempotence(false);
                batch.SetBatchType(BatchType.Unlogged);

                var pos = -1;
                for (int idx = 0; idx < aggregateCommit.Events.Count; idx++)
                {
                    byte[] data = serializer.SerializeToBytes(aggregateCommit.Events[idx]);
                    BoundStatement boundStatement = writeStatement.Bind(aggregateCommit.AggregateRootId, aggregateCommit.Revision, ++pos, aggregateCommit.Timestamp, data);
                    batch.Add(boundStatement);
                }

                pos += AggregateCommitBlock.PublicEventsOffset;
                for (int idx = 0; idx < aggregateCommit.PublicEvents.Count; idx++)
                {
                    byte[] data = serializer.SerializeToBytes(aggregateCommit.PublicEvents[idx]);
                    BoundStatement boundStatement = writeStatement.Bind(aggregateCommit.AggregateRootId, aggregateCommit.Revision, pos++, aggregateCommit.Timestamp, data);
                    batch.Add(boundStatement);
                }

                await session.ExecuteAsync(batch).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
            }
        }

        public async Task AppendAsync(AggregateEventRaw aggregateCommitRaw)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetWriteStatementAsync(session).ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Revision, aggregateCommitRaw.Position, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Data);

                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
            }
        }

        public async Task<EventStream> LoadAsync(IBlobId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = await LoadAggregateCommitsAsync(aggregateId).ConfigureAwait(false);

            return new EventStream(aggregateCommits);
        }

        public async Task<LoadAggregateRawEventsWithPagingResult> LoadWithPagingDescendingAsync(IBlobId aggregateId, PagingOptions pagingOptions)
        {
            LoadAggregateRawEventsWithPagingResult result = await LoadAggregateRawEventsWithPagingAsync(aggregateId, pagingOptions).ConfigureAwait(false);

            return result;
        }

        public async Task<bool> DeleteAsync(AggregateEventRaw eventRaw)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetDeleteStatement(session).ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(eventRaw.AggregateRootId, eventRaw.Revision, eventRaw.Position);

                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);

                return true;
            }
            catch (WriteTimeoutException ex) when (logger.WarnException(ex, () => "Failed to delete event."))
            {
                return false;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to delete event.")) { }
            {
                return false;
            }
        }

        public async Task EnumerateEventStore(PlayerOperator @operator, PlayerOptions replayOptions)
        {
            if (replayOptions.EventTypeId is null)
            {
                await EnumerateEventStoreGG(@operator, replayOptions).ConfigureAwait(false);
            }
            else
            {
                await EnumerateEventStoreForSpecifiedEvent(@operator, replayOptions).ConfigureAwait(false);
            }
        }

        public async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync()
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var statement = await GetReplayStatementAsync(session).ConfigureAwait(false);
            var queryStatement = statement.Bind();
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            AggregateCommitBlock block = null;

            foreach (var row in result.GetRows())
            {
                var id = new AggregateCommitBlock.CassandraRawId(row.GetValue<byte[]>(CassandraColumn.Id));
                var revision = row.GetValue<int>(CassandraColumn.Revision);
                var position = row.GetValue<int>(CassandraColumn.Position);
                long timestamp = row.GetValue<long>(CassandraColumn.Timestamp);
                var data = row.GetValue<byte[]>(CassandraColumn.Data);

                if (block is null)
                    block = new AggregateCommitBlock(id);

                AggregateCommit commit = null;

                bool isBlockCompleted = false;

                // TODO: What if we have missing blocks?
                try
                {
                    var @event = serializer.DeserializeFromBytes<IMessage>(data);
                    block.AppendBlock(revision, position, @event, timestamp);
                    if (isBlockCompleted)
                        block = null;
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

        public async Task<IEvent> LoadEventWithRebuildProjectionAsync(IndexRecord indexRecord)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetRebuildDataStatementAsync(session).ConfigureAwait(false);

            BoundStatement boundStatement = statement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            var row = result.GetRows().Single();
            byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

            return serializer.DeserializeFromBytes<IEvent>(data);
        }

        public async Task<AggregateEventRaw> LoadAggregateEventRaw(IndexRecord indexRecord)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement queryStatement = await PrepareLoadEventQueryStatementAsync(session).ConfigureAwait(false);
            BoundStatement query = queryStatement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);
            RowSet rowSet = await session.ExecuteAsync(query).ConfigureAwait(false);
            Row row = rowSet.SingleOrDefault();
            if (row is not null)
            {
                byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);
                return new AggregateEventRaw(indexRecord.AggregateRootId, data, indexRecord.Revision, indexRecord.Position, indexRecord.TimeStamp);
            }

            logger.Error(() => $"Unable to load aggregate event by index record: {indexRecord.ToJson()}");

            return default;
        }

        public IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync(int batchSize = 5000)
        {
            throw new NotImplementedException();
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
                int revision = row.GetValue<int>(CassandraColumn.Revision);
                int position = row.GetValue<int>(CassandraColumn.Position);
                long timestamp = row.GetValue<long>(CassandraColumn.Timestamp);
                byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

                IMessage messageData = serializer.DeserializeFromBytes<IMessage>(data);
                block.AppendBlock(revision, position, messageData, timestamp);
            }

            return block.Complete();
        }

        private async Task<AggregateEventRaw> LoadAggregateEventRaw(IndexRecord indexRecord, PreparedStatement queryStatement, ISession session)
        {
            try
            {
                BoundStatement query = queryStatement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);
                RowSet rowSet = await session.ExecuteAsync(query).ConfigureAwait(false);
                Row row = rowSet.SingleOrDefault();
                if (row is not null)
                {
                    byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);
                    return new AggregateEventRaw(indexRecord.AggregateRootId, data, indexRecord.Revision, indexRecord.Position, indexRecord.TimeStamp);
                }

                logger.Error(() => $"Unable to load aggregate event by index record: {indexRecord.ToJson()}");
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Unable to load aggregate event by index record: {indexRecord.ToJson()}"))
            {
            }

            return default;
        }

        private async Task<LoadAggregateRawEventsWithPagingResult> LoadAggregateRawEventsWithPagingAsync(IBlobId id, PagingOptions pagingOptions)
        {
            List<AggregateEventRaw> aggregateEventRawCollection = new List<AggregateEventRaw>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement ps = await GetReadWithPagingByRevisionStatementAsync(session).ConfigureAwait(false);
            IStatement boundStatement = ps.Bind(id.RawId)
                .SetPageSize(pagingOptions.Take)
                .SetAutoPage(false);

            if (pagingOptions.PaginationToken is not null)
                boundStatement.SetPagingState(pagingOptions.PaginationToken);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            var rows = result.GetRows();
            foreach (var row in rows)
            {
                int revision = row.GetValue<int>(CassandraColumn.Revision);
                int position = row.GetValue<int>(CassandraColumn.Position);
                long timestamp = row.GetValue<long>(CassandraColumn.Timestamp);
                byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

                aggregateEventRawCollection.Add(new AggregateEventRaw(id.RawId, data, revision, position, timestamp));
            }

            return new LoadAggregateRawEventsWithPagingResult(aggregateEventRawCollection, new PagingOptions(pagingOptions.Take, result.PagingState));
        }

        private async Task EnumerateEventStoreForSpecifiedEvent(PlayerOperator @operator, PlayerOptions replayOptions)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement queryStatement = await PrepareLoadEventQueryStatementAsync(session).ConfigureAwait(false);

            List<Task> tasks = new List<Task>();
            await foreach (IndexRecord indexRecord in indexByEventTypeStore.GetRecordsAsync(replayOptions, @operator.NotifyProgressAsync))
            {
                if (@operator.OnLoadAsync is not null)
                {
                    Task task =
                        LoadAggregateEventRaw(indexRecord, queryStatement, session)
                        .ContinueWith(input =>
                        {
                            if (input.Result is not null)
                                @operator.OnLoadAsync(input.Result);
                        });
                    tasks.Add(task);

                    if (tasks.Count >= replayOptions.MaxDegreeOfParallelism)
                    {
                        Task completedTask = await Task.WhenAny(tasks);
                        if (completedTask.Status == TaskStatus.Faulted)
                        {
                            logger.ErrorException(completedTask.Exception, () => $"Failed to replay event for index record: {indexRecord.ToJson()}");
                        }
                        tasks.Remove(completedTask);
                    }
                }
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private async Task EnumerateEventStoreGG(PlayerOperator @operator, PlayerOptions replayOptions)
        {
            List<AggregateEventRaw> aggregateEventRaws = new List<AggregateEventRaw>();

            List<Task> tasks = new List<Task>();
            await foreach (AggregateEventRaw @event in LoadEntireEventStoreAsync(replayOptions, @operator.NotifyProgressAsync))
            {
                if (@operator.OnLoadAsync is not null)
                {
                    Task opTask = @operator.OnLoadAsync(@event);
                    tasks.Add(opTask);

                    if (tasks.Count >= replayOptions.MaxDegreeOfParallelism)
                    {
                        Task completedTask = await Task.WhenAny(tasks);
                        if (completedTask.Status == TaskStatus.Faulted)
                        {
                            string dataAsJson = System.Text.Json.JsonSerializer.Serialize(@event);
                            logger.ErrorException(completedTask.Exception, () => $"Failed to replay event: {dataAsJson}");
                        }
                        tasks.Remove(completedTask);
                    }
                }

                if (@operator.OnAggregateStreamLoadedAsync is not null)
                {
                    if (aggregateEventRaws.Any() && ByteArrayHelper.Compare(aggregateEventRaws.First().AggregateRootId, @event.AggregateRootId) == false)
                    {
                        AggregateStream stream = new AggregateStream(aggregateEventRaws);
                        await @operator.OnAggregateStreamLoadedAsync(stream);

                        aggregateEventRaws.Clear();
                    }

                    aggregateEventRaws.Add(@event);
                }
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
        }

        private async IAsyncEnumerable<AggregateEventRaw> LoadEntireEventStoreAsync(PlayerOptions replayOptions, Func<PlayerOptions, Task> onPagingInfoChanged = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            PagingInfo pagingInfo = PagingInfo.Parse(replayOptions.PaginationToken);
            long after = replayOptions.After.HasValue ? replayOptions.After.Value.ToFileTime() : 0;
            long before = replayOptions.Before.HasValue ? replayOptions.Before.Value.ToFileTime() : 0;

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReplayStatementAsync(session).ConfigureAwait(false);

            IStatement queryStatement = statement.Bind();
            queryStatement
                .SetPageSize(replayOptions.BatchSize)
                .SetAutoPage(false);

            while (pagingInfo.HasMore)
            {
                if (pagingInfo.HasToken())
                    queryStatement.SetPagingState(pagingInfo.Token);

                RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);

                foreach (var row in result.GetRows())
                {
                    long timestamp = row.GetValue<long>(CassandraColumn.Timestamp);
                    if (after <= timestamp && timestamp <= before)
                    {
                        byte[] id = row.GetValue<byte[]>(CassandraColumn.Id);
                        int revision = row.GetValue<int>(CassandraColumn.Revision);
                        int position = row.GetValue<int>(CassandraColumn.Position);
                        byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

                        var @event = new AggregateEventRaw(id, data, revision, position, timestamp);
                        yield return @event;
                    }

                    if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested)
                        break;
                }

                pagingInfo = HandlePaginationStateChanges(replayOptions, onPagingInfoChanged, pagingInfo, result);
            }
        }

        private PagingInfo HandlePaginationStateChanges(PlayerOptions replayOptions, Func<PlayerOptions, Task> onPagingInfoChanged, PagingInfo pagingInfo, RowSet result)
        {
            PagingInfo nextPagingInfo = PagingInfo.From(result);

            bool isFirstTime = pagingInfo.Token is null;
            bool hasMoreRecords = result.PagingState is not null;

            bool weHaveNewPagingState = (isFirstTime && hasMoreRecords) || (isFirstTime == false && hasMoreRecords && ByteArrayHelper.Compare(pagingInfo.Token, nextPagingInfo.Token) == false);
            pagingInfo = nextPagingInfo;
            if (onPagingInfoChanged is not null && weHaveNewPagingState)
            {
                try { Task notify = onPagingInfoChanged(replayOptions.WithPaginationToken(pagingInfo.ToString())); }
                catch (Exception ex) when (logger.ErrorException(ex, () => "Failed to execute onPagingInfoChanged() function.")) { }
            }

            return pagingInfo;
        }

        private async Task<PreparedStatement> GetRebuildDataStatementAsync(ISession session)
        {
            if (replayWithoutDataStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayWithoutDataStatement = await session.PrepareAsync(string.Format(LoadAggregateEventsRebuildQueryTemplate, tableName)).ConfigureAwait(false);
                replayWithoutDataStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayWithoutDataStatement;
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

        private async Task<PreparedStatement> GetDeleteStatement(ISession session)
        {
            if (deleteStatement is null)
            {

                string tableName = tableNameStrategy.GetName();
                deleteStatement = await session.PrepareAsync(string.Format(DeleteEventQueryTemplate, tableName)).ConfigureAwait(false);
                deleteStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return deleteStatement;
        }

        private async Task<PreparedStatement> GetReplayStatementAsync(ISession session)
        {
            if (replayStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayStatement = await session.PrepareAsync(string.Format(LoadEventsQueryTemplate, tableName)).ConfigureAwait(false);
                replayStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayStatement;
        }

        private async Task<PreparedStatement> GetReadWithPagingByRevisionStatementAsync(ISession session)
        {
            if (readWithPagingByRevisionStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                readWithPagingByRevisionStatement = await session.PrepareAsync(string.Format(LoadAggregateEventsWithinSpecifiedRevisionsQueryTemplate, tableName)).ConfigureAwait(false);
                readWithPagingByRevisionStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readWithPagingByRevisionStatement;
        }
    }
}
