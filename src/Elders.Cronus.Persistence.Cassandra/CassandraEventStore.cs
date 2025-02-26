using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStore<TSettings> : CassandraEventStore, IEventStorePlayer<TSettings>
    where TSettings : class, ICassandraEventStoreSettings
    {
        public CassandraEventStore(ICronusContextAccessor cronusContextAccessor, TSettings settings, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger)
            : base(cronusContextAccessor, settings.CassandraProvider, settings.TableNameStrategy, settings.Serializer, indexByEventTypeStore, logger)
        {
        }
    }




    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public class CassandraEventStore : IEventStore, IEventStorePlayer
    {
        private readonly ISerializer serializer;
        private readonly IndexByEventTypeStore indexByEventTypeStore;
        private readonly ILogger<CassandraEventStore> logger;
        private readonly ICronusContextAccessor cronusContextAccessor;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        // the store is registered as tenant singleton and the events table is only 1 so there could only be one prepared statement per tenant
        private LoadAggregateEventsQuery _loadAggregateEventsQuery;
        private InsertEventsQuery _insertEventsQuery;
        private LoadEventsQuery _loadEventsQuery;
        private LoadAggregateEventsWithinSpecifiedRevisionsQuery _loadAggregateEventsWithinSpecifiedRevisionsQuery;
        private LoadAggregateEventsRebuildQuery _loadAggregateRebuildEventsPreparedStatements;
        private LoadEventQuery _loadEventQuery;
        private DeleteEventQuery _deleteEventQuery;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraEventStore(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer, IndexByEventTypeStore indexByEventTypeStore, ILogger<CassandraEventStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cronusContextAccessor = cronusContextAccessor;
            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            this.indexByEventTypeStore = indexByEventTypeStore ?? throw new ArgumentNullException(nameof(indexByEventTypeStore));
            this.logger = logger;

            _loadEventQuery = new LoadEventQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _loadAggregateEventsQuery = new LoadAggregateEventsQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _insertEventsQuery = new InsertEventsQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _loadEventsQuery = new LoadEventsQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _loadAggregateEventsWithinSpecifiedRevisionsQuery = new LoadAggregateEventsWithinSpecifiedRevisionsQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _loadEventQuery = new LoadEventQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
            _deleteEventQuery = new DeleteEventQuery(cronusContextAccessor, cassandraProvider, tableNameStrategy);
        }

        public async Task AppendAsync(AggregateCommit aggregateCommit)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement writeStatement = await _insertEventsQuery.PrepareAsync(session).ConfigureAwait(false);
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
                logger.LogWarning(ex, "Write timeout while persisting an aggregate commit.");
            }
        }

        public async Task AppendAsync(AggregateEventRaw aggregateCommitRaw)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await _insertEventsQuery.PrepareAsync(session).ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Revision, aggregateCommitRaw.Position, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Data);

                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.LogWarning(ex, "Write timeout while persisting an aggregate commit.");
            }
        }

        public async Task<EventStream> LoadAsync(IBlobId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = await LoadAggregateCommitsAsync(aggregateId).ConfigureAwait(false);

            return new EventStream(aggregateCommits);
        }

        public async Task<LoadAggregateRawEventsWithPagingResult> LoadWithPagingAsync(IBlobId aggregateId, PagingOptions pagingOptions)
        {
            LoadAggregateRawEventsWithPagingResult result = await LoadAggregateRawEventsWithPagingAsync(aggregateId, pagingOptions).ConfigureAwait(false);

            return result;
        }

        public async Task<bool> DeleteAsync(AggregateEventRaw eventRaw)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await _deleteEventQuery.PrepareAsync(session).ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(eventRaw.AggregateRootId, eventRaw.Revision, eventRaw.Position);

                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);

                return true;
            }
            catch (WriteTimeoutException ex) when (True(() => logger.LogWarning(ex, "Failed to delete event.")))
            {
                return false;
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to delete event."))) { }
            {
                return false;
            }
        }

        public Task EnumerateEventStore(PlayerOperator @operator, PlayerOptions replayOptions, CancellationToken cancellationToken = default)
        {
            if (@operator is null) throw new ArgumentNullException(nameof(@operator));
            if (replayOptions is null) throw new ArgumentNullException(nameof(replayOptions));

            if (replayOptions.EventTypeId is null)
            {
                return EnumerateEventStoreGG(@operator, replayOptions, cancellationToken);
            }
            else
            {
                return EnumerateEventStoreForSpecifiedEvent(@operator, replayOptions, cancellationToken);
            }
        }

        public async Task<IEvent> LoadEventWithRebuildProjectionAsync(IndexRecord indexRecord)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _loadAggregateRebuildEventsPreparedStatements.PrepareAsync(session).ConfigureAwait(false);

            BoundStatement boundStatement = statement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            var row = result.GetRows().Single();
            byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

            return serializer.DeserializeFromBytes<IEvent>(data);
        }

        public async Task<AggregateEventRaw> LoadAggregateEventRaw(IndexRecord indexRecord)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement queryStatement = await _loadEventQuery.PrepareAsync(session).ConfigureAwait(false);
            BoundStatement query = queryStatement.Bind(indexRecord.AggregateRootId, indexRecord.Revision, indexRecord.Position);
            RowSet rowSet = await session.ExecuteAsync(query).ConfigureAwait(false);
            Row row = rowSet.SingleOrDefault();
            if (row is not null)
            {
                byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);
                return new AggregateEventRaw(indexRecord.AggregateRootId, data, indexRecord.Revision, indexRecord.Position, indexRecord.TimeStamp);
            }

            logger.LogError("Unable to load aggregate event by index record: {cronus_messageData}", indexRecord.ToJson());

            return default;
        }

        private async Task<List<AggregateCommit>> LoadAggregateCommitsAsync(IBlobId id)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement bs = await _loadAggregateEventsQuery.PrepareAsync(session).ConfigureAwait(false);
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

        private async Task<AggregateEventRaw> LoadAggregateEventRawAsync(IndexRecord indexRecord, PreparedStatement queryStatement, ISession session)
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

                logger.LogError("Unable to load aggregate event by index record: {cronus_messageData}", indexRecord.ToJson());
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Unable to load aggregate event by index record: {cronus_messageData}", indexRecord.ToJson()))) { }

            return default;
        }

        private async Task<LoadAggregateRawEventsWithPagingResult> LoadAggregateRawEventsWithPagingAsync(IBlobId id, PagingOptions pagingOptions)
        {
            List<AggregateEventRaw> aggregateEventRawCollection = new List<AggregateEventRaw>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            IStatement boundStatement;

            if (pagingOptions.Order?.Equals(Order.Descending) ?? false)
            {
                PreparedStatement ps = await _loadAggregateEventsWithinSpecifiedRevisionsQuery.PrepareAsync(session).ConfigureAwait(false);
                boundStatement = ps.Bind(id.RawId)
                    .SetPageSize(pagingOptions.Take)
                    .SetAutoPage(false);
            }
            else
            {
                PreparedStatement ps = await _loadAggregateEventsQuery.PrepareAsync(session).ConfigureAwait(false);
                boundStatement = ps.Bind(id.RawId)
                    .SetPageSize(pagingOptions.Take)
                    .SetAutoPage(false);
            }

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

            return new LoadAggregateRawEventsWithPagingResult(aggregateEventRawCollection, new PagingOptions(pagingOptions.Take, result.PagingState, pagingOptions.Order));
        }

        private async Task EnumerateEventStoreForSpecifiedEvent(PlayerOperator @operator, PlayerOptions replayOptions, CancellationToken cancellationToken = default)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement queryStatement = await _loadEventQuery.PrepareAsync(session).ConfigureAwait(false);

            List<Task> tasks = new List<Task>();
            await foreach (IndexRecord indexRecord in indexByEventTypeStore.GetRecordsAsync(replayOptions, @operator.NotifyProgressAsync, cancellationToken))
            {
                if (@operator.OnLoadAsync is not null)
                {
                    Task task = Task.Run(async () =>
                    {
                        var rawEventLoaded = await LoadAggregateEventRawAsync(indexRecord, queryStatement, session).ConfigureAwait(false);
                        if (rawEventLoaded is not null)
                            await @operator.OnLoadAsync(rawEventLoaded).ConfigureAwait(false);
                    });

                    tasks.Add(task);

                    if (tasks.Count >= replayOptions.MaxDegreeOfParallelism)
                    {
                        Task completedTask = await Task.WhenAny(tasks);
                        if (completedTask.Status == TaskStatus.Faulted)
                        {
                            logger.LogError(completedTask.Exception, "Failed to replay event for index record: {cronus_messageData}", indexRecord.ToJson());
                        }
                        tasks.Remove(completedTask);
                    }
                }

                if (@operator.OnAggregateStreamLoadedAsync is not null)
                {
                    Task task = Task.Run(async () =>
                    {
                        var stream = await LoadAsync(indexRecord.AggregateRootId).ConfigureAwait(false);
                        await @operator.OnAggregateStreamLoadedAsync(stream).ConfigureAwait(false);
                    });
                    tasks.Add(task);

                    if (tasks.Count >= replayOptions.MaxDegreeOfParallelism)
                    {
                        Task completedTask = await Task.WhenAny(tasks);
                        if (completedTask.Status == TaskStatus.Faulted)
                        {
                            logger.LogError(completedTask.Exception, "Failed to replay event for index record: {cronus_messageData}", indexRecord.ToJson());
                        }
                        tasks.Remove(completedTask);
                    }
                }
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            if (@operator.OnFinish is not null)
            {
                await @operator.OnFinish().ConfigureAwait(false);
            }
        }

        private async Task EnumerateEventStoreGG(PlayerOperator @operator, PlayerOptions replayOptions, CancellationToken cancellationToken)
        {
            List<AggregateEventRaw> aggregateEventRaws = new List<AggregateEventRaw>();

            List<Task> tasks = new List<Task>();
            await foreach (AggregateEventRaw @event in LoadEntireEventStoreAsync(replayOptions, @operator.NotifyProgressAsync).ConfigureAwait(false))
            {
                if (@operator.OnLoadAsync is not null)
                {
                    Task opTask = @operator.OnLoadAsync(@event);
                    tasks.Add(opTask);

                    if (tasks.Count >= replayOptions.MaxDegreeOfParallelism)
                    {
                        Task completedTask = await Task.WhenAny(tasks).ConfigureAwait(false);
                        if (completedTask.Status == TaskStatus.Faulted)
                        {
                            string dataAsJson = System.Text.Json.JsonSerializer.Serialize(@event);
                            logger.LogError(completedTask.Exception, "Failed to replay event: {cronus_messageData}", dataAsJson);
                        }
                        tasks.Remove(completedTask);
                    }
                }

                if (@operator.OnAggregateStreamLoadedAsync is not null)
                {
                    // I know you are confused. Do not worry, just read the comments bellow:
                    // All aggregates events are stored in a single partition where the ID of the AR is the partition value.
                    // This way all events for an AR will be loaded before proceeding to the next AR.
                    // This is Cassandra specific behavior and should not be cloned to other DB implementations.
                    if (aggregateEventRaws.Count > 0 && aggregateEventRaws.First().AggregateRootId.Span.SequenceEqual(@event.AggregateRootId.Span) == false)
                    {
                        AggregateStream stream = new AggregateStream(aggregateEventRaws);
                        await @operator.OnAggregateStreamLoadedAsync(stream).ConfigureAwait(false);

                        aggregateEventRaws.Clear();
                    }

                    aggregateEventRaws.Add(@event);
                }
            }

            // No child left behind. Make sure the last aggregate is also passed allong.
            if (@operator.OnAggregateStreamLoadedAsync is not null && aggregateEventRaws.Count > 0)
            {
                AggregateStream stream = new AggregateStream(aggregateEventRaws);
                await @operator.OnAggregateStreamLoadedAsync(stream).ConfigureAwait(false);
                aggregateEventRaws.Clear();
            }

            await Task.WhenAll(tasks).ConfigureAwait(false);
            if (@operator.OnFinish is not null)
            {
                await @operator.OnFinish().ConfigureAwait(false);
            }
        }

        private async Task<AggregateStream> LoadAsync(ReadOnlyMemory<byte> id)
        {
            List<AggregateEventRaw> aggregateEvents = new List<AggregateEventRaw>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement bs = await _loadAggregateEventsQuery.PrepareAsync(session).ConfigureAwait(false);
            BoundStatement boundStatement = bs.Bind(id);

            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                int revision = row.GetValue<int>(CassandraColumn.Revision);
                int position = row.GetValue<int>(CassandraColumn.Position);
                long timestamp = row.GetValue<long>(CassandraColumn.Timestamp);
                byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);

                var eventRaw = new AggregateEventRaw(id, data, revision, position, timestamp);
                aggregateEvents.Add(eventRaw);
            }

            return new AggregateStream(aggregateEvents);
        }

        private async IAsyncEnumerable<AggregateEventRaw> LoadEntireEventStoreAsync(PlayerOptions replayOptions, Func<PlayerOptions, Task> onPagingInfoChanged = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            PagingInfo pagingInfo = PagingInfo.Parse(replayOptions.PaginationToken);
            long after = replayOptions.After.HasValue ? replayOptions.After.Value.ToFileTime() : 0;
            long before = replayOptions.Before.HasValue ? replayOptions.Before.Value.ToFileTime() : 0;

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _loadEventsQuery.PrepareAsync(session).ConfigureAwait(false);

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

                if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested)
                    break;

                pagingInfo = await HandlePaginationStateChangesAsync(replayOptions, onPagingInfoChanged, pagingInfo, result).ConfigureAwait(false);
            }
        }

        private async Task<PagingInfo> HandlePaginationStateChangesAsync(PlayerOptions replayOptions, Func<PlayerOptions, Task> onPagingInfoChanged, PagingInfo pagingInfo, RowSet result)
        {
            pagingInfo = PagingInfo.From(result);
            if (onPagingInfoChanged is not null)
            {
                try { await onPagingInfoChanged(replayOptions.WithPaginationToken(pagingInfo.ToString())).ConfigureAwait(false); }
                catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to execute onPagingInfoChanged() function."))) { }
            }

            return pagingInfo;
        }

    }

    internal class LoadEventQuery : PreparedStatementCache
    {
        private const string LoadEventQueryTemplate = @"SELECT data,ts FROM {0}.{1} WHERE id = ? AND rev = ? AND pos = ?;";

        public LoadEventQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => LoadEventQueryTemplate;
    }

    internal class LoadAggregateEventsQuery : PreparedStatementCache
    {
        private const string Template = @"SELECT rev,pos,ts,data FROM {0}.{1} WHERE id = ?;";

        public LoadAggregateEventsQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class InsertEventsQuery : PreparedStatementCache
    {
        private const string Template = @"INSERT INTO {0}.{1} (id,rev,pos,ts,data) VALUES (?,?,?,?,?);";

        public InsertEventsQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class LoadEventsQuery : PreparedStatementCache
    {
        private const string Template = @"SELECT id,rev,pos,ts,data FROM {0}.{1};";

        public LoadEventsQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class LoadAggregateEventsWithinSpecifiedRevisionsQuery : PreparedStatementCache
    {
        private const string Template = @"SELECT rev,pos,ts,data FROM {0}.{1} WHERE id = ? order by rev desc, pos desc";

        public LoadAggregateEventsWithinSpecifiedRevisionsQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class LoadAggregateEventsRebuildQuery : PreparedStatementCache
    {
        private const string Template = @"SELECT data FROM {0}.{1} WHERE id = ? AND rev = ? AND pos = ?;";

        public LoadAggregateEventsRebuildQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class DeleteEventQuery : PreparedStatementCache
    {
        private const string Template = @"DELETE FROM {0}.{1} WHERE id = ? and rev=? and pos=?;";

        public DeleteEventQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy) : base(context, cassandraProvider, tableNameStrategy) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal abstract class PreparedStatementCache
    {
        private readonly ICronusContextAccessor context;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private SemaphoreSlim threadGate = new SemaphoreSlim(1);
        private Dictionary<string, PreparedStatement> _tenantCache;

        protected PreparedStatementCache(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : this(context, cassandraProvider, default) { }

        public PreparedStatementCache(ICronusContextAccessor context, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy)
        {
            _tenantCache = new Dictionary<string, PreparedStatement>();

            this.context = context;
            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy;
        }

        internal abstract string GetQueryTemplate();
        internal virtual string GetTableName() => tableNameStrategy?.GetName();

        internal async Task<PreparedStatement> PrepareAsync(ISession session)
        {
            try
            {
                PreparedStatement preparedStatement = default;
                if (_tenantCache.TryGetValue(context.CronusContext.Tenant, out preparedStatement) == false)
                {
                    await threadGate.WaitAsync(10000).ConfigureAwait(false);
                    if (_tenantCache.TryGetValue(context.CronusContext.Tenant, out preparedStatement))
                        return preparedStatement;

                    string keyspace = cassandraProvider.GetKeyspace();
                    string tableName = GetTableName();
                    string template = GetQueryTemplate();
                    string query = string.Format(template, keyspace, tableName);

                    preparedStatement = await session.PrepareAsync(query).ConfigureAwait(false);
                    SetPreparedStatementOptions(preparedStatement);

                    _tenantCache.TryAdd(context.CronusContext.Tenant, preparedStatement);
                }

                return preparedStatement;
            }
            finally
            {
                threadGate?.Release();
            }
        }

        internal virtual void SetPreparedStatementOptions(PreparedStatement statement)
        {
            statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        }
    }
}
