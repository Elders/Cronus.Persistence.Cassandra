using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStore<TSettings> : CassandraEventStore, IEventStorePlayer<TSettings>
        where TSettings : class, ICassandraEventStoreSettings
    {
        public CassandraEventStore(TSettings settings)
            : base(settings.CassandraProvider, settings.TableNameStrategy, settings.Serializer)
        {
        }
    }

    public class CassandraEventStore : IEventStore, IEventStorePlayer
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStore));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";
        private const string LoadAggregateCommitsQueryTemplate = @"SELECT id,ts,rev,data FROM {0};";
        private const string LoadAggregateCommitsQueryWithoutDataTemplate = @"SELECT ts FROM {0} WHERE id = ?;";


        private const string LoadAggregateCommitsMetaQueryTemplate = @"SELECT ts,rev,data FROM {0} WHERE id = ?;";

        private readonly ISerializer serializer;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;
        private PreparedStatement replayStatement;
        private PreparedStatement replayWithoutDataStatement;
        private PreparedStatement loadAggregateCommitsMetaStatement;

        public CassandraEventStore(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer)); ;
        }

        public async Task AppendAsync(AggregateCommit aggregateCommit)
        {
            byte[] data = SerializeEvent(aggregateCommit);

            try
            {
                PreparedStatement statement = await GetWriteStatementAsync().ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(Convert.ToBase64String(aggregateCommit.AggregateRootId), aggregateCommit.Timestamp, aggregateCommit.Revision, data);

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
            }
        }

        public async Task<EventStream> LoadAsync(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();
            PreparedStatement bs = await GetReadStatementAsync().ConfigureAwait(false);
            BoundStatement boundStatement = bs.Bind(Convert.ToBase64String(aggregateId.RawId));

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    aggregateCommits.Add((AggregateCommit)serializer.Deserialize(stream));
                }
            }

            return new EventStream(aggregateCommits);
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

        public async Task<LoadAggregateCommitsResult> LoadAggregateCommitsAsync(string paginationToken, int pageSize = 5000)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadAggregateCommitsResult() { PaginationToken = paginationToken };

            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            IStatement queryStatement = (await GetReplayStatementAsync()).Bind().SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            ISession session = await GetSessionAsync();
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    AggregateCommit commit;
                    try
                    {
                        commit = (AggregateCommit)serializer.Deserialize(stream);
                    }
                    catch (Exception ex)
                    {
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }
                    aggregateCommits.Add(commit);
                }
            }

            if (result.IsFullyFetched == false)
            {
                logger.Warn(() => "Not implemented logic. => if (result.IsFullyFetched == false)");
            }

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
            ISession session = await GetSessionAsync();
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    AggregateCommit commit;
                    try
                    {
                        commit = (AggregateCommit)serializer.Deserialize(stream);
                    }
                    catch (Exception ex)
                    {
                        string error = "[EventStore] Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }

                    yield return commit;
                }
            }
        }

        public async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync()
        {
            var queryStatement = (await GetReplayStatementAsync().ConfigureAwait(false)).Bind();
            ISession session = await GetSessionAsync();
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    AggregateCommit commit;
                    try
                    {
                        commit = (AggregateCommit)serializer.Deserialize(stream);
                    }
                    catch (Exception ex)
                    {
                        string error = "[EventStore] Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        logger.ErrorException(ex, () => error);
                        continue;
                    }

                    yield return commit;
                }
            }
        }

        public async IAsyncEnumerable<AggregateCommitRaw> LoadAggregateCommitsRawAsync(int batchSize = 5000)
        {
            var queryStatement = (await GetReplayStatementAsync().ConfigureAwait(false)).Bind().SetPageSize(batchSize);
            ISession session = await GetSessionAsync();
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                string id = row.GetValue<string>("id");
                byte[] data = row.GetValue<byte[]>("data");
                int revision = row.GetValue<int>("rev");
                long timestamp = row.GetValue<long>("ts");

                using (var stream = new MemoryStream(data))
                {
                    AggregateCommitRaw commitRaw = new AggregateCommitRaw(id, data, revision, timestamp);

                    yield return commitRaw;
                }
            }
        }

        private async Task<PreparedStatement> LoadAggregateCommitsMetaStatementAsync()
        {
            if (loadAggregateCommitsMetaStatement is null)
            {
                ISession session = await GetSessionAsync();
                string tableName = tableNameStrategy.GetName();
                loadAggregateCommitsMetaStatement = await session.PrepareAsync(string.Format(LoadAggregateCommitsMetaQueryTemplate, tableName)).ConfigureAwait(false);
                loadAggregateCommitsMetaStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return loadAggregateCommitsMetaStatement;
        }

        private async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsMetaAsync(IEnumerable<IAggregateRootId> arIds, long afterTimestamp, long beforeStamp)
        {
            PreparedStatement queryStatement = await LoadAggregateCommitsMetaStatementAsync().ConfigureAwait(false);
            foreach (IAggregateRootId arId in arIds)
            {
                BoundStatement q = queryStatement.Bind(Convert.ToBase64String(arId.RawId));
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                RowSet result = await session.ExecuteAsync(q).ConfigureAwait(false);

                foreach (var row in result.GetRows())
                {
                    long timestamp = row.GetValue<long>("ts");
                    if (afterTimestamp > timestamp || timestamp > beforeStamp)
                        continue;

                    var data = row.GetValue<byte[]>("data");
                    using (var stream = new MemoryStream(data))
                    {
                        AggregateCommit commit;
                        try
                        {
                            commit = (AggregateCommit)serializer.Deserialize(stream);
                        }
                        catch (Exception ex)
                        {
                            string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                            logger.ErrorException(ex, () => error);
                            continue;
                        }
                        yield return commit;
                    }
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
                string id = row.GetValue<string>("id");
                byte[] data = row.GetValue<byte[]>("data");
                int revision = row.GetValue<int>("rev");
                long timestamp = row.GetValue<long>("ts");

                using (var stream = new MemoryStream(data))
                {
                    AggregateCommitRaw commitRaw = new AggregateCommitRaw(id, data, revision, timestamp);

                    yield return commitRaw;
                }
            }
        }

        private byte[] SerializeEvent(AggregateCommit commit)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, commit);
                return stream.ToArray();
            }
        }

        private async Task<PreparedStatement> GetWriteStatementAsync()
        {
            if (writeStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                string tableName = tableNameStrategy.GetName();
                writeStatement = await session.PrepareAsync(string.Format(InsertEventsQueryTemplate, tableName)).ConfigureAwait(false);
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private async Task<PreparedStatement> GetReadStatementAsync()
        {
            if (readStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
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

        private async Task<PreparedStatement> GetReplayWithoutDataStatementAsync()
        {
            if (replayWithoutDataStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                string tableName = tableNameStrategy.GetName();
                replayWithoutDataStatement = await session.PrepareAsync(string.Format(LoadAggregateCommitsQueryWithoutDataTemplate, tableName)).ConfigureAwait(false);
                replayWithoutDataStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayWithoutDataStatement;
        }

        public async Task AppendAsync(AggregateCommitRaw aggregateCommitRaw)
        {
            try
            {
                PreparedStatement statement = await GetWriteStatementAsync().ConfigureAwait(false);
                BoundStatement boundStatement = statement.Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Revision, aggregateCommitRaw.Data);

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit");
            }
        }

        public async Task<LoadAggregateCommitsResult> LoadAggregateCommitsAsync(ReplayOptions replayOptions)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            string paginationToken = replayOptions.PaginationToken;
            int pageSize = replayOptions.BatchSize;

            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadAggregateCommitsResult() { PaginationToken = paginationToken };

            #region TerribleCode
            // AAAAAAAAAAAAAAAAA why did you expand this. Now you have to fix it.
            bool hasTimeRangeFilter = replayOptions.After.HasValue || replayOptions.Before.HasValue;
            long afterTimestamp = 0; // 1/1/1601 2:00:00 AM +02:00
            long beforeStamp = 2650381343999999999; // DateTimeOffset.MaxValue.Subtract(TimeSpan.FromDays(100)).ToFileTime()
            if (replayOptions.After.HasValue)
                afterTimestamp = replayOptions.After.Value.ToFileTime();
            if (replayOptions.Before.HasValue)
                beforeStamp = replayOptions.Before.Value.ToFileTime();
            #endregion

            var found = LoadAggregateCommitsMetaAsync(replayOptions.AggregateIds, afterTimestamp, beforeStamp).ConfigureAwait(false);
            await foreach (var meta in found)
                aggregateCommits.Add(meta);


            return new LoadAggregateCommitsResult()
            {
                Commits = aggregateCommits,
                PaginationToken = null
            };
        }
    }
}
