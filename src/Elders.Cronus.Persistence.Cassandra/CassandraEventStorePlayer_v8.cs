using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Migrations;
using Elders.Cronus.Persistence.Cassandra.Migrations;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStorePlayer_v8 : IMigrationEventStorePlayer
    {
        private readonly ILogger<CassandraEventStorePlayer_v8> logger;

        private const string LoadAggregateCommitsQueryTemplate = @"SELECT id,ts,rev,data FROM {0};";
        private const string LoadAggregateCommitsMetaQueryTemplate = @"SELECT ts,rev,data FROM {0} WHERE id = ?;";

        private readonly ISerializer serializer;
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        private PreparedStatement replayStatement;
        private PreparedStatement loadAggregateCommitsMetaStatement;

        public CassandraEventStorePlayer_v8(MigratorCassandraReplaySettings settings, ILogger<CassandraEventStorePlayer_v8> logger)
        {
            if (settings is null) throw new ArgumentNullException(nameof(settings));

            this.cassandraProvider = settings.CassandraProvider;
            this.tableNameStrategy = settings.TableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.serializer = settings.Serializer ?? throw new ArgumentNullException(nameof(serializer)); ;
            this.logger = logger;
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

        public async Task<LoadAggregateCommitsResult> LoadAggregateCommitsAsync(string paginationToken, int pageSize = 5000)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadAggregateCommitsResult() { PaginationToken = paginationToken };

            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            IStatement queryStatement = (await GetReplayStatementAsync().ConfigureAwait(false)).Bind().SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
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
            ISession session = await GetSessionAsync().ConfigureAwait(false);
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
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                byte[] id = row.GetValue<byte[]>("id");
                byte[] data = row.GetValue<byte[]>("data");
                int revision = row.GetValue<int>("rev");
                long timestamp = row.GetValue<long>("ts");

                using (var stream = new MemoryStream(data))
                {
                    AggregateCommitRaw commitRaw = new AggregateCommitRaw(id, data, revision, 0, timestamp);

                    yield return commitRaw;
                }
            }
        }

        private async Task<PreparedStatement> LoadAggregateCommitsMetaStatementAsync()
        {
            if (loadAggregateCommitsMetaStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
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

        private PagingInfo GetPagingInfo(string paginationToken)
        {
            // I am not shure about this conflict...
            // This is another version of the GetPagingInfo()
            //  ISession session = await GetSessionAsync().ConfigureAwait(false);
            // var queryStatement = (await GetReplayStatementAsync().ConfigureAwait(false)).Bind();
            // var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            // foreach (var row in result.GetRows())
            // {
            //     string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
            //     pagingInfo = JsonSerializer.Deserialize<PagingInfo>(paginationJson);
            // }
            // return pagingInfo;

            PagingInfo pagingInfo = new PagingInfo();
            if (string.IsNullOrEmpty(paginationToken) == false)
            {
                string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
                pagingInfo = JsonSerializer.Deserialize<PagingInfo>(paginationJson);
            }
            return pagingInfo;
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

        public Task<IEvent> LoadEventWithRebuildProjectionAsync(IndexRecord indexRecord)
        {
            throw new NotImplementedException();
        }
    }
}
