using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
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

        private ISession GetSession() => cassandraProvider.GetSession(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

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

        public void Append(AggregateCommit aggregateCommit)
        {
            byte[] data = SerializeEvent(aggregateCommit);

            try
            {
                GetSession()
                    .Execute(GetWriteStatement()
                        .Bind(Convert.ToBase64String(aggregateCommit.AggregateRootId), aggregateCommit.Timestamp, aggregateCommit.Revision, data));
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit.");
            }
        }

        public EventStream Load(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();
            BoundStatement bs = GetReadStatement().Bind(Convert.ToBase64String(aggregateId.RawId));
            var result = GetSession().Execute(bs);
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

        public LoadAggregateCommitsResult LoadAggregateCommits(string paginationToken, int pageSize = 5000)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadAggregateCommitsResult() { PaginationToken = paginationToken };

            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            IStatement queryStatement = GetReplayStatement().Bind().SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            RowSet result = GetSession().Execute(queryStatement);
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

        public IEnumerable<AggregateCommit> LoadAggregateCommits(int batchSize)
        {
            var queryStatement = GetReplayStatement().Bind().SetPageSize(batchSize);
            RowSet result = GetSession().Execute(queryStatement);
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
            var queryStatement = GetReplayStatement().Bind();
            RowSet result = await GetSession().ExecuteAsync(queryStatement).ConfigureAwait(false);
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

        public IEnumerable<AggregateCommitRaw> LoadAggregateCommitsRaw(int batchSize = 5000)
        {
            var queryStatement = GetReplayStatement().Bind().SetPageSize(batchSize);
            var result = GetSession().Execute(queryStatement);
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

        private PreparedStatement LoadAggregateCommitsMetaStatement()
        {
            if (loadAggregateCommitsMetaStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                loadAggregateCommitsMetaStatement = GetSession().Prepare(string.Format(LoadAggregateCommitsMetaQueryTemplate, tableName));
                loadAggregateCommitsMetaStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return loadAggregateCommitsMetaStatement;
        }

        private IEnumerable<AggregateCommit> LoadAggregateCommitsMeta(IEnumerable<IAggregateRootId> arIds, long afterTimestamp, long beforeStamp)
        {
            var queryStatement = LoadAggregateCommitsMetaStatement();
            foreach (IAggregateRootId arId in arIds)
            {
                var q = queryStatement.Bind(Convert.ToBase64String(arId.RawId));
                var result = GetSession().Execute(q);

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
            var queryStatement = GetReplayStatement().Bind();
            var result = await GetSession().ExecuteAsync(queryStatement);
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

        private PreparedStatement GetWriteStatement()
        {
            if (writeStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                writeStatement = GetSession().Prepare(string.Format(InsertEventsQueryTemplate, tableName));
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private PreparedStatement GetReadStatement()
        {
            if (readStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                readStatement = GetSession().Prepare(string.Format(LoadAggregateEventsQueryTemplate, tableName));
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }

        private PreparedStatement GetReplayStatement()
        {
            if (replayStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayStatement = GetSession().Prepare(string.Format(LoadAggregateCommitsQueryTemplate, tableName));
                replayStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayStatement;
        }

        private PreparedStatement GetReplayWithoutDataStatement()
        {
            if (replayWithoutDataStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayWithoutDataStatement = GetSession().Prepare(string.Format(LoadAggregateCommitsQueryWithoutDataTemplate, tableName));
                replayWithoutDataStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayWithoutDataStatement;
        }

        public void Append(AggregateCommitRaw aggregateCommitRaw)
        {
            try
            {
                GetSession()
                    .Execute(GetWriteStatement()
                        .Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Revision, aggregateCommitRaw.Data));
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting an aggregate commit");
            }
        }

        public LoadAggregateCommitsResult LoadAggregateCommits(ReplayOptions replayOptions)
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

            var found = LoadAggregateCommitsMeta(replayOptions.AggregateIds, afterTimestamp, beforeStamp);

            aggregateCommits.AddRange(found);

            //if (replayOptions.AggregateIds.Any() == false)
            //{
            //    IStatement queryStatement = GetReplayStatement().Bind().SetPageSize(pageSize).SetAutoPage(false);

            //    if (pagingInfo.HasToken())
            //        queryStatement.SetPagingState(pagingInfo.Token);

            //    result = GetSession().Execute(queryStatement);
            //    foreach (var row in result.GetRows())
            //    {
            //        var data = row.GetValue<byte[]>("data");
            //        using (var stream = new MemoryStream(data))
            //        {
            //            AggregateCommit commit;
            //            try
            //            {
            //                commit = (AggregateCommit)serializer.Deserialize(stream);
            //            }
            //            catch (Exception ex)
            //            {
            //                string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
            //                logger.ErrorException(ex, () => error);
            //                continue;
            //            }
            //            aggregateCommits.Add(commit);
            //        }
            //    }

            //    if (result.IsFullyFetched == false)
            //    {
            //        logger.Warn(() => "Not implemented logic. => if (result.IsFullyFetched == false)");
            //    }
            //}

            return new LoadAggregateCommitsResult()
            {
                Commits = aggregateCommits,
                PaginationToken = null
            };
        }
    }

    class PagingInfo
    {
        public byte[] Token { get; set; }

        public bool HasMore { get; set; } = true;

        public bool HasToken() => Token is null == false;

        public static PagingInfo From(RowSet result)
        {
            return new PagingInfo()
            {
                HasMore = result.PagingState is null == false,
                Token = result.PagingState
            };
        }

        public override string ToString()
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this)));
        }
    }
}
