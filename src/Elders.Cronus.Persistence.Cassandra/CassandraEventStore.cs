using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;

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
        private static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStore));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";
        private const string LoadAggregateCommitsQueryTemplate = @"SELECT id,ts,rev,data FROM {0};";

        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly ITableNamingStrategy tableNameStrategy;

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;
        private PreparedStatement replayStatement;

        public CassandraEventStore(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISerializer serializer)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.session = cassandraProvider.GetSession();
            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer)); ;
        }

        public void Append(AggregateCommit aggregateCommit)
        {
            byte[] data = SerializeEvent(aggregateCommit);

            try
            {
                session
                    .Execute(GetWriteStatement()
                        .Bind(Convert.ToBase64String(aggregateCommit.AggregateRootId), aggregateCommit.Timestamp, aggregateCommit.Revision, data));
            }
            catch (WriteTimeoutException ex)
            {
                log.WarnException("[EventStore] Write timeout while persisting an aggregate commit", ex);
            }
        }

        public EventStream Load(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();
            BoundStatement bs = GetReadStatement().Bind(Convert.ToBase64String(aggregateId.RawId));
            var result = session.Execute(bs);
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

        public LoadAggregateCommitsResult LoadAggregateCommits(string paginationToken, int batchSize = 5000)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();

            IStatement queryStatement = GetReplayStatement().Bind().SetPageSize(batchSize);
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.IsFullyFetched)
                return new LoadAggregateCommitsResult() { PaginationToken = pagingInfo.ToString() };

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            RowSet result = session.Execute(queryStatement);
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
                        log.ErrorException(error, ex);
                        continue;
                    }
                    aggregateCommits.Add(commit);
                }
            }

            return new LoadAggregateCommitsResult()
            {
                Commits = aggregateCommits,
                PaginationToken = PagingInfo.From(result).ToString()
            };
        }

        public IEnumerable<AggregateCommit> LoadAggregateCommits(int batchSize)
        {
            return LoadAggregateCommitsAsync().Wait().Result;
        }

        public async IAsyncEnumerable<AggregateCommit> LoadAggregateCommitsAsync()
        {
            var queryStatement = GetReplayStatement().Bind();
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
                        log.ErrorException(error, ex);
                        continue;
                    }

                    yield return commit;
                }
            }
        }

        public IEnumerable<AggregateCommitRaw> LoadAggregateCommitsRaw(int batchSize = 5000)
        {
            return LoadAggregateCommitsRawAsync().Wait().Result;
        }

        public async IAsyncEnumerable<AggregateCommitRaw> LoadAggregateCommitsRawAsync()
        {
            var queryStatement = GetReplayStatement().Bind();
            var result = await session.ExecuteAsync(queryStatement);
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
                writeStatement = session.Prepare(string.Format(InsertEventsQueryTemplate, tableName));
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private PreparedStatement GetReadStatement()
        {
            if (readStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                readStatement = session.Prepare(string.Format(LoadAggregateEventsQueryTemplate, tableName));
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }

        private PreparedStatement GetReplayStatement()
        {
            if (replayStatement is null)
            {
                string tableName = tableNameStrategy.GetName();
                replayStatement = session.Prepare(string.Format(LoadAggregateCommitsQueryTemplate, tableName));
                replayStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayStatement;
        }

        public void Append(AggregateCommitRaw aggregateCommitRaw)
        {
            try
            {
                session
                    .Execute(GetWriteStatement()
                        .Bind(aggregateCommitRaw.AggregateRootId, aggregateCommitRaw.Timestamp, aggregateCommitRaw.Revision, aggregateCommitRaw.Data));
            }
            catch (WriteTimeoutException ex)
            {
                log.WarnException("[EventStore] Write timeout while persisting an aggregate commit", ex);
            }
        }
    }

    class PagingInfo
    {
        public byte[] Token { get; set; }

        public bool IsFullyFetched { get; set; }

        public bool HasToken() => Token is null == false;

        public static PagingInfo From(RowSet result)
        {
            return new PagingInfo()
            {
                IsFullyFetched = result.IsFullyFetched,
                Token = result.PagingState
            };
        }

        public override string ToString()
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this)));
        }
    }
}
