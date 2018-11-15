using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSettings : ICassandraEventStoreSettings
    {
        public CassandraEventStoreSettings(BoundedContext boundedContext, ICassandraProvider cassandraProvider, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            BoundedContext = boundedContext;
            CassandraProvider = cassandraProvider;
            TableNameStrategy = tableNameStrategy;
            Serializer = serializer;
        }

        public BoundedContext BoundedContext { get; }
        public ICassandraProvider CassandraProvider { get; }
        public ICassandraEventStoreTableNameStrategy TableNameStrategy { get; }
        public ISerializer Serializer { get; }
    }

    public class CassandraEventStore<TSettings> : CassandraEventStore, IEventStorePlayer<TSettings> where TSettings : class, ICassandraEventStoreSettings
    {
        public CassandraEventStore(TSettings settings)
            : base(settings.BoundedContext, settings.CassandraProvider, settings.TableNameStrategy, settings.Serializer)
        {
        }
    }

    public class CassandraEventStore : IEventStore, IEventStorePlayer
    {
        private static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStore));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";
        private const string LoadAggregateCommitsQueryTemplate = @"SELECT data FROM {0};";

        private readonly BoundedContext boundedContext;
        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;
        private PreparedStatement replayStatement;

        public CassandraEventStore(BoundedContext boundedContext, ICassandraProvider cassandraProvider, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.boundedContext = boundedContext ?? throw new ArgumentNullException(nameof(boundedContext));
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

        public IEnumerable<AggregateCommit> LoadAggregateCommits(int batchSize)
        {
            var queryStatement = GetReplayStatement().Bind().SetPageSize(batchSize);
            var result = session.Execute(queryStatement);
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
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext.Name);
                writeStatement = session.Prepare(string.Format(InsertEventsQueryTemplate, tableName));
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private PreparedStatement GetReadStatement()
        {
            if (readStatement is null)
            {
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext.Name);
                readStatement = session.Prepare(string.Format(LoadAggregateEventsQueryTemplate, tableName));
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }

        private PreparedStatement GetReplayStatement()
        {
            if (replayStatement is null)
            {
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext.Name);
                replayStatement = session.Prepare(string.Format(LoadAggregateCommitsQueryTemplate, tableName));
                replayStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return replayStatement;
        }
    }
}
