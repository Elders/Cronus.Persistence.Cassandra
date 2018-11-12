using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStore : IEventStore
    {
        private static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStore));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";
        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";

        private readonly BoundedContext boundedContext;
        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;

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
    }
}
