using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStore : IEventStore
    {
        static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStore));

        private readonly string boundedContext;

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";

        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";

        private readonly ISerializer serializer;
        private readonly IConfiguration configuration;
        private readonly ISession session;

        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private PreparedStatement writeStatement;
        private PreparedStatement readStatement;

        public CassandraEventStore(IConfiguration configuration, ICassandraProvider cassandraProvider, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            string boundedContext = configuration["cronus_boundedcontext"];
            if (string.IsNullOrEmpty(boundedContext)) throw new ArgumentException("Missing setting: cronus_boundedcontext");
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));

            this.tableNameStrategy = tableNameStrategy;
            this.boundedContext = boundedContext;
            this.configuration = configuration;
            this.session = cassandraProvider.GetSession();
            this.serializer = serializer;
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
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext);
                writeStatement = session.Prepare(string.Format(InsertEventsQueryTemplate, tableName));
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }

        private PreparedStatement GetReadStatement()
        {
            if (readStatement is null)
            {
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext);
                readStatement = session.Prepare(string.Format(LoadAggregateEventsQueryTemplate, tableName));
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }
    }
}
