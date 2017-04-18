using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventStore;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStore : IEventStore
    {
        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";

        private const string InsertEventsQueryTemplate = @"INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);";

        private readonly ISerializer serializer;

        private readonly ISession session;

        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private readonly ConsistencyLevel writeConsistencyLevel;

        private readonly ConsistencyLevel readConsistencyLevel;

        private readonly ConcurrentDictionary<string, PreparedStatement> persistAggregateEventsPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> loadAggregateEventsPreparedStatements;

        public CassandraEventStore(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer, ConsistencyLevel writeConsistencyLevel, ConsistencyLevel readConsistencyLevel)
        {
            this.tableNameStrategy = tableNameStrategy;
            this.session = session;
            this.serializer = serializer;
            this.writeConsistencyLevel = writeConsistencyLevel;
            this.readConsistencyLevel = readConsistencyLevel;
            this.persistAggregateEventsPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.loadAggregateEventsPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        private PreparedStatement GetPreparedStatementToPersistAnAggregateCommit(AggregateCommit aggregateCommit)
        {
            PreparedStatement persistAggregatePreparedStatement;
            if (!persistAggregateEventsPreparedStatements.TryGetValue(aggregateCommit.BoundedContext, out persistAggregatePreparedStatement))
            {
                persistAggregatePreparedStatement.SetConsistencyLevel(writeConsistencyLevel);
                string tableName = tableNameStrategy.GetEventsTableName(aggregateCommit);
                persistAggregatePreparedStatement = session.Prepare(String.Format(InsertEventsQueryTemplate, tableName));
                persistAggregateEventsPreparedStatements.TryAdd(aggregateCommit.BoundedContext, persistAggregatePreparedStatement);
            }

            return persistAggregatePreparedStatement;
        }

        private byte[] SerializeEvent(AggregateCommit commit)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, commit);
                return stream.ToArray();
            }
        }

        public void Append(AggregateCommit aggregateCommit)
        {
            byte[] data = SerializeEvent(aggregateCommit);
            session
                .Execute(GetPreparedStatementToPersistAnAggregateCommit(aggregateCommit)
                .Bind(Convert.ToBase64String(aggregateCommit.AggregateRootId), aggregateCommit.Timestamp, aggregateCommit.Revision, data));
        }

        public EventStream Load(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> aggregateCommits = new List<AggregateCommit>();
            string boundedContext = aggregateId.GetType().GetBoundedContext().BoundedContextName;
            BoundStatement bs = GetPreparedStatementToLoadAnAggregateCommit(boundedContext).Bind(Convert.ToBase64String(aggregateId.RawId));
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

        private PreparedStatement GetPreparedStatementToLoadAnAggregateCommit(string boundedContext)
        {
            PreparedStatement loadAggregatePreparedStatement;
            if (!loadAggregateEventsPreparedStatements.TryGetValue(boundedContext, out loadAggregatePreparedStatement))
            {
                loadAggregatePreparedStatement.SetConsistencyLevel(readConsistencyLevel);
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext);
                loadAggregatePreparedStatement = session.Prepare(String.Format(LoadAggregateEventsQueryTemplate, tableName));
                loadAggregateEventsPreparedStatements.TryAdd(boundedContext, loadAggregatePreparedStatement);
            }

            return loadAggregatePreparedStatement;
        }
    }
}
