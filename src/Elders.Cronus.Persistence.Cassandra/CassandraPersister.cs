using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventStore;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraPersister : IEventStorePersister
    {
        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0} WHERE id = ?;";

        private const string InsertEventsBatchQueryTemplate = @"
BEGIN BATCH
  INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);
  UPDATE {0}player SET events = events + ? WHERE date=?;
APPLY BATCH;";

        private PreparedStatement insertEventsBatchPreparedStatement;

        private readonly ISerializer serializer;

        private readonly ISession session;

        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private readonly ConcurrentDictionary<string, PreparedStatement> persistAggregateEventsPreparedStatements;
        private readonly ConcurrentDictionary<string, PreparedStatement> loadAggregateEventsPreparedStatements;

        public CassandraPersister(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            this.tableNameStrategy = tableNameStrategy;
            this.session = session;
            this.serializer = serializer;
            this.persistAggregateEventsPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
            this.loadAggregateEventsPreparedStatements = new ConcurrentDictionary<string, PreparedStatement>();
        }

        private PreparedStatement GetPreparedStatementToPersistAnAggregateCommit(AggregateCommit aggregateCommit)
        {
            PreparedStatement persistAggregatePreparedStatement;
            if (!persistAggregateEventsPreparedStatements.TryGetValue(aggregateCommit.BoundedContext, out persistAggregatePreparedStatement))
            {
                string tableName = tableNameStrategy.GetEventsTableName(aggregateCommit);
                persistAggregatePreparedStatement = session.Prepare(String.Format(InsertEventsBatchQueryTemplate, tableName));
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

        public void Persist(AggregateCommit aggregateCommit)
        {
            byte[] data = SerializeEvent(aggregateCommit);
            session.Execute(GetPreparedStatementToPersistAnAggregateCommit(aggregateCommit).Bind(Convert.ToBase64String(aggregateCommit.AggregateId), aggregateCommit.Timestamp, aggregateCommit.Revision, data, new List<byte[]>() { data }, DateTime.FromFileTimeUtc(aggregateCommit.Timestamp).ToString("yyyyMMdd")));
        }

        public List<AggregateCommit> Load(IAggregateRootId aggregateId)
        {
            List<AggregateCommit> events = new List<AggregateCommit>();
            string boundedContext = aggregateId.GetType().GetBoundedContext().BoundedContextName;
            BoundStatement bs = GetPreparedStatementToLoadAnAggregateCommit(boundedContext).Bind(Convert.ToBase64String(aggregateId.RawId));
            var result = session.Execute(bs);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    events.Add((AggregateCommit)serializer.Deserialize(stream));
                }
            }
            return events;
        }

        private PreparedStatement GetPreparedStatementToLoadAnAggregateCommit(string boundedContext)
        {
            PreparedStatement loadAggregatePreparedStatement;
            if (!loadAggregateEventsPreparedStatements.TryGetValue(boundedContext, out loadAggregatePreparedStatement))
            {
                string tableName = tableNameStrategy.GetEventsTableName(boundedContext);
                loadAggregatePreparedStatement = session.Prepare(String.Format(LoadAggregateEventsQueryTemplate, tableName));
                loadAggregateEventsPreparedStatements.TryAdd(boundedContext, loadAggregatePreparedStatement);
            }

            return loadAggregatePreparedStatement;
        }
    }
}