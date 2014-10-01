using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventSourcing;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraPersister : IEventStorePersister
    {
        private const string InsertEventsBatchQueryTemplate = @"
BEGIN BATCH
  INSERT INTO {0} (id,ts,rev,data) VALUES (?,?,?,?);
  UPDATE {0}player SET events = events + ? WHERE date=?;
APPLY BATCH;";

        private PreparedStatement insertEventsBatchPreparedStatement;

        private readonly ISerializer serializer;

        private readonly ISession session;

        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        private readonly ConcurrentDictionary<Type, PreparedStatement> persistAggregateEventsPreparedStatements;

        public CassandraPersister(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            this.tableNameStrategy = tableNameStrategy;
            this.session = session;
            this.serializer = serializer;
            this.persistAggregateEventsPreparedStatements = new ConcurrentDictionary<Type, PreparedStatement>();
        }

        public void Persist(List<IAggregateRoot> aggregates)
        {
            foreach (var ar in aggregates)
            {
                AggregateCommit arCommit = new AggregateCommit(ar.State.Id, ar.State.Version, ar.UncommittedEvents);
                byte[] data = SerializeEvent(arCommit);
                session.Execute(GetPreparedStatementToPersistAnAggregate(ar).Bind(arCommit.AggregateId, arCommit.Timestamp, arCommit.Revision, data, new List<byte[]>() { data }, DateTime.FromFileTimeUtc(arCommit.Timestamp).ToString("yyyyMMdd")));
            }
        }

        private PreparedStatement GetPreparedStatementToPersistAnAggregate<AR>(AR aggregate) where AR : IAggregateRoot
        {
            PreparedStatement persistAggregatePreparedStatement;
            Type aggregateType = aggregate.GetType();
            if (!persistAggregateEventsPreparedStatements.TryGetValue(aggregateType, out persistAggregatePreparedStatement))
            {
                string tableName = tableNameStrategy.GetEventsTableName(aggregate);
                persistAggregatePreparedStatement = session.Prepare(String.Format(InsertEventsBatchQueryTemplate, tableName));
                persistAggregateEventsPreparedStatements.TryAdd(aggregateType, persistAggregatePreparedStatement);
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
    }
}