using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventSourcing;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStorePlayer : IEventStorePlayer
    {
        private const string LoadAggregateEventsQueryTemplate = @"SELECT events FROM {0}player WHERE date = ?;";
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly PreparedStatement loadAggregateEventsPreparedStatement;
        public CassandraEventStorePlayer(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            this.serializer = serializer;
            this.tableNameStrategy = tableNameStrategy;
            this.session = session;
            //this.loadAggregateEventsPreparedStatement = session.Prepare(String.Format(LoadAggregateEventsQueryTemplate, tableNameStrategy.GetEventsTableName()));
        }

        private List<AggregateCommit> LoadAggregateCommits()
        {
            List<AggregateCommit> events = new List<AggregateCommit>();
            BoundStatement bs = loadAggregateEventsPreparedStatement.Bind("20140917");
            var result = session.Execute(bs);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<List<byte[]>>("events");
                foreach (var @event in data)
                {
                    using (var stream = new MemoryStream(@event))
                    {
                        events.Add((AggregateCommit)serializer.Deserialize(stream));
                    }
                }
            }
            return events;
        }

        public IEnumerable<IEvent> GetEventsFromStart(int batchPerQuery = 1)
        {
            foreach (var item in LoadAggregateCommits())
            {
                foreach (var evnt in item.Events)
                {
                    yield return evnt;
                }

            }
        }
    }
}