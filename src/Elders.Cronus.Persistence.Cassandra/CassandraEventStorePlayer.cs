using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.DomainModeling;
using Elders.Cronus.EventStore;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStorePlayer : IEventStorePlayer
    {
        public static log4net.ILog log = log4net.LogManager.GetLogger(typeof(CassandraEventStorePlayer));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT events FROM {0}player WHERE date = ?;";
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly PreparedStatement loadAggregateEventsPreparedStatement;
        public CassandraEventStorePlayer(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy, string boundedContext, ISerializer serializer)
        {
            this.serializer = serializer;
            this.tableNameStrategy = tableNameStrategy;
            this.session = session;
            this.loadAggregateEventsPreparedStatement = session.Prepare(String.Format(LoadAggregateEventsQueryTemplate, tableNameStrategy.GetEventsTableName(boundedContext)));
        }

        public IEnumerable<IEvent> GetEventsFromStart(int batchPerQuery = 100)
        {
            var startDate = new DateTime(2014, 1, 1);
            while (startDate < DateTime.UtcNow.AddDays(1))
            {
                foreach (var item in LoadAggregateCommits(startDate, batchPerQuery))
                {
                    foreach (var evnt in item.Events)
                    {
                        yield return evnt;
                    }
                }
                startDate = startDate.AddDays(1);
            }
        }

        private List<AggregateCommit> LoadAggregateCommits(DateTime date, int batchSize)
        {
            List<AggregateCommit> commits = new List<AggregateCommit>();
            var queryStatement = loadAggregateEventsPreparedStatement.Bind(date.ToString("yyyyMMdd")).SetPageSize(batchSize);
            var result = session.Execute(queryStatement);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<List<byte[]>>("events");
                foreach (var @event in data)
                {
                    using (var stream = new MemoryStream(@event))
                    {
                        AggregateCommit commit;
                        try
                        {
                            commit = (AggregateCommit)serializer.Deserialize(stream);
                        }
                        catch (Exception ex)
                        {
                            string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(@event);
                            log.Error(error, ex);
                            continue;
                        }

                        commits.Add(commit);
                    }
                }
            }
            return commits;
        }
    }
}