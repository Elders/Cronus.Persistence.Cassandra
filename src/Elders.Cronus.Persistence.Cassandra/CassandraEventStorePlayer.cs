using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Serializer;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStorePlayer : IEventStorePlayer
    {
        public static log4net.ILog log = log4net.LogManager.GetLogger(typeof(CassandraEventStorePlayer));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0};";
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

        public IEnumerable<AggregateCommit> LoadAggregateCommits(int batchSize)
        {
            var queryStatement = loadAggregateEventsPreparedStatement.Bind().SetPageSize(batchSize);
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
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        log.Error(error, ex);
                        continue;
                    }
                    yield return commit;
                }
            }
        }
    }
}