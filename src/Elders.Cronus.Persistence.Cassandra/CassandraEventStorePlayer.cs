using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStorePlayer : IEventStorePlayer
    {
        static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStorePlayer));

        private const string LoadAggregateEventsQueryTemplate = @"SELECT data FROM {0};";
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ISerializer serializer;
        private readonly ISession session;
        private readonly PreparedStatement loadAggregateEventsPreparedStatement;

        public CassandraEventStorePlayer(BoundedContext boundedContext, ICassandraProvider cassandraProvider, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            if (boundedContext is null) throw new ArgumentNullException(nameof(boundedContext));
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.serializer = serializer ?? throw new ArgumentNullException(nameof(serializer));
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy)); ;
            this.session = cassandraProvider.GetSession();
            this.loadAggregateEventsPreparedStatement = session.Prepare(string.Format(LoadAggregateEventsQueryTemplate, tableNameStrategy.GetEventsTableName(boundedContext.Name)));
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
                        string error = "[EventStore] Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        log.ErrorException(error, ex);
                        continue;
                    }
                    yield return commit;
                }
            }
        }
    }
}
