using System;
using System.Collections.Generic;
using System.IO;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;
using Microsoft.Extensions.Configuration;

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

        public CassandraEventStorePlayer(IConfiguration configuration, ICassandraProvider cassandraProvider, ICassandraEventStoreTableNameStrategy tableNameStrategy, ISerializer serializer)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            string boundedContext = configuration["cronus_boundedcontext"];
            if (string.IsNullOrEmpty(boundedContext)) throw new ArgumentException("Missing setting: cronus_boundedcontext");
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));
            if (serializer is null) throw new ArgumentNullException(nameof(serializer));

            this.serializer = serializer;
            this.tableNameStrategy = tableNameStrategy;
            this.session = cassandraProvider.GetSession();
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
