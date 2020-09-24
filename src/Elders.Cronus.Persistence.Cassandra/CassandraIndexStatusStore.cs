using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraIndexStatusStore : IIndexStatusStore
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStore));

        private const string Read = @"SELECT status FROM index_status WHERE id = ?;";
        private const string Write = @"INSERT INTO index_status (id,status) VALUES (?,?);";

        private readonly ICassandraProvider cassandraProvider;

        private ISession GetSession() => cassandraProvider.GetSession(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraIndexStatusStore(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
        }

        public IndexStatus Get(string indexId)
        {
            BoundStatement bs = GetSession().Prepare(Read).Bind(indexId);
            var row = GetSession().Execute(bs).GetRows().SingleOrDefault();
            return IndexStatus.Parse(row?.GetValue<string>("status"));
        }

        public void Save(string indexId, IndexStatus status)
        {
            try
            {
                PreparedStatement statement = GetSession().Prepare(Write);
                GetSession().Execute(statement.Bind(indexId, status.ToString()));
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException("[EventStore] Write timeout while persisting in CassandraIndexStatusStore", ex);
            }
        }
    }
}
