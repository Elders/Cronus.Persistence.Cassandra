using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Persistence.Cassandra.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraIndexStatusStore : IIndexStatusStore
    {
        private static readonly ILog log = LogProvider.GetLogger(typeof(CassandraEventStore));

        private const string Read = @"SELECT status FROM index_status WHERE id = ?;";
        private const string Write = @"INSERT INTO index_status (id,status) VALUES (?,?);";

        private readonly ISession session;

        public CassandraIndexStatusStore(ISession session)
        {
            if (session is null) throw new ArgumentNullException(nameof(session));

            this.session = session;
        }

        public IndexStatus Get(string indexId)
        {
            BoundStatement bs = session.Prepare(Read).Bind(indexId);
            var row = session.Execute(bs).GetRows().SingleOrDefault();
            return IndexStatus.Parse(row?.GetValue<string>("status"));
        }

        public void Save(string indexId, IndexStatus status)
        {
            try
            {
                PreparedStatement statement = session.Prepare(Write);
                session.Execute(statement.Bind(indexId, status.ToString()));
            }
            catch (WriteTimeoutException ex)
            {
                log.WarnException("Write timeout while persisting in CassandraIndexStatusStore", ex);
            }
        }
    }
}
