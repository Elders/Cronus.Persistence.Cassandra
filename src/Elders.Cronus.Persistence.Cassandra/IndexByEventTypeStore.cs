using System;
using System.Collections.Generic;
using Cassandra;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class IndexByEventTypeStore : IIndexStore
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(IndexByEventTypeStore));

        private const string Read = @"SELECT aid FROM index_by_eventtype WHERE et = ?;";
        private const string Write = @"INSERT INTO index_by_eventtype (et,aid) VALUES (?,?);";

        private readonly ISession session;

        public IndexByEventTypeStore(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.session = cassandraProvider.GetSession();
        }

        public void Apend(IEnumerable<IndexRecord> indexRecords)
        {
            try
            {
                PreparedStatement statement = session.Prepare(Write);

                foreach (var record in indexRecords)
                {
                    session.Execute(statement.Bind(record.Id, Convert.ToBase64String(record.AggregateRootId)));
                }

            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException("[EventStore] Write timeout while persisting in IndexByEventTypeStore", ex);
            }
        }

        public IEnumerable<IndexRecord> Get(string indexRecordId)
        {
            List<IndexRecord> indexRecords = new List<IndexRecord>();
            BoundStatement bs = session.Prepare(Read).Bind(indexRecordId);
            var result = session.Execute(bs);
            foreach (var row in result.GetRows())
            {
                yield return new IndexRecord(indexRecordId, Convert.FromBase64String(row.GetValue<string>("aid")));
            }
        }
    }
}
