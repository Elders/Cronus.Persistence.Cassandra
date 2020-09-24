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

        private readonly ICassandraProvider cassandraProvider;

        private ISession GetSession() => cassandraProvider.GetSession(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public IndexByEventTypeStore(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
        }

        public void Apend(IEnumerable<IndexRecord> indexRecords)
        {
            try
            {
                PreparedStatement statement = GetSession().Prepare(Write);

                foreach (var record in indexRecords)
                {
                    GetSession().Execute(statement.Bind(record.Id, Convert.ToBase64String(record.AggregateRootId)));
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
            BoundStatement bs = GetSession().Prepare(Read).Bind(indexRecordId);
            var result = GetSession().Execute(bs);
            foreach (var row in result.GetRows())
            {
                yield return new IndexRecord(indexRecordId, Convert.FromBase64String(row.GetValue<string>("aid")));
            }
        }
    }
}
