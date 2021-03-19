using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
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

        private PreparedStatement readStatement;
        private PreparedStatement writeStatement;

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
                PreparedStatement statement = GetWritePreparedStatement();

                foreach (var record in indexRecords)
                {
                    GetSession().Execute(statement.Bind(record.Id, Convert.ToBase64String(record.AggregateRootId)));
                }

            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting in IndexByEventTypeStore");
            }
        }

        private PreparedStatement GetWritePreparedStatement()
        {
            if (writeStatement is null)
            {
                writeStatement = GetSession()
                    .Prepare(Write)
                    .SetConsistencyLevel(ConsistencyLevel.Any);
            }

            return writeStatement;
        }

        private PreparedStatement GetReadPreparedStatement()
        {
            if (readStatement is null)
            {
                readStatement = GetSession()
                    .Prepare(Read)
                    .SetConsistencyLevel(ConsistencyLevel.One);
            }

            return readStatement;
        }

        public IEnumerable<IndexRecord> Get(string indexRecordId)
        {
            PreparedStatement statement = GetReadPreparedStatement();

            BoundStatement bs = statement.Bind(indexRecordId);
            var result = GetSession().Execute(bs);
            foreach (var row in result.GetRows())
            {
                yield return new IndexRecord(indexRecordId, Convert.FromBase64String(row.GetValue<string>("aid")));
            }
        }

        public LoadIndexRecordsResult Get(string indexRecordId, string paginationToken, int pageSize)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadIndexRecordsResult() { PaginationToken = paginationToken };

            List<IndexRecord> indexRecords = new List<IndexRecord>();

            PreparedStatement statement = GetReadPreparedStatement();
            IStatement queryStatement = statement.Bind(indexRecordId).SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            RowSet result = GetSession().Execute(queryStatement);
            foreach (var row in result.GetRows())
            {
                var indexRecord = new IndexRecord(indexRecordId, Convert.FromBase64String(row.GetValue<string>("aid")));
                indexRecords.Add(indexRecord);
            }

            if (result.IsFullyFetched == false)
            {
                logger.Warn(() => "Not implemented logic. => if (result.IsFullyFetched == false)");
            }

            return new LoadIndexRecordsResult()
            {
                Records = indexRecords,
                PaginationToken = PagingInfo.From(result).ToString()
            };
        }

        private PagingInfo GetPagingInfo(string paginationToken)
        {
            PagingInfo pagingInfo = new PagingInfo();
            if (string.IsNullOrEmpty(paginationToken) == false)
            {
                string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
                pagingInfo = JsonSerializer.Deserialize<PagingInfo>(paginationJson);
            }
            return pagingInfo;
        }
    }
}
