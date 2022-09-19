using Cassandra;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class IndexByEventTypeStore : IIndexStore
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(IndexByEventTypeStore));

        private const string Read = @"SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et = ?;";
        private const string Write = @"INSERT INTO index_by_eventtype (et,aid,rev,pos,ts) VALUES (?,?,?,?,?);";

        const int MaxConcurrencyLevel = 16;

        private PreparedStatement readStatement;
        private PreparedStatement writeStatement;

        private readonly ICassandraProvider cassandraProvider;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public IndexByEventTypeStore(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
        }

        public async Task ApendAsync(IEnumerable<IndexRecord> indexRecords)
        {
            try
            {
                PreparedStatement statement = await GetWritePreparedStatementAsync().ConfigureAwait(false);
                var session = await GetSessionAsync().ConfigureAwait(false);

                int totalLength = indexRecords.Count();
                var concurrencyLevel = MaxConcurrencyLevel >= totalLength ? totalLength : MaxConcurrencyLevel;

                int maxCount = (int)Math.Ceiling(totalLength / (double)concurrencyLevel);   // Compute operations per Task (rounded up, so the first tasks will process more operations)
                List<Task> tasks = new List<Task>(concurrencyLevel);   // The maximum amount of async executions that are going to be launched in parallel at any given time

                var skip = 0;
                while (skip < totalLength)
                {
                    var take = maxCount;
                    if (skip + maxCount > totalLength)
                        take = (totalLength - skip);

                    tasks.Add(ExecuteOneAtATimeAsync(session, statement, indexRecords.Skip(skip).Take(take)));
                    skip += take;
                }

                Task.WhenAll(tasks).ConfigureAwait(false).GetAwaiter().GetResult();

            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting in IndexByEventTypeStore");
            }
        }

        private async Task ExecuteOneAtATimeAsync(ISession session, PreparedStatement preparedStatement, IEnumerable<IndexRecord> indexRecords)
        {
            foreach (IndexRecord record in indexRecords)
            {
                byte[] arId = record.AggregateRootId;
                var bs = preparedStatement.Bind(record.Id, arId, record.Revision, record.Position, record.TimeStamp).SetIdempotence(true);
                await session.ExecuteAsync(bs).ConfigureAwait(false);
            }
        }

        private async Task<PreparedStatement> GetWritePreparedStatementAsync()
        {
            if (writeStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                writeStatement = await session.PrepareAsync(Write).ConfigureAwait(false);
                writeStatement.SetConsistencyLevel(ConsistencyLevel.Any);
            }

            return writeStatement;
        }

        private async Task<PreparedStatement> GetReadPreparedStatementAsync()
        {
            if (readStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                readStatement = await session.PrepareAsync(Read).ConfigureAwait(false);
                readStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }

            return readStatement;
        }

        public async IAsyncEnumerable<IndexRecord> GetAsync(string indexRecordId)
        {
            PreparedStatement statement = await GetReadPreparedStatementAsync().ConfigureAwait(false);

            BoundStatement bs = statement.Bind(indexRecordId);
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                yield return new IndexRecord(indexRecordId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
            }
        }

        public async Task<LoadIndexRecordsResult> GetAsync(string indexRecordId, string paginationToken, int pageSize)
        {
            PagingInfo pagingInfo = GetPagingInfo(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadIndexRecordsResult() { PaginationToken = paginationToken };

            List<IndexRecord> indexRecords = new List<IndexRecord>();

            PreparedStatement statement = await GetReadPreparedStatementAsync().ConfigureAwait(false);
            IStatement queryStatement = statement.Bind(indexRecordId).SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                IndexRecord indexRecord = new IndexRecord(indexRecordId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
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
