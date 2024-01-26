using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class IndexByEventTypeStore : IIndexStore
    {
        private const string Read = @"SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=?;";
        private const string ReadRange = @"SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=? AND ts>=? AND ts<=?;";
        private const string Write = @"INSERT INTO index_by_eventtype (et,aid,rev,pos,ts) VALUES (?,?,?,?,?);";

        private PreparedStatement readStatement;
        private PreparedStatement readRangeStatement;
        private PreparedStatement writeStatement;

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<IndexByEventTypeStore> logger;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public IndexByEventTypeStore(ICassandraProvider cassandraProvider, ILogger<IndexByEventTypeStore> logger)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public async Task ApendAsync(IndexRecord record)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetWritePreparedStatementAsync(session).ConfigureAwait(false);

                var bs = statement.Bind(record.Id, record.AggregateRootId, record.Revision, record.Position, record.TimeStamp).SetIdempotence(true);
                await session.ExecuteAsync(bs).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting in IndexByEventTypeStore");
            }
        }

        private async Task<PreparedStatement> GetWritePreparedStatementAsync(ISession session)
        {
            if (writeStatement is null)
            {
                writeStatement = await session.PrepareAsync(Write).ConfigureAwait(false);
                writeStatement.SetConsistencyLevel(ConsistencyLevel.Any);
            }

            return writeStatement;
        }

        private async Task<PreparedStatement> GetReadPreparedStatementAsync(ISession session)
        {
            if (readStatement is null)
            {
                readStatement = await session.PrepareAsync(Read).ConfigureAwait(false);
                readStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }

            return readStatement;
        }

        private async Task<PreparedStatement> GetReadRangePreparedStatementAsync(ISession session)
        {
            if (readRangeStatement is null)
            {
                readRangeStatement = await session.PrepareAsync(ReadRange).ConfigureAwait(false);
                readRangeStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }

            return readRangeStatement;
        }

        public async Task<long> GetCountAsync(string indexRecordId)
        {
            try
            {
                ISession session = await (cassandraProvider as CassandraProvider).GetSessionHighTimeoutAsync();

                IStatement countStatement = new SimpleStatement($"SELECT count(*) FROM index_by_eventtype WHERE et='{indexRecordId}'")
                    .SetConsistencyLevel(ConsistencyLevel.One)
                    .SetReadTimeoutMillis(1000 * 60 * 10);

                RowSet result = await session.ExecuteAsync(countStatement).ConfigureAwait(false);

                long count = result.GetRows().First().GetValue<long>("count");

                logger.Info(() => $"Number of messages for {indexRecordId}: {count}");

                return count;
            }
            catch (Exception ex) when (logger.ErrorException(ex, () => $"Failed to count number of messages for {indexRecordId}."))
            {
                return 0;
            }
        }

        public async IAsyncEnumerable<IndexRecord> GetAsync(string indexRecordId)
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadPreparedStatementAsync(session).ConfigureAwait(false);

            BoundStatement bs = statement.Bind(indexRecordId);
            RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                yield return new IndexRecord(indexRecordId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
            }
        }

        public async Task<LoadIndexRecordsResult> GetAsync(string indexRecordId, string paginationToken, int pageSize)
        {
            PagingInfo pagingInfo = PagingInfo.Parse(paginationToken);
            if (pagingInfo.HasMore == false)
                return new LoadIndexRecordsResult() { PaginationToken = paginationToken };

            List<IndexRecord> indexRecords = new List<IndexRecord>();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadPreparedStatementAsync(session).ConfigureAwait(false);
            IStatement queryStatement = statement.Bind(indexRecordId).SetPageSize(pageSize).SetAutoPage(false);

            if (pagingInfo.HasToken())
                queryStatement.SetPagingState(pagingInfo.Token);

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

        internal async IAsyncEnumerable<IndexRecord> GetRecordsAsync(PlayerOptions replayOptions, Func<PlayerOptions, Task> onPagingInfoChanged = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            if (replayOptions.EventTypeId is null)
            {
                logger.Warn(() => "The PlayerOptions did not specify what EventTypeId should be replayed. Exiting...");
                yield break;
            }

            PagingInfo pagingInfo = PagingInfo.Parse(replayOptions.PaginationToken);
            long after = replayOptions.After.HasValue ? replayOptions.After.Value.ToFileTime() : 0;
            long before = replayOptions.Before.HasValue ? replayOptions.Before.Value.ToFileTime() : 0;

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadRangePreparedStatementAsync(session).ConfigureAwait(false);

            IStatement queryStatement = statement.Bind(replayOptions.EventTypeId, after, before);
            queryStatement
                .SetPageSize(replayOptions.BatchSize)
                .SetAutoPage(false);

            while (pagingInfo.HasMore)
            {
                if (pagingInfo.HasToken())
                    queryStatement.SetPagingState(pagingInfo.Token);

                RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);

                foreach (var row in result.GetRows())
                {
                    IndexRecord indexRecord = new IndexRecord(replayOptions.EventTypeId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
                    yield return indexRecord;

                    if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested) break;
                }

                PagingInfo nextPagingInfo = PagingInfo.From(result);

                bool isFirstTime = pagingInfo.Token is null;
                bool hasMoreRecords = result.PagingState is not null;

                bool weHaveNewPagingState = isFirstTime && hasMoreRecords || isFirstTime == false && hasMoreRecords && pagingInfo.Token.AsSpan().SequenceEqual(nextPagingInfo.Token) == false;
                pagingInfo = nextPagingInfo;
                if (onPagingInfoChanged is not null && weHaveNewPagingState)
                {
                    try { await onPagingInfoChanged(replayOptions.WithPaginationToken(pagingInfo.ToString())).ConfigureAwait(false); }
                    catch (Exception ex) when (logger.ErrorException(ex, () => "Failed to execute onPagingInfoChanged() function.")) { }
                }
            }
        }
    }
}
