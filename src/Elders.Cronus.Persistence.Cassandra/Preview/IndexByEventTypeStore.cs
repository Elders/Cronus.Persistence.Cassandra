﻿using Cassandra;
using Elders.Cronus.EventStore.Index;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class IndexByEventTypeStore : IIndexStore
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(IndexByEventTypeStore));

        private const string Read = @"SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=?;";
        private const string ReadRange = @"SELECT aid,rev,pos,ts FROM index_by_eventtype WHERE et=? AND ts>=? AND ts<=?;";
        private const string Write = @"INSERT INTO index_by_eventtype (et,aid,rev,pos,ts) VALUES (?,?,?,?,?);";

        private PreparedStatement readStatement;
        private PreparedStatement readRangeStatement;
        private PreparedStatement writeStatement;

        private readonly ICassandraProvider cassandraProvider;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public IndexByEventTypeStore(ICassandraProvider cassandraProvider)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
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
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            IStatement countStatement = new SimpleStatement($"SELECT count(*) FROM index_by_eventtype WHERE et='{indexRecordId}'")
                .SetReadTimeoutMillis(1000 * 60 * 10)
                .SetConsistencyLevel(ConsistencyLevel.LocalOne);

            RowSet result = await session.ExecuteAsync(countStatement).ConfigureAwait(false);

            return result.GetRows().First().GetValue<long>("count");
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

        internal async IAsyncEnumerable<IndexRecord> GetRecordsAsync(string indexRecordId, long from, long to, string paginationToken, int pageSize, Action<PagingInfo> onPagingInfoChanged = null, [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            PagingInfo pagingInfo = PagingInfo.Parse(paginationToken);
            while (pagingInfo.HasMore)
            {
                if (onPagingInfoChanged is not null)
                    onPagingInfoChanged(pagingInfo);

                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetReadRangePreparedStatementAsync(session).ConfigureAwait(false);
                IStatement queryStatement = statement.Bind(indexRecordId, from, to).SetPageSize(pageSize).SetAutoPage(false);

                if (pagingInfo.HasToken())
                    queryStatement.SetPagingState(pagingInfo.Token);

                RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
                foreach (var row in result.GetRows())
                {
                    IndexRecord indexRecord = new IndexRecord(indexRecordId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
                    yield return indexRecord;

                    if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested) break;
                }

                pagingInfo = PagingInfo.From(result);
            }
        }
    }
}
