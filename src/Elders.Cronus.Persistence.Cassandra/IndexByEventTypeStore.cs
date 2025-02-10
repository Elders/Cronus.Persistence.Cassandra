using System;
using System.Collections.Concurrent;
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
    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public class IndexByEventTypeStore : IIndexStore
    {
        private const string Read = @"SELECT aid,rev,pos,ts FROM {0}.index_by_eventtype WHERE et=? AND pid=?;";
        private const string ReadRange = @"SELECT aid,rev,pos,ts FROM {0}.index_by_eventtype WHERE et=? AND ts>=? AND ts<=? ALLOW FILTERING;";
        private const string Write = @"INSERT INTO {0}.index_by_eventtype (et,pid,aid,rev,pos,ts) VALUES (?,?,?,?,?,?);";
        private const string Delete = @"DELETE FROM {0}.index_by_eventtype where et=? AND pid=? AND ts=? AND aid=? AND rev=? AND pos=?;";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<IndexByEventTypeStore> logger;

        private PreparedStatement _readPreparedStatements;
        private PreparedStatement _readRangePreparedStatements;
        private PreparedStatement _writePreparedStatements;
        private PreparedStatement _deletePreparedStatements;

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
                int partitionId = CalculatePartition(record.TimeStamp);

                var bs = statement.Bind(record.Id, partitionId, record.AggregateRootId, record.Revision, record.Position, record.TimeStamp).SetIdempotence(true);
                await session.ExecuteAsync(bs).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex) when (True(() => logger.LogWarning(ex, "Write timeout while persisting in IndexByEventTypeStore"))) { }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to write index record.");
                throw;
            }
        }

        public async Task DeleteAsync(IndexRecord record)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement statement = await GetDeletePreparedStatementAsync(session).ConfigureAwait(false);

                var partitionId = CalculatePartition(record.TimeStamp);
                var bs = statement.Bind(record.Id, partitionId, record.TimeStamp, record.AggregateRootId, record.Revision, record.Position);
                await session.ExecuteAsync(bs).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.LogWarning(ex, "Delete timeout while deleting from IndexByEventTypeStore");
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to delete index record.");
                throw;
            }
        }

        public static int CalculatePartition(long filetimeUtc)
        {
            DateTime timestamp = DateTime.FromFileTimeUtc(filetimeUtc);

            return CalculatePartition(timestamp);
        }

        public static int CalculatePartition(DateTime filetimeUtc)
        {
            int day = filetimeUtc.DayOfYear;
            int partitionId = filetimeUtc.Year * 1000 + day;

            return partitionId;
        }

        private async Task<PreparedStatement> GetWritePreparedStatementAsync(ISession session)
        {
            if (_writePreparedStatements is null)
            {
                _writePreparedStatements = await session.PrepareAsync(string.Format(Write, session.Keyspace)).ConfigureAwait(false);
                _writePreparedStatements.SetConsistencyLevel(ConsistencyLevel.Any);
            }
            return _writePreparedStatements;
        }

        private async Task<PreparedStatement> GetDeletePreparedStatementAsync(ISession session)
        {
            if (_deletePreparedStatements is null)
            {
                _deletePreparedStatements = await session.PrepareAsync(string.Format(Delete, session.Keyspace)).ConfigureAwait(false);
                _deletePreparedStatements.SetConsistencyLevel(ConsistencyLevel.Any);
            }
            return _deletePreparedStatements;
        }

        private async Task<PreparedStatement> GetReadPreparedStatementAsync(ISession session)
        {
            if (_readPreparedStatements is null)
            {
                _readPreparedStatements = await session.PrepareAsync(string.Format(Read, session.Keyspace)).ConfigureAwait(false);
                _readPreparedStatements.SetConsistencyLevel(ConsistencyLevel.One);
            }
            return _readPreparedStatements;
        }

        private async Task<PreparedStatement> GetReadRangePreparedStatementAsync(ISession session)
        {
            if (_readRangePreparedStatements is null)
            {
                _readRangePreparedStatements = await session.PrepareAsync(string.Format(ReadRange, session.Keyspace)).ConfigureAwait(false);
                _readRangePreparedStatements.SetConsistencyLevel(ConsistencyLevel.One);
            }
            return _readRangePreparedStatements;
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

                if (logger.IsEnabled(LogLevel.Information))
                    logger.LogInformation("Number of messages for {indexRecordId}:{count}", indexRecordId, count);

                return count;
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to count number of messages for {indexRecordId}.", indexRecordId)))
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
                logger.LogWarning("Not implemented logic. => if (result.IsFullyFetched == false)");
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
                logger.LogWarning("The PlayerOptions did not specify what EventTypeId should be replayed. Exiting...");
                yield break;
            }

            PagingInfo pagingInfo = PagingInfo.Parse(replayOptions.PaginationToken);
            long after = replayOptions.After.HasValue ? replayOptions.After.Value.ToFileTime() : new PlayerOptions().After.Value.ToFileTime();
            long before = replayOptions.Before.HasValue ? replayOptions.Before.Value.ToFileTime() : DateTimeOffset.UtcNow.AddDays(1).ToFileTime();

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await GetReadRangePreparedStatementAsync(session).ConfigureAwait(false);

            IStatement queryStatement = statement.Bind(replayOptions.EventTypeId, after, before);
            queryStatement
                .SetPageSize(replayOptions.BatchSize)
                .SetAutoPage(false);

            while (pagingInfo.HasMore)
            {
                if (pagingInfo.HasToken())// eh
                    queryStatement.SetPagingState(pagingInfo.Token);

                RowSet result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);

                foreach (var row in result)
                {
                    IndexRecord indexRecord = new IndexRecord(replayOptions.EventTypeId, row.GetValue<byte[]>("aid"), row.GetValue<int>("rev"), row.GetValue<int>("pos"), row.GetValue<long>("ts"));
                    yield return indexRecord;

                    if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested)
                        break;
                }

                if (cancellationToken.CanBeCanceled && cancellationToken.IsCancellationRequested)
                    break;

                pagingInfo = PagingInfo.From(result);

                if (onPagingInfoChanged is not null)
                {
                    try { await onPagingInfoChanged(replayOptions.WithPaginationToken(pagingInfo.ToString())).ConfigureAwait(false); }
                    catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to execute onPagingInfoChanged() function."))) { }
                }
            }
        }
    }
}
