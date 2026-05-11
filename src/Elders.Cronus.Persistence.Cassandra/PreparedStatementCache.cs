using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.MessageProcessing;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal abstract class PreparedStatementCache
    {
        private readonly ICronusContextAccessor _context;
        private readonly ICassandraProvider _cassandraProvider;
        private readonly ITableNamingStrategy _tableNameStrategy;
        private readonly Dictionary<string, PreparedStatement> _tenantCache;
        private readonly SemaphoreSlim _threadGate = new SemaphoreSlim(1);

        protected PreparedStatementCache(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : this(context, cassandraProvider, default) { }

        protected PreparedStatementCache(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy)
        {
            _tenantCache = new Dictionary<string, PreparedStatement>();

            _context = cronusContextAccessor ?? throw new ArgumentNullException(nameof(cronusContextAccessor));
            _cassandraProvider = cassandraProvider ?? throw new ArgumentNullException(nameof(cassandraProvider));
            _tableNameStrategy = tableNameStrategy; // allows null/default
        }

        protected abstract string GetQueryTemplate();

        protected virtual string GetTableName() => _tableNameStrategy?.GetName();

        protected virtual void SetPreparedStatementOptions(PreparedStatement statement)
        {
            statement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
        }

        internal async Task<PreparedStatement> PrepareAsync(ISession session)
        {
            try
            {
                if (_tenantCache.TryGetValue(_context.CronusContext.Tenant, out var preparedStatement))
                    return preparedStatement;

                await _threadGate.WaitAsync(10000).ConfigureAwait(false);
                if (_tenantCache.TryGetValue(_context.CronusContext.Tenant, out preparedStatement))
                    return preparedStatement;

                string keyspace = _cassandraProvider.GetKeyspace();
                string tableName = GetTableName();
                string template = GetQueryTemplate();

                if (string.IsNullOrEmpty(keyspace)) throw new Exception($"Invalid keyspace while preparing query template: {template}");
                if (_tableNameStrategy is not null && string.IsNullOrEmpty(tableName)) throw new Exception($"Invalid table name while preparing query template: {template}");

                string query = string.Format(template, keyspace, tableName);

                preparedStatement = await session.PrepareAsync(query).ConfigureAwait(false);
                SetPreparedStatementOptions(preparedStatement);

                _tenantCache.TryAdd(_context.CronusContext.Tenant, preparedStatement);

                return preparedStatement;
            }
            catch (Exception ex)
            {
                throw new Exception($"Failed to prepare query statement for {GetType().Name}", ex);
            }
            finally
            {
                _threadGate?.Release();
            }
        }
    }
}
