using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSchema : ICassandraEventStoreSchema
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchema));

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string INDEX_REV = @"DROP INDEX IF EXISTS ""{0}_idx_rev""";
        private const string INDEX_POS = @"DROP INDEX IF EXISTS ""{0}_idx_pos""";

        private const string CREATE_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, status text, PRIMARY KEY (id));";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, pid int, aid blob, rev int, pos int, ts bigint, PRIMARY KEY ((et,pid),ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column

        private const string INDEX_STATUS_TABLE_NAME = "index_status";
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)
        public CassandraEventStoreSchema(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
        }

        public Task CreateStorageAsync()
        {
            Task[] createESTasks = new Task[]
            {
                CreateEventsStorageAsync(),
                CreateIndeciesAsync(),
                CreateSnapshotsStorageAsync()
            };

            return Task.WhenAll(createESTasks);
        }

        public Task CreateEventsStorageAsync()
        {
            string tableName = tableNameStrategy.GetName();
            return CreateEventStoragePersistenceAsync(tableName);
        }

        public Task CreateSnapshotsStorageAsync()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
            return Task.CompletedTask;
        }

        public Task CreateIndeciesAsync()
        {
            Task[] createTableTasks = new Task[]
                {
                    CreateTableAsync(CREATE_INDEX_STATUS_TABLE_TEMPLATE, INDEX_STATUS_TABLE_NAME),
                    CreateTableAsync(CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, INDEX_BY_EVENT_TYPE_TABLE_NAME),
                    CreateTableAsync(MessageCounter.CreateTableTemplate, "EventCounter")
                };

            return Task.WhenAll(createTableTasks);
        }

        private async Task CreateTableAsync(string cqlQuery, string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", tableName, session.Cluster.AllHosts().First().Address, session.Keyspace);

                PreparedStatement createEventsTableStatement = await session.PrepareAsync(string.Format(cqlQuery, tableName).ToLower()).ConfigureAwait(false);
                createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", tableName, session.Keyspace);
            }
            catch (Exception)
            {
                throw;
            }
        }

        private async Task CreateEventStoragePersistenceAsync(string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", tableName, session.Cluster.AllHosts().First().Address, session.Keyspace);

                string tableQuery = string.Format(CREATE_EVENTS_TABLE_TEMPLATE, tableName).ToLower();
                string rev = string.Format(INDEX_REV, tableName).ToLower();
                string pos = string.Format(INDEX_POS, tableName).ToLower();


                PreparedStatement tableStatement = await session.PrepareAsync(string.Format(tableQuery, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                PreparedStatement revStatement = await session.PrepareAsync(string.Format(rev, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                PreparedStatement posStatement = await session.PrepareAsync(string.Format(pos, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(tableStatement.Bind()).ConfigureAwait(false);
                await session.ExecuteAsync(revStatement.Bind()).ConfigureAwait(false);
                await session.ExecuteAsync(posStatement.Bind()).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", tableName, session.Keyspace);
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
