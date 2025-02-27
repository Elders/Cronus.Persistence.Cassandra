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

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS {0}.{1} (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS {0}.{1} (et text, pid int, aid blob, rev int, pos int, ts bigint, PRIMARY KEY ((et,pid),ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

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
            string keyspace = cassandraProvider.GetKeyspace();

            Task[] createTableTasks = new Task[]
                {
                    CreateTableAsync(CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE,keyspace, INDEX_BY_EVENT_TYPE_TABLE_NAME),
                    CreateTableAsync(MessageCounter.CreateTableTemplate, keyspace, "EventCounter")
                };

            return Task.WhenAll(createTableTasks);
        }

        private async Task CreateTableAsync(string cqlQuery, string keyspace, string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", tableName, session.Cluster.AllHosts().First().Address, keyspace);

                string query = string.Format(cqlQuery, keyspace, tableName).ToLower();
                PreparedStatement createEventsTableStatement = await session.PrepareAsync(query).ConfigureAwait(false);
                createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

                if (logger.IsEnabled(LogLevel.Debug))
                    logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", tableName, keyspace);
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

                string keyspace = cassandraProvider.GetKeyspace();
                string tableQuery = string.Format(CREATE_EVENTS_TABLE_TEMPLATE, keyspace, tableName).ToLower();

                PreparedStatement tableStatement = await session.PrepareAsync(string.Format(tableQuery, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(tableStatement.Bind()).ConfigureAwait(false);

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
