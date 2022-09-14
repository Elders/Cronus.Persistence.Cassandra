using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class CassandraEventStoreSchemaNew : ICassandraEventStoreSchema
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchemaNew));

        private const string NEW_CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string INDEX_REV = @"CREATE INDEX IF NOT EXISTS ""{0}_idx_rev"" ON ""{0}"" (rev);";
        private const string INDEX_POS = @"CREATE INDEX IF NOT EXISTS ""{0}_idx_pos"" ON ""{0}"" (pos);";

        private const string CREATE_NEW_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, status text, PRIMARY KEY (id));";
        private const string CREATE_NEW_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid blob, rev int, pos int, ts bigint, PRIMARY KEY (et,ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column

        private const string NEW_INDEX_STATUS_TABLE_NAME = "new_index_status";
        private const string NEW_INDEX_BY_EVENT_TYPE_TABLE_NAME = "new_index_by_eventtype";


        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)
        public CassandraEventStoreSchemaNew(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ILock @lock)
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
            return CreateEventStoragePersistanseAsync(tableName);
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
                    CreateTableAsync(CREATE_NEW_INDEX_STATUS_TABLE_TEMPLATE, NEW_INDEX_STATUS_TABLE_NAME),
                    CreateTableAsync(CREATE_NEW_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, NEW_INDEX_BY_EVENT_TYPE_TABLE_NAME),
                    CreateTableAsync(MessageCounter.CreateTableTemplate, "EventCounter")
                };

            return Task.WhenAll(createTableTasks);
        }

        private async Task CreateTableAsync(string cqlQuery, string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                logger.Debug(() => $"[EventStore] Creating table `{tableName}` with `{session.Cluster.AllHosts().First().Address}` in keyspace `{session.Keyspace}`...");

                PreparedStatement createEventsTableStatement = await session.PrepareAsync(string.Format(cqlQuery, tableName).ToLower()).ConfigureAwait(false);
                createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

                logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{session.Keyspace}`...");
            }
            catch (Exception)
            {
                throw;
            }
        }

        private async Task CreateEventStoragePersistanseAsync(string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);

                logger.Debug(() => $"[EventStore] Creating table `{tableName}` with `{session.Cluster.AllHosts().First().Address}` in keyspace `{session.Keyspace}`...");

                string tableQuery = string.Format(NEW_CREATE_EVENTS_TABLE_TEMPLATE, tableName).ToLower();
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

                logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{session.Keyspace}`...");
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
