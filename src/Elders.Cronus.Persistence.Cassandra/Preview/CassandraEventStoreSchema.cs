using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Elders.Cronus.Persistence.Cassandra.Snapshots;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class CassandraEventStoreSchema : ICassandraEventStoreSchema
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchema));

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string CREATE_SNAPSHOTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, rev int, data blob, PRIMARY KEY (id,rev)) WITH CLUSTERING ORDER BY (rev ASC);";
        private const string INDEX_REV = @"CREATE INDEX IF NOT EXISTS ""{0}_idx_rev"" ON ""{0}"" (rev);";
        private const string INDEX_POS = @"CREATE INDEX IF NOT EXISTS ""{0}_idx_pos"" ON ""{0}"" (pos);";

        private const string CREATE_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, status text, PRIMARY KEY (id));";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid blob, rev int, pos int, ts bigint, PRIMARY KEY (et,ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column

        private const string INDEX_STATUS_TABLE_NAME = "index_status";
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";


        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();// In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)
        public CassandraEventStoreSchema(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.snapshotsTableNamingStrategy = snapshotsTableNamingStrategy;
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
            string tableName = snapshotsTableNamingStrategy.GetName();
            return CreateSnapshotPersistanseAsync(tableName);
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

                logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{session.Keyspace}`...");
            }
            catch (Exception)
            {
                throw;
            }
        }

        private async Task CreateSnapshotPersistanseAsync(string tableName)
        {
            try
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);

                logger.Debug(() => $"[EventStore] Creating table `{tableName}` with `{session.Cluster.AllHosts().First().Address}` in keyspace `{session.Keyspace}`...");

                string tableQuery = string.Format(CREATE_SNAPSHOTS_TABLE_TEMPLATE, tableName).ToLower();
                string rev = string.Format(INDEX_REV, tableName).ToLower();

                PreparedStatement tableStatement = await session.PrepareAsync(string.Format(tableQuery, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                PreparedStatement revStatement = await session.PrepareAsync(string.Format(rev, tableName).ToLower()).ConfigureAwait(false);
                tableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

                await session.ExecuteAsync(tableStatement.Bind()).ConfigureAwait(false);
                await session.ExecuteAsync(revStatement.Bind()).ConfigureAwait(false);

                logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{session.Keyspace}`...");
            }
            catch (Exception)
            {
                throw;
            }
        }
    }
}
