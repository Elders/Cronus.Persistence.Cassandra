using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSchema : ICassandraEventStoreSchema
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchema));

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        private const string CREATE_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, status text, PRIMARY KEY (id));";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid text, PRIMARY KEY (et,aid));";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private const string INDEX_STATUS_TABLE_NAME = "index_status";
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraEventStoreSchema(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ILock @lock)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
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
            return CreateTableAsync(CREATE_EVENTS_TABLE_TEMPLATE, tableName);
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

        async Task CreateTableAsync(string cqlQuery, string tableName)
        {
            if (@lock.Lock(tableName, lockTtl))
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
                finally
                {
                    @lock.Unlock(tableName);
                }
            }
            else
            {
                logger.Warn(() => $"[EventStore] Could not acquire lock for `{tableName}` to create table.");
            }
        }
    }
}
