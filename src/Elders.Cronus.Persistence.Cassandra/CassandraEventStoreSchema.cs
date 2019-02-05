using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Persistence.Cassandra.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSchema : ICassandraEventStoreSchema
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraEventStoreSchema));

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        private const string CREATE_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, status text, PRIMARY KEY (id));";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid text, PRIMARY KEY (et,aid));";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private const string INDEX_STATUS_TABLE_NAME = "index_status";
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";

        private readonly BoundedContext boundedContext;
        private readonly ISession session;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        public CassandraEventStoreSchema(BoundedContext boundedContext, ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ILock @lock)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.boundedContext = boundedContext ?? throw new ArgumentNullException(nameof(boundedContext));
            this.session = cassandraProvider.GetSession();
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        public void CreateStorage()
        {
            CreateEventsStorage();
            CreateIndecies();
            CreateSnapshotsStorage();
        }

        public void CreateEventsStorage()
        {
            string tableName = tableNameStrategy.GetName();
            CreateTable(CREATE_EVENTS_TABLE_TEMPLATE, tableName);
        }

        public void CreateSnapshotsStorage()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
        }

        public void CreateIndecies()
        {
            CreateTable(CREATE_INDEX_STATUS_TABLE_TEMPLATE, INDEX_STATUS_TABLE_NAME);
            CreateTable(CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, INDEX_BY_EVENT_TYPE_TABLE_NAME);
        }

        void CreateTable(string cqlQuery, string tableName)
        {
            if (@lock.Lock(tableName, lockTtl))
            {
                try
                {
                    log.Debug(() => $"[EventStore] Creating table `{tableName}` with `{session.Cluster.AllHosts().First().Address}` in keyspace `{session.Keyspace}`...");

                    var createEventsTable = string.Format(cqlQuery, tableName).ToLower();
                    session.Execute(createEventsTable);

                    log.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{session.Keyspace}`...");
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
                log.Warn($"[EventStore] Could not acquire lock for `{tableName}` to create table.");
            }
        }
    }
}
