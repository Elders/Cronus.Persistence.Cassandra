using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Preview
{
    public class CassandraEventStoreSchemaNew : ICassandraEventStoreSchema
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchemaNew));

        private const string NEW_CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""new{0}"" (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string INDEX_REV = @"CREATE INDEX IF NOT EXISTS ""new{0}_idx_rev"" ON ""new{0}"" (rev);";
        private const string INDEX_POS = @"CREATE INDEX IF NOT EXISTS ""new{0}_idx_pos"" ON ""new{0}"" (pos);";

        private const string CREATE_NEW_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id blob, status text, PRIMARY KEY (id));";
        private const string CREATE_NEW_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid blob, rev int, pos int, ts bigint, PRIMARY KEY (et,ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column

        private const string NEW_INDEX_STATUS_TABLE_NAME = "new_index_status";
        private const string NEW_INDEX_BY_EVENT_TYPE_TABLE_NAME = "new_index_by_eventtype";


        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        private ISession GetSession() => cassandraProvider.GetSession(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

        public CassandraEventStoreSchemaNew(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ILock @lock)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        private void CreatePersistencePreview(string tableName)
        {
            if (@lock.Lock(tableName, lockTtl))
            {
                try
                {
                    logger.Debug(() => $"[EventStore] Creating table `{tableName}` with `{GetSession().Cluster.AllHosts().First().Address}` in keyspace `{GetSession().Keyspace}`...");

                    var table = string.Format(NEW_CREATE_EVENTS_TABLE_TEMPLATE, tableName).ToLower();
                    var rev = string.Format(INDEX_REV, tableName).ToLower();
                    var pos = string.Format(INDEX_POS, tableName).ToLower();

                    GetSession().Execute(table);
                    GetSession().Execute(rev);
                    GetSession().Execute(pos);

                    logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{GetSession().Keyspace}`...");
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

        public void CreateStorage()
        {
            CreateEventsStorage();
            CreateIndecies();
            CreateSnapshotsStorage();

            CreateEventsStoragePreview();
        }

        public void CreateEventsStorage()
        {
            string tableName = tableNameStrategy.GetName();
            CreateTable(NEW_CREATE_EVENTS_TABLE_TEMPLATE, tableName);
        }

        public void CreateEventsStoragePreview()
        {
            string tableName = tableNameStrategy.GetName();
            CreatePersistencePreview(tableName);
        }

        public void CreateSnapshotsStorage()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
        }

        public void CreateIndecies()
        {
            CreateTable(CREATE_NEW_INDEX_STATUS_TABLE_TEMPLATE, NEW_INDEX_STATUS_TABLE_NAME);
            CreateTable(CREATE_NEW_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, NEW_INDEX_BY_EVENT_TYPE_TABLE_NAME);
            CreateTable(MessageCounter.CreateTableTemplate, "EventCounter");
        }

        public void CreateTable(string cqlQuery, string tableName)
        {
            if (@lock.Lock(tableName, lockTtl))
             {
                try
                {
                    logger.Debug(() => $"[EventStore] Creating table `{tableName}` with `{GetSession().Cluster.AllHosts().First().Address}` in keyspace `{GetSession().Keyspace}`...");

                    var createEventsTable = string.Format(cqlQuery, tableName).ToLower();
                    GetSession().Execute(createEventsTable);

                    logger.Debug(() => $"[EventStore] Created table `{tableName}` in keyspace `{GetSession().Keyspace}`...");
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
