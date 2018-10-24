using System;
using System.Linq;
using Cassandra;
using Elders.Cronus.AtomicAction;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.Logging;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreStorageManager : IEventStoreStorageManager
    {
        static ILog log = LogProvider.GetLogger(typeof(CassandraEventStoreStorageManager));

        private const string CREATE_EVENTS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        private const string CREATE_INDEX_STATUS_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, status text, PRIMARY KEY (id));";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS ""{0}"" (et text, aid text, PRIMARY KEY (et));";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private const string INDEX_STATUS_TABLE_NAME = "index_status";
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";

        private readonly string boundedContext;
        private readonly ISession schema;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        public CassandraEventStoreStorageManager(IConfiguration configuration, ISession schemaSession, ICassandraEventStoreTableNameStrategy tableNameStrategy, ILock @lock)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (schemaSession is null) throw new ArgumentNullException(nameof(schemaSession));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));

            this.boundedContext = configuration["cronus_boundedcontext"];
            this.schema = schemaSession;
            this.tableNameStrategy = tableNameStrategy;
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
            foreach (var tableName in tableNameStrategy.GetAllTableNames(boundedContext))
            {
                CreateTable(CREATE_EVENTS_TABLE_TEMPLATE, tableName);
            }
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
                    log.Info(() => $"[Event Store] Creating table `{tableName}` with `{schema.Cluster.AllHosts().First().Address}` in keyspace `{schema.Keyspace}`...");

                    var createEventsTable = string.Format(cqlQuery, tableName).ToLower();
                    schema.Execute(createEventsTable);

                    log.Info(() => $"[Event Store] Created table `{tableName}` in keyspace `{schema.Keyspace}`...");
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
                log.Info($"[Event Store] Could not acquire lock for `{tableName}` to create table.");
            }
        }
    }
}
