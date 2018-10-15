using System;
using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreStorageManager : IEventStoreStorageManager
    {
        private const string CreateEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private readonly ISession session;
        private readonly string keyspace;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ICassandraReplicationStrategy replicationStrategy;

        public CassandraEventStoreStorageManager(ISession session, string keyspace, ICassandraEventStoreTableNameStrategy tableNameStrategy, ICassandraReplicationStrategy replicationStrategy)
        {
            this.session = session;
            this.keyspace = keyspace;
            this.tableNameStrategy = tableNameStrategy;
            this.replicationStrategy = replicationStrategy;
        }

        public CassandraEventStoreStorageManager(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy)
        {
            this.session = session;
            this.tableNameStrategy = tableNameStrategy;
        }

        public void CreateStorage()
        {
            CreateEventsStorage();
            CreateSnapshotsStorage();
        }

        public void CreateEventsStorage()
        {
            foreach (var tableName in tableNameStrategy.GetAllTableNames())
            {
                var createEventsTable = string.Format(CreateEventsTableTemplate, tableName).ToLower();
                session.Execute(createEventsTable);
            }
        }

        public void CreateSnapshotsStorage()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
        }
    }
}
