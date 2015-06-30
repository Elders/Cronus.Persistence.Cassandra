using System;
using Cassandra;
using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreStorageManager : IEventStoreStorageManager
    {
        private const string CreateKeySpaceTemplate = @"CREATE KEYSPACE IF NOT EXISTS {0} WITH replication = {{'class':'SimpleStrategy', 'replication_factor':1}};";
        private const string CreateEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private readonly ISession session;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        public CassandraEventStoreStorageManager(ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy)
        {
            this.session = session;
            this.tableNameStrategy = tableNameStrategy;
        }

        public void CreateEventsStorage()
        {
            foreach (var tableName in tableNameStrategy.GetAllTableNames())
            {
                var createEventsTable = String.Format(CreateEventsTableTemplate, tableName).ToLower();
                session.Execute(createEventsTable);
            }
        }

        public void CreateStorage()
        {
            var createKeySpaceQuery = String.Format(CreateKeySpaceTemplate, session.Keyspace);
            session.Execute(createKeySpaceQuery);

            CreateEventsStorage();
            CreateSnapshotsStorage();
        }

        public void CreateSnapshotsStorage()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
        }
    }
}
