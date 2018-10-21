using System;
using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreStorageManager : IEventStoreStorageManager
    {
        private const string CreateEventsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id text, ts bigint, rev int, data blob, PRIMARY KEY (id,rev,ts)) WITH CLUSTERING ORDER BY (rev ASC);";
        private const string CreateIndexStatusTableTemplate = @"CREATE TABLE IF NOT EXISTS ""index_status"" (id text, status text, PRIMARY KEY (id));";
        private const string CreateIndexByEventTypeTableTemplate = @"CREATE TABLE IF NOT EXISTS ""index_by_eventtype"" (et text, aid text, PRIMARY KEY (et));";
        //private const string CreateSnapshotsTableTemplate = @"CREATE TABLE IF NOT EXISTS ""{0}"" (id uuid, ver int, ts bigint, data blob, PRIMARY KEY (id,ver));";

        private readonly string boundedContext;
        private readonly ISession session;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;

        public CassandraEventStoreStorageManager(IConfiguration configuration, ISession session, ICassandraEventStoreTableNameStrategy tableNameStrategy)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (session is null) throw new ArgumentNullException(nameof(session));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));

            this.boundedContext = configuration["cronus_boundedcontext"];
            this.session = session;
            this.tableNameStrategy = tableNameStrategy;
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
                var createEventsTable = string.Format(CreateEventsTableTemplate, tableName).ToLower();
                session.Execute(createEventsTable);
            }
        }

        public void CreateSnapshotsStorage()
        {
            //var createSnapshotsTable = String.Format(CreateSnapshotsTableTemplate, tableNameStrategy.GetSnapshotsTableName()).ToLower();
            //session.Execute(createSnapshotsTable);
        }

        public void CreateIndecies()
        {
            session.Execute(CreateIndexStatusTableTemplate);
            session.Execute(CreateIndexByEventTypeTableTemplate);
        }
    }
}
