using System;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreSchema // should be internal but the xunit prevents that. Why? :D
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraEventStoreSchema));

        private const string CreateEventsTableQueryTemplate = @"CREATE TABLE IF NOT EXISTS {0}.{1} (id blob, ts bigint, rev int, pos int, data blob, PRIMARY KEY (id,rev,pos)) WITH CLUSTERING ORDER BY (rev ASC, pos ASC);";
        private const string CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE = @"CREATE TABLE IF NOT EXISTS {0}.{1} (et text, pid int, aid blob, rev int, pos int, ts bigint, PRIMARY KEY ((et,pid),ts,aid,rev,pos)) WITH CLUSTERING ORDER BY (ts ASC);"; // ASC element required to be in second position in primary key https://stackoverflow.com/questions/23185331/cql-bad-request-missing-clustering-order-for-column
        private const string INDEX_BY_EVENT_TYPE_TABLE_NAME = "index_by_eventtype";
        private readonly ICassandraProvider cassandraProvider;
        private readonly ITableNamingStrategy tableNameStrategy;
        private readonly ICassandraReplicationStrategy replicationStrategy;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        public CassandraEventStoreSchema(ICassandraProvider cassandraProvider, ITableNamingStrategy tableNameStrategy, ICassandraReplicationStrategy replicationStrategy)
        {
            if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

            this.cassandraProvider = cassandraProvider;
            this.tableNameStrategy = tableNameStrategy ?? throw new ArgumentNullException(nameof(tableNameStrategy));
            this.replicationStrategy = replicationStrategy;
        }

        /// <summary>
        /// This is the main method which the framework invokes. Other methods are also exposed, why not?!?
        /// </summary>
        /// <returns></returns>
        public async Task CreateStorageAsync()
        {
            ISession session = await GetSessionAsync().ConfigureAwait(false);

            await CreateKeyspace(session).ConfigureAwait(false);

            Task[] createESTasks =
            [
                CreateEventsStorageAsync(session),
                CreateIndeciesAsync(session)
            ];

            await Task.WhenAll(createESTasks).ConfigureAwait(false);
        }

        public async Task CreateKeyspace(ISession session)
        {
            IStatement createTableStatement = await GetCreateKeySpaceQuery(session).ConfigureAwait(false);
            await session.ExecuteAsync(createTableStatement).ConfigureAwait(false);
        }

        public Task CreateEventsStorageAsync(ISession session)
        {
            string tableName = tableNameStrategy.GetName();
            return CreateTableAsync(session, CreateEventsTableQueryTemplate, tableName);
        }

        public Task CreateIndeciesAsync(ISession session)
        {
            Task[] createTableTasks = new Task[]
                {
                    CreateTableAsync(session, CREATE_INDEX_BY_EVENT_TYPE_TABLE_TEMPLATE, INDEX_BY_EVENT_TYPE_TABLE_NAME),
                    CreateTableAsync(session, MessageCounter.CreateTableTemplate, "EventCounter")
                };

            return Task.WhenAll(createTableTasks);
        }

        private async Task<IStatement> GetCreateKeySpaceQuery(ISession session)
        {
            string keyspace = cassandraProvider.GetKeyspace();
            string createKeySpaceQueryTemplate = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            PreparedStatement createEventsTableStatement = await session.PrepareAsync(createKeySpaceQueryTemplate).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

            return createEventsTableStatement.Bind();
        }

        private async Task CreateTableAsync(ISession session, string cqlQueryTemplate, string tableName)
        {
            string keyspace = cassandraProvider.GetKeyspace();

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[EventStore] Creating table `{tableName}` with `{address}` in keyspace `{keyspace}`...", tableName, session.Cluster.AllHosts().First().Address, keyspace);

            string query = string.Format(cqlQueryTemplate, keyspace, tableName).ToLower();
            PreparedStatement createEventsTableStatement = await session.PrepareAsync(query).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.All);

            await session.ExecuteAsync(createEventsTableStatement.Bind()).ConfigureAwait(false);

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("[EventStore] Created table `{tableName}` in keyspace `{keyspace}`...", tableName, keyspace);
        }
    }
}
