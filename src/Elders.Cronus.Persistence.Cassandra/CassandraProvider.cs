using System;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using Cassandra;
using Elders.Cronus.AtomicAction;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        private readonly CronusContext context;
        private readonly ICassandraReplicationStrategy replicationStrategy;
        private readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        private readonly ILock @lock;
        private Cluster cluster;

        private readonly string baseConfigurationKeyspace;
        public CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy, ICassandraEventStoreTableNameStrategy tableNameStrategy, ILock @lock)
            : this(configuration, context, replicationStrategy, tableNameStrategy, Cluster.Builder(), @lock) { }

        protected CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy, ICassandraEventStoreTableNameStrategy tableNameStrategy, IInitializer initializer, ILock @lock)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (initializer is null) throw new ArgumentNullException(nameof(initializer));

            Builder builder = initializer as Builder;
            if (builder is null == false)
            {
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = configuration["cronus_persistence_cassandra_connectionstring"];

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, "");

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithRetryPolicy(new EventStoreNoHintedHandOff())
                    .Build();
            }
            else
            {
                cluster = Cluster.BuildFrom(initializer);
            }

            this.context = context;
            this.replicationStrategy = replicationStrategy;
            this.tableNameStrategy = tableNameStrategy;
            this.@lock = @lock;
            var storageManager = new CassandraEventStoreStorageManager(configuration, GetSession(), tableNameStrategy, @lock);
            storageManager.CreateStorage();
        }

        public Cluster GetCluster()
        {
            return cluster;
        }

        public ISession GetSession()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            ISession session = GetCluster().Connect();
            session.CreateKeyspace(keyspace, replicationStrategy);

            return session;
        }
    }
}
