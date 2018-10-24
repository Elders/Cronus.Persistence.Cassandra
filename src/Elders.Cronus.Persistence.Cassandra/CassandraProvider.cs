using System;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using Cassandra;
using Elders.Cronus.AtomicAction;
using System.Linq;

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
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));
            if (initializer is null) throw new ArgumentNullException(nameof(initializer));
            if (@lock is null) throw new ArgumentNullException(nameof(@lock));

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
            var storageManager = new CassandraEventStoreSchema(configuration, GetSchemaSession(), tableNameStrategy, @lock);
            storageManager.CreateStorage();
        }

        public Cluster GetCluster()
        {
            return cluster;
        }

        string GetKeyspace()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }

        public ISession GetSession()
        {
            ISession session = GetCluster().Connect();
            session.CreateKeyspace(GetKeyspace(), replicationStrategy);

            return session;
        }

        public ISession GetSchemaSession()
        {
            var hosts = GetCluster().AllHosts().ToList();
            ISession schemaSession = null;
            var counter = 0;

            while (ReferenceEquals(null, schemaSession))
            {
                var schemaCreatorVoltron = hosts.ElementAtOrDefault(counter++);
                if (ReferenceEquals(null, schemaCreatorVoltron))
                    throw new InvalidOperationException($"Could not find a Cassandra node! Hosts: '{string.Join(", ", hosts.Select(x => x.Address))}'");

                var schemaCluster = Cluster
                    .Builder()
                    .AddContactPoint(schemaCreatorVoltron.Address)
                    .Build();

                try
                {
                    schemaSession = schemaCluster.Connect();
                    schemaSession.CreateKeyspace(GetKeyspace(), replicationStrategy);
                }
                catch (NoHostAvailableException)
                {
                    if (counter < hosts.Count)
                        continue;
                    else
                        throw;
                }
            }

            return schemaSession;
        }
    }
}
