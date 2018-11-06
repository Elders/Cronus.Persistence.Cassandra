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
        public const string ConnectionStringSettingKey = "cronus_persistence_cassandra_connectionstring";

        private readonly IConfiguration configuration;
        protected readonly CronusContext context;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly ICassandraEventStoreTableNameStrategy tableNameStrategy;
        protected readonly IInitializer initializer;

        protected Cluster cluster;
        protected ISession session;

        private string baseConfigurationKeyspace;

        public CassandraProvider(IConfiguration configuration, CronusContext context, ICassandraReplicationStrategy replicationStrategy, ICassandraEventStoreTableNameStrategy tableNameStrategy, IInitializer initializer = null)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));
            if (tableNameStrategy is null) throw new ArgumentNullException(nameof(tableNameStrategy));

            this.configuration = configuration;
            this.context = context;
            this.replicationStrategy = replicationStrategy;
            this.tableNameStrategy = tableNameStrategy;
        }

        public Cluster GetCluster()
        {
            if (cluster is null == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = configuration.GetRequired(ConnectionStringSettingKey);

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, "");
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();
            }

            else
            {
                cluster = Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            string tenantPrefix = string.IsNullOrEmpty(context.Tenant) ? string.Empty : $"{context.Tenant}_";
            var keyspace = $"{tenantPrefix}{baseConfigurationKeyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            return keyspace;
        }

        public ISession GetSession()
        {
            if (session is null)
            {
                session = GetCluster().Connect();
                CreateKeyspace(GetKeyspace(), replicationStrategy);
            }

            return session;
        }

        private void CreateKeyspace(string keyspace, ICassandraReplicationStrategy replicationStrategy)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
            session.ChangeKeyspace(keyspace);
        }
    }

    class NoHintedHandOffRetryPolicy : IRetryPolicy
    {
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            if (nbRetry != 0)
                return RetryDecision.Rethrow();

            return receivedResponses >= requiredResponses && !dataRetrieved
                       ? RetryDecision.Retry(cl)
                       : RetryDecision.Rethrow();
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }
    }
}
