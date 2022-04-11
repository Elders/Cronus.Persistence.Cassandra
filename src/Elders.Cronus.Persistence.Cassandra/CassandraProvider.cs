using System;
using Cassandra;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        protected CassandraProviderOptions options;

        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly IInitializer initializer;

        protected ICluster cluster;
        protected ISession session;

        private static object establishNewConnection = new object();

        private string baseConfigurationKeyspace;

        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer = null)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.options = optionsMonitor.CurrentValue;
            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
        }

        public ICluster GetCluster()
        {
            if (cluster is null == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = DataStax.Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = options.ConnectionString;

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                cluster?.Shutdown(30000);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();

                cluster.RefreshSchema();
            }

            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        public ISession GetSession()
        {
            if (session is null || session.IsDisposed)
            {
                lock (establishNewConnection)
                {
                    if (session is null || session.IsDisposed)
                    {
                        try
                        {
                            session = GetCluster().Connect(GetKeyspace());
                        }
                        catch (InvalidQueryException)
                        {
                            using (ISession schemaSession = GetCluster().Connect())
                            {
                                var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(GetKeyspace());
                                schemaSession.Execute(createKeySpaceQuery);
                            }

                            session = GetCluster().Connect(GetKeyspace());
                        }
                    }
                }
            }

            return session;
        }

        private void CreateKeyspace(string keyspace, ICassandraReplicationStrategy replicationStrategy)
        {
            var createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(keyspace);
            session.Execute(createKeySpaceQuery);
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
