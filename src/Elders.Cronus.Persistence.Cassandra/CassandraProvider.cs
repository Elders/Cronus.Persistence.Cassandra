﻿using System;
using Cassandra;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        private bool optionsHasChanged = true;

        protected CassandraProviderOptions options;

        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly IInitializer initializer;

        protected ICluster cluster;
        protected ISession session;

        private string baseConfigurationKeyspace;

        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer = null)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.options = optionsMonitor.CurrentValue;
            optionsMonitor.OnChange(Changed);

            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
        }

        public ICluster GetCluster()
        {
            if (cluster is null == false && optionsHasChanged == false)
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
            }

            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            optionsHasChanged = false;

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        public ISession GetSession()
        {
            if (session is null || session.IsDisposed || optionsHasChanged)
            {
                session?.Dispose();
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

        private void Changed(CassandraProviderOptions newOptions)
        {
            if (options != newOptions)
            {
                options = newOptions;
                optionsHasChanged = true;
            }
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
