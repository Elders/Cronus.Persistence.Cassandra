using System;
using Elders.Cronus.Persistence.Cassandra.Config;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using DataStaxCassandra = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProviderForEventStore
    {
        private readonly ICassandraReplicationStrategy replicationStrategy;

        private DataStaxCassandra.Cluster cluster;

        public CassandraProviderForEventStore(IConfiguration configuration, ICassandraReplicationStrategy replicationStrategy) : this(configuration, replicationStrategy, DataStaxCassandra.Cluster.Builder()) { }

        public CassandraProviderForEventStore(IConfiguration configuration, ICassandraReplicationStrategy replicationStrategy, DataStaxCassandra.IInitializer initializer)
        {
            if (configuration is null) throw new ArgumentNullException(nameof(configuration));
            if (initializer is null) throw new ArgumentNullException(nameof(initializer));

            DataStaxCassandra.Builder builder = initializer as DataStaxCassandra.Builder;
            if (builder is null == false)
            {
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = configuration["cronus_persistence_cassandra_connectionstring"];
                var connStrBuilder = new DataStaxCassandra.CassandraConnectionStringBuilder(connectionString);
                Keyspace = connStrBuilder.DefaultKeyspace;

                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithRetryPolicy(new EventStoreNoHintedHandOff())
                    .Build();
            }
            else
            {
                cluster = DataStaxCassandra.Cluster.BuildFrom(initializer);
            }

            this.replicationStrategy = replicationStrategy;
        }

        public string Keyspace { get; private set; }

        public DataStaxCassandra.Cluster GetCluster()
        {
            return cluster;
        }

        public DataStaxCassandra.ISession GetSession(string tenant)
        {
            string tenantPrefix = string.IsNullOrEmpty(tenant) ? string.Empty : $"{tenant}_";
            var keyspace = $"{tenantPrefix}{Keyspace}";
            if (keyspace.Length > 48) throw new ArgumentException($"Cassandra keyspace exceeds maximum length of 48. Keyspace: {keyspace}");

            DataStaxCassandra.ISession session = GetCluster().Connect();
            session.CreateKeyspace(Keyspace, replicationStrategy);

            return session;
        }
    }
}
