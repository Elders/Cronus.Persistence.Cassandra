using System;
using System.Collections.Generic;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreDiscovery : DiscoveryBasedOnExecutingDirAssemblies<IEventStore>
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            var result = new DiscoveryResult<IEventStore>();

            result.Models.Add(new DiscoveredModel(typeof(IEventStoreFactory), typeof(CassandraEventStoreFactory)));
            result.Models.Add(new DiscoveredModel(typeof(IEventStore), typeof(MultiTenantEventStore)));
            result.Models.Add(new DiscoveredModel(typeof(CassandraProviderForEventStore)));
            result.Models.Add(new DiscoveredModel(typeof(ICassandraEventStoreTableNameStrategy), typeof(TablePerBoundedContext)));

            result.Models.Add(new DiscoveredModel(typeof(ICassandraReplicationStrategy), typeof(ICassandraReplicationStrategy), GetReplicationStrategy(context.Configuration)));

            return result;
        }

        int GetReplocationFactor(IConfiguration configuration)
        {
            var replFactorCfg = configuration["cronus_persistence_cassandra_replication_factor"];
            return string.IsNullOrEmpty(replFactorCfg) ? 2 : int.Parse(replFactorCfg);
        }

        ICassandraReplicationStrategy GetReplicationStrategy(IConfiguration configuration)
        {
            var replStratefyCfg = configuration["cronus_persistence_cassandra_replication_strategy"];
            var replFactorCfg = configuration["cronus_persistence_cassandra_replication_factor"];

            ICassandraReplicationStrategy replicationStrategy = null;
            if (string.IsNullOrEmpty(replStratefyCfg))
            {
                replicationStrategy = new SimpleReplicationStrategy(2);
            }
            else if (replStratefyCfg.Equals("simple", StringComparison.OrdinalIgnoreCase))
            {
                replicationStrategy = new SimpleReplicationStrategy(GetReplocationFactor(configuration));
            }
            else if (replStratefyCfg.Equals("network_topology", StringComparison.OrdinalIgnoreCase))
            {
                int replicationFactor = GetReplocationFactor(configuration);
                var settings = new List<NetworkTopologyReplicationStrategy.DataCenterSettings>();
                string[] datacenters = configuration["cronus_persistence_cassandra__datacenters"].Split(',');
                foreach (var datacenter in datacenters)
                {
                    var setting = new NetworkTopologyReplicationStrategy.DataCenterSettings(datacenter, replicationFactor);
                    settings.Add(setting);
                }
                replicationStrategy = new NetworkTopologyReplicationStrategy(settings);
            }

            return replicationStrategy;
        }
    }
}
