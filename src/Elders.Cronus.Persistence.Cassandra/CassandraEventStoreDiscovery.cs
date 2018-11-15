using System;
using System.Collections.Generic;
using System.Linq;
using Cassandra;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreDiscovery : DiscoveryBasedOnExecutingDirAssemblies<IEventStore>
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            return new DiscoveryResult<IEventStore>(GetModels(context));
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(IEventStore<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IEventStore), typeof(CassandraEventStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IEventStorePlayer<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IEventStorePlayer), typeof(CassandraEventStore), ServiceLifetime.Transient);

            var cassandraSettings = context.Assemblies.SelectMany(asm => asm.GetLoadableTypes())
                .Where(type => type.IsAbstract == false && type.IsInterface == false && typeof(ICassandraEventStoreSettings).IsAssignableFrom(type));
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            yield return new DiscoveredModel(typeof(EventStoreIndex), typeof(EventStoreIndex), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ISession), provider => provider.GetRequiredService<ICassandraProvider>().GetSession(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ICassandraEventStoreTableNameStrategy), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => GetReplicationStrategy(context.Configuration), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IIndexStatusStore), typeof(CassandraIndexStatusStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IIndexStore), typeof(IndexByEventTypeStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraEventStoreSchema), typeof(CassandraEventStoreSchema), ServiceLifetime.Transient);
        }

        int GetReplocationFactor(IConfiguration configuration)
        {
            var replFactorCfg = configuration["cronus_persistence_cassandra_replication_factor"];
            return string.IsNullOrEmpty(replFactorCfg) ? 1 : int.Parse(replFactorCfg);
        }

        ICassandraReplicationStrategy GetReplicationStrategy(IConfiguration configuration)
        {
            var replStratefyCfg = configuration["cronus_persistence_cassandra_replication_strategy"];

            ICassandraReplicationStrategy replicationStrategy = null;
            if (string.IsNullOrEmpty(replStratefyCfg))
            {
                replicationStrategy = new SimpleReplicationStrategy(1);
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
