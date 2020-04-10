﻿using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.DependencyInjection;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreDiscovery : EventStoreDiscoveryBase
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            IEnumerable<DiscoveredModel> models = base.DiscoverFromAssemblies(context).Models.Concat(GetModels(context));
            return new DiscoveryResult<IEventStore>(models, services => services.AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>());
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(IEventStore<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IEventStore), typeof(CassandraEventStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IEventStorePlayer<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IEventStorePlayer), typeof(CassandraEventStore), ServiceLifetime.Transient);

            var cassandraSettings = context.FindService<ICassandraEventStoreSettings>();
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            yield return new DiscoveredModel(typeof(EventToAggregateRootId), typeof(EventToAggregateRootId), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ITableNamingStrategy), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(NoTableNamingStrategy), typeof(NoTableNamingStrategy), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(TablePerBoundedContext), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);

            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IIndexStatusStore), typeof(CassandraIndexStatusStore), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(IIndexStore), typeof(IndexByEventTypeStore), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(ICassandraEventStoreSchema), typeof(CassandraEventStoreSchema), ServiceLifetime.Transient);
        }
    }
}
