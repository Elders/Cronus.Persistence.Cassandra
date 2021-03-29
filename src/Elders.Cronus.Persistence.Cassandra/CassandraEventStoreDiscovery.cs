using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Migrations;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Elders.Cronus.Persistence.Cassandra.Migrations;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.DependencyInjection;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraEventStoreDiscovery : EventStoreDiscovery
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            IEnumerable<DiscoveredModel> models = base.DiscoverFromAssemblies(context).Models
                .Concat(GetModels(context))
                .Concat(DiscoverCassandraTableNameStrategy(context));


            return new DiscoveryResult<IEventStore>(models, services =>
            {
                services.AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>();
                services.AddOptions<MigrationCassandraProviderOptions, MigrationCassandraProviderOptionsProvider>();
            });
        }

        protected virtual IEnumerable<DiscoveredModel> DiscoverCassandraTableNameStrategy(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(ITableNamingStrategy), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(TablePerBoundedContext), typeof(TablePerBoundedContext), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(NoTableNamingStrategy), typeof(NoTableNamingStrategy), ServiceLifetime.Singleton);
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(IEventStore<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStore), typeof(CassandraEventStore), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(IEventStorePlayer<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStorePlayer), typeof(CassandraEventStore), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(CassandraEventStore<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraEventStore), typeof(CassandraEventStore), ServiceLifetime.Transient);

            var cassandraSettings = context.FindService<ICassandraEventStoreSettings>();
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            //yield return new DiscoveredModel(typeof(EventToAggregateRootId), typeof(EventToAggregateRootId), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IIndexStore), typeof(IndexByEventTypeStore), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(ICassandraEventStoreSchema), typeof(CassandraEventStoreSchema), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IMessageCounter), typeof(MessageCounter), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MessageCounter), typeof(MessageCounter), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        }
    }


    public class CassandraEventStoreMigrationDiscovery : EventStoreDiscovery
    {
        protected override DiscoveryResult<IEventStore> DiscoverFromAssemblies(DiscoveryContext context)
        {
            IEnumerable<DiscoveredModel> models = base.DiscoverFromAssemblies(context).Models
                .Concat(GetModels(context));


            return new DiscoveryResult<IEventStore>(models, services => services.AddOptions<CassandraProviderOptions, CassandraProviderOptionsProvider>());
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {
            yield return new DiscoveredModel(typeof(MigratorCassandraProvider), typeof(MigratorCassandraProvider), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigratorCassandraReplaySettings), typeof(MigratorCassandraReplaySettings), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigrationCassandraProviderOptions), typeof(MigrationCassandraProviderOptions), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IMigrationEventStorePlayer), typeof(CassandraMigratorEventStorePlayer), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigrationRunner<,>), typeof(MigrationRunner<,>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        }
    }
}
