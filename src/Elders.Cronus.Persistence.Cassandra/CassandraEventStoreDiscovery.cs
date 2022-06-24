using System;
using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.EventStore.Index;
using Elders.Cronus.Persistence.Cassandra.Counters;
using Elders.Cronus.Persistence.Cassandra.Migrations;
using Elders.Cronus.Persistence.Cassandra.Preview;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
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
            yield return new DiscoveredModel(typeof(ITableNamingStrategy), typeof(TablePerBoundedContextNew), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(TablePerBoundedContextNew), typeof(TablePerBoundedContextNew), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(NoTableNamingStrategyStiopa), typeof(NoTableNamingStrategyStiopa), ServiceLifetime.Singleton);
        }

        IEnumerable<DiscoveredModel> GetModels(DiscoveryContext context)
        {

            yield return new DiscoveredModel(typeof(IEventStore<>), typeof(CassandraEventStoreNew<>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStore), typeof(CassandraEventStoreNew), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(IEventStorePlayer<>), typeof(CassandraEventStoreNew<>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IEventStorePlayer), typeof(CassandraEventStoreNew), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(CassandraEventStoreNew<>), typeof(CassandraEventStoreNew<>), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(CassandraEventStoreNew), typeof(CassandraEventStoreNew), ServiceLifetime.Transient);

            var cassandraSettings = context.FindService<ICassandraEventStoreSettings>();
            foreach (var setting in cassandraSettings)
            {
                yield return new DiscoveredModel(setting, setting, ServiceLifetime.Transient);
            }

            yield return new DiscoveredModel(typeof(IEventStoreJobIndex), typeof(NewEventToAggregateRootId), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(ICronusEventStoreIndex), typeof(NewEventToAggregateRootId), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(CassandraProvider), typeof(CassandraProvider), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(ICassandraProvider), provider => provider.GetRequiredService<SingletonPerTenant<CassandraProvider>>().Get(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IKeyspaceNamingStrategy), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(NoKeyspaceNamingStrategy), typeof(NoKeyspaceNamingStrategy), ServiceLifetime.Transient);
            yield return new DiscoveredModel(typeof(KeyspacePerTenantKeyspace), typeof(KeyspacePerTenantKeyspace), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(CassandraReplicationStrategyFactory), typeof(CassandraReplicationStrategyFactory), ServiceLifetime.Singleton);
            yield return new DiscoveredModel(typeof(ICassandraReplicationStrategy), provider => provider.GetRequiredService<CassandraReplicationStrategyFactory>().GetReplicationStrategy(), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IIndexStore), typeof(NewIndexByEventTypeStore), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(ICassandraEventStoreSchema), typeof(CassandraEventStoreSchemaNew), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(IMessageCounter), typeof(MessageCounter), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MessageCounter), typeof(MessageCounter), ServiceLifetime.Transient) { CanOverrideDefaults = true };

            yield return new DiscoveredModel(typeof(IRebuildIndex_EventToAggregateRootId_JobFactory), typeof(RebuildNewIndex_EventToAggregateRootId_JobFactory), ServiceLifetime.Transient);
        }
    }

    /*public static class CronusPersistenceCassandraServiceCollectionExtensions
    {
        public static IServiceCollection AddCronusEventStorePreview(this IServiceCollection services, IConfiguration configuration)
        {
            services.Replace(typeof(ITableNamingStrategy), typeof(TablePerBoundedContextNew));
            services.AddSingleton(typeof(TablePerBoundedContextNew), typeof(TablePerBoundedContextNew));
            services.AddSingleton(typeof(NoTableNamingStrategyStiopa), typeof(NoTableNamingStrategyStiopa));

            services.Replace(typeof(IEventStore<>), typeof(CassandraEventStoreNew<>));
            services.Replace(typeof(IEventStore), typeof(CassandraEventStoreNew));

            services.Replace(typeof(IEventStorePlayer<>), typeof(CassandraEventStoreNew<>));
            services.Replace(typeof(IEventStorePlayer), typeof(CassandraEventStoreNew));

            services.Replace(typeof(IIndexStore), typeof(NewIndexByEventTypeStore));

            services.AddTransient(typeof(IEventStoreJobIndex), typeof(NewEventToAggregateRootId));
            services.AddTransient(typeof(ICronusEventStoreIndex), typeof(NewEventToAggregateRootId));

            services.Replace(typeof(ICassandraEventStoreSchema), typeof(CassandraEventStoreSchemaNew));

            services.Replace(typeof (IRebuildIndex_EventToAggregateRootId_JobFactory), typeof(RebuildNewIndex_EventToAggregateRootId_JobFactory));    

            services.AddTransient(typeof(CassandraEventStoreNew<>), typeof(CassandraEventStoreNew<>));
            services.AddTransient(typeof(CassandraEventStoreNew), typeof(CassandraEventStoreNew));


            services.AddTransient(typeof(RebuildNewIndex_EventToAggregateRootId_JobFactory), typeof(RebuildNewIndex_EventToAggregateRootId_JobFactory));
            services.AddTransient(typeof(RebuildNewIndex_EventToAggregateRootId_Job), typeof(RebuildNewIndex_EventToAggregateRootId_Job));

            var toRemove = services.Where(x => x.ServiceType == typeof(TypeContainer<ICronusEventStoreIndex>)).Single();
            services.Remove(toRemove);
            *//*services.Remove(new DiscoveredModel(typeof(TypeContainer<ICronusEventStoreIndex>), typeof(TypeContainer<EventToAggregateRootId>)));
            services.Remove(new DiscoveredModel(typeof(EventToAggregateRootId), typeof(EventToAggregateRootId)));*//*


            DiscoveryContext context = new DiscoveryContext(AppDomain.CurrentDomain.GetAssemblies(), configuration);
            var loadedIndeces = context.Assemblies.FindExcept<IEventStoreJobIndex>(typeof(EventToAggregateRootId));

            foreach (var indexDef in loadedIndeces)
            {
                services.AddScoped(indexDef, indexDef);
            }

            var loadedCronusIndeces = context.Assemblies.FindExcept<ICronusEventStoreIndex>(typeof(EventToAggregateRootId));

            foreach (var indexDef in loadedCronusIndeces)
            {
                services.AddScoped(indexDef, indexDef);
            }

            services.Add(new DiscoveredModel(typeof(TypeContainer<IEventStoreIndex>), new TypeContainer<IEventStoreIndex>(loadedIndeces)));
            services.Add(new DiscoveredModel(typeof(TypeContainer<ICronusEventStoreIndex>), new TypeContainer<ICronusEventStoreIndex>(loadedCronusIndeces)));

            return services;
        }
    }*/
}
