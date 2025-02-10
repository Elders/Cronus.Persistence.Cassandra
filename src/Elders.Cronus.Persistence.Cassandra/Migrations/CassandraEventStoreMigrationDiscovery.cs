using System.Collections.Generic;
using System.Linq;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.Migrations;
using Elders.Cronus.Projections;
using Microsoft.Extensions.DependencyInjection;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
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
            yield return new DiscoveredModel(typeof(CassandraEventStore<>), typeof(CassandraEventStore<>), ServiceLifetime.Transient);

            yield return new DiscoveredModel(typeof(MigratorCassandraProvider), typeof(MigratorCassandraProvider), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigratorCassandraReplaySettings), typeof(MigratorCassandraReplaySettings), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigrationCassandraProviderOptions), typeof(MigrationCassandraProviderOptions), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IMigrationEventStorePlayer), typeof(CassandraMigratorEventStorePlayer), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        }
    }
}
