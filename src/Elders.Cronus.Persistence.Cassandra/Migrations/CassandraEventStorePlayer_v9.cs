using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Migrations;
using Elders.Cronus.Projections;
using Elders.Cronus.Projections.Versioning;
using Elders.Cronus.Testing;

namespace Elders.Cronus.Persistence.Cassandra.Migrations
{
    public class CassandraEventStorePlayer_v9 : IMigrationEventStorePlayer, IProjectionVersionFinder
    {
        private readonly CassandraEventStore<MigratorCassandraReplaySettings> eventStore_v9;
        private readonly ICronusContextAccessor cronusContextAccessor;
        private readonly TypeContainer<IProjection> projectionsTypeContainer;

        public CassandraEventStorePlayer_v9(CassandraEventStore<MigratorCassandraReplaySettings> eventStore_v9, ICronusContextAccessor cronusContextAccessor, TypeContainer<IProjection> projectionsTypeContainer)
        {
            this.cronusContextAccessor = cronusContextAccessor;
            this.projectionsTypeContainer = projectionsTypeContainer;
            this.eventStore_v9 = eventStore_v9;
        }

        public IEnumerable<ProjectionVersion> GetProjectionVersionsToBootstrap()
        {
            foreach (Type projectionType in projectionsTypeContainer.Items)
            {
                var arId = new ProjectionVersionManagerId(projectionType.GetContractId(), cronusContextAccessor.CronusContext.Tenant);
                var projectionVersionManagerEventStream = eventStore_v9.LoadAsync(arId).GetAwaiter().GetResult();
                ProjectionVersionManager manager;
                bool success = projectionVersionManagerEventStream.TryRestoreFromHistory(out manager);
                if (success)
                {
                    var live = manager.RootState().Versions.GetLive();
                    if (live is not null)
                        yield return live;
                }
            }
        }

        public Task EnumerateEventStore(PlayerOperator @operator, PlayerOptions replayOptions)
        {
            return eventStore_v9.EnumerateEventStore(@operator, replayOptions);
        }
    }
}
