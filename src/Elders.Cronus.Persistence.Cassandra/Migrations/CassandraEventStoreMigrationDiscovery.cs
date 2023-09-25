using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Elders.Cronus.Discoveries;
using Elders.Cronus.EventStore;
using Elders.Cronus.Migrations;
using Elders.Cronus.Projections;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using static System.Formats.Asn1.AsnWriter;

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
            yield return new DiscoveredModel(typeof(MigratorCassandraProvider), typeof(MigratorCassandraProvider), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigratorCassandraReplaySettings), typeof(MigratorCassandraReplaySettings), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(MigrationCassandraProviderOptions), typeof(MigrationCassandraProviderOptions), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            yield return new DiscoveredModel(typeof(IMigrationEventStorePlayer), typeof(CassandraMigratorEventStorePlayer), ServiceLifetime.Transient) { CanOverrideDefaults = true };
            // yield return new DiscoveredModel(typeof(MigrationRunner<,>), typeof(MigrationRunner<,>), ServiceLifetime.Transient) { CanOverrideDefaults = true };
        }
    }

    public static class CronusMigratorServiceCollectionExtensions
    {
        public static IServiceCollection AddCronusMigratorFromV8toV9(this IServiceCollection services)
        {
            services.Replace<IMigrationCustomLogic, MigrateAggregateCommitFrom_Cronus_v8_to_v9>();
            services.AddTransient<CronusMigrator>();
            services.AddTransient<CassandraEventStorePlayer_v8>();
            services.AddTransient<IProjectionVersionFinder, CassandraEventStorePlayer_v8>();
            services.AddTransient<MigrateEventStore>();

            return services;
        }
    }

    public class MigrateEventStore
    {
        private readonly IServiceProvider _serviceProvider;
        private CassandraEventStorePlayer_v8 _source;
        private CronusMigrator _migrator;
        private readonly ILogger<MigrateEventStore> logger;

        public MigrateEventStore(IServiceProvider serviceProvider, ILogger<MigrateEventStore> logger)
        {
            _serviceProvider = serviceProvider;
            this.logger = logger;
        }

        public async Task RunMigratorAsync(string tenant)
        {
            using (var scope = _serviceProvider.CreateScope())
            {
                var cronusContextFactory = scope.ServiceProvider.GetRequiredService<Elders.Cronus.MessageProcessing.DefaultCronusContextFactory>();
                var cronusContext = cronusContextFactory.Create(tenant, scope.ServiceProvider);

                _source = scope.ServiceProvider.GetRequiredService<CassandraEventStorePlayer_v8>();
                _migrator = scope.ServiceProvider.GetRequiredService<CronusMigrator>();

                await RunAsync().ConfigureAwait(false);
            }
        }

        private async Task RunAsync()
        {
            var data = _source.LoadAggregateCommitsAsync(1000);

            try
            {
                List<Task> tasks = new List<Task>();

                await foreach (AggregateCommit sourceCommit in data)
                {
                    Task task = _migrator.MigrateAsync(sourceCommit);
                    tasks.Add(task);

                    if (tasks.Count > 100)
                    {
                        Task finished = await Task.WhenAny(tasks).ConfigureAwait(false);
                        tasks.Remove(finished);

                        if (Counter.DevidsBy100())
                            logger.LogInformation($"Migrator proccessed aggregate commits: {Counter.ProccesedCount}");
                        Counter.ProccesedCount++;
                    }
                }

                await Task.WhenAll(tasks).ConfigureAwait(false);
            }
            catch (System.Exception ex)
            {
                logger.ErrorException(ex, () => $"Something boom bam while runnning migration.");
            }
        }

        static class Counter
        {
            public static int ProccesedCount { get; set; } = 0;

            public static bool DevidsBy100() => ProccesedCount % 100 == 0;
        }
    }
}
