using Elders.Cronus.AtomicAction;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;

namespace Elders.Cronus.Persistence.Cassandra
{
    [CronusStartup(Bootstraps.ExternalResource)]
    public class CronusCassandraEventStoreStartup : ICronusStartup
    {
        private readonly ILogger<CronusCassandraEventStoreStartup> logger;
        private readonly IServiceProvider serviceProvider;
        private readonly ILock @lock;
        private TenantsOptions tenants;
        private BoundedContext bc;
        private readonly TimeSpan lockTtl;

        public CronusCassandraEventStoreStartup(IOptionsMonitor<BoundedContext> bc, IServiceProvider serviceProvider, IOptionsMonitor<TenantsOptions> tenantsOptions, ILock @lock, ILogger<CronusCassandraEventStoreStartup> logger)
        {
            this.serviceProvider = serviceProvider;
            this.tenants = tenantsOptions.CurrentValue;
            this.logger = logger;
            this.bc = bc.CurrentValue;
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        public void Bootstrap()
        {
            string lockKey = $"{bc.Name}{Enum.GetName(typeof(Bootstraps), Bootstraps.ExternalResource)}";
            if (@lock.LockAsync(lockKey, lockTtl).GetAwaiter().GetResult())
            {
                foreach (var tenant in tenants.Tenants)
                {
                    using (var scope = serviceProvider.CreateScope())
                    {
                        CronusContextFactory contextFactory = scope.ServiceProvider.GetRequiredService<CronusContextFactory>();
                        CronusContext context = contextFactory.GetContext(tenant, scope.ServiceProvider);

                        scope.ServiceProvider.GetRequiredService<ICassandraEventStoreSchema>().CreateStorageAsync().GetAwaiter().GetResult();
                    }
                }

                @lock.UnlockAsync(lockKey).GetAwaiter().GetResult();
            }
            else
            {
                logger.Warn(() => $"[EventStore] Could not acquire lock for `{bc.Name}` to create table.");
            }
        }
    }
}
