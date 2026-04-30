using Elders.Cronus.AtomicAction;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Multitenancy;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

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
            this.bc = bc.CurrentValue; // We decide that changing the bounded context is not supported, because if we change it runtime we could have a lot of problems.
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));

            tenantsOptions.OnChange(OptionsChangedBootstrapEventStoreForTenant);
        }

        /// <summary>
        /// Bootstraps the Cassandra event store schema for all configured tenants.
        /// </summary>
        /// <param name="cancellationToken">Token used to cancel the bootstrap operation.</param>
        public async Task BootstrapAsync(CancellationToken cancellationToken = default)
        {
            await BootstrapTenantsAsync(tenants.Tenants, cancellationToken).ConfigureAwait(false);
        }

        private async Task BootstrapTenantsAsync(IEnumerable<string> tenants, CancellationToken cancellationToken = default)
        {
            string lockKey = $"{bc.Name}{Enum.GetName(typeof(Bootstraps), Bootstraps.ExternalResource)}";
            if (await @lock.LockAsync(lockKey, lockTtl).ConfigureAwait(false))
            {
                foreach (var tenant in tenants)
                {
                    DefaultCronusContextFactory contextFactory = serviceProvider.GetRequiredService<DefaultCronusContextFactory>();
                    CronusContext context = contextFactory.Create(tenant, serviceProvider);

                    await serviceProvider.GetRequiredService<CassandraEventStoreSchema>().CreateStorageAsync().ConfigureAwait(false);
                }

                await @lock.UnlockAsync(lockKey).ConfigureAwait(false);
            }
            else
            {
                logger.LogWarning("[EventStore] Could not acquire lock for `{boundedContext}` to create table.", bc.Name);
            }
        }

        private void OptionsChangedBootstrapEventStoreForTenant(TenantsOptions newOptions)
        {
            if (tenants.Tenants.SequenceEqual(newOptions.Tenants))
                return;

            if (logger.IsEnabled(LogLevel.Debug))
                logger.LogDebug("Cronus tenant options re-loaded with {@options}", newOptions);

            // Find the difference between the old and new tenants and bootstrap the new ones.
            // The IOptionsMonitor.OnChange callback is sync (Action<T>) so the async bootstrap
            // is fire-and-forget; exceptions are caught and logged inside the helper, and
            // the local tenants snapshot only commits to the new value on success so a failed
            // bootstrap will be retried on the next options reload.
            var newTenants = newOptions.Tenants.Except(tenants.Tenants).ToList();
            _ = BootstrapAndCommitAsync(newOptions, newTenants);
        }

        private async Task BootstrapAndCommitAsync(TenantsOptions newOptions, List<string> newTenants)
        {
            try
            {
                await BootstrapTenantsAsync(newTenants).ConfigureAwait(false);
                tenants = newOptions;
            }
            catch (Exception ex)
            {
                logger.LogError(ex, "Failed to bootstrap event store for new tenants: {tenants}", string.Join(", ", newTenants));
            }
        }
    }
}
