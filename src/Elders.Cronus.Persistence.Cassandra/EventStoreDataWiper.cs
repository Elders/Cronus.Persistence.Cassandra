using System;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.DangerZone;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra;

public class EventStoreDataWiper : IDangerZone
{
    private readonly ICassandraProvider cassandraProvider;
    private readonly ICronusContextAccessor cronusContextAccessor;
    private readonly ILogger<EventStoreDataWiper> logger;

    private DropKeyspaceQuery _dropKeyspaceQuery;

    private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync(); // In order to keep only 1 session alive (https://docs.datastax.com/en/developer/csharp-driver/3.16/faq/)

    public EventStoreDataWiper(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ILogger<EventStoreDataWiper> logger)
    {
        if (cassandraProvider is null) throw new ArgumentNullException(nameof(cassandraProvider));

        this.cassandraProvider = cassandraProvider;
        this.cronusContextAccessor = cronusContextAccessor;
        this.logger = logger;

        _dropKeyspaceQuery = new DropKeyspaceQuery(cronusContextAccessor, cassandraProvider);
    }

    public async Task WipeDataAsync(string tenant)
    {
        try
        {
            if (tenant.Equals(cronusContextAccessor.CronusContext.Tenant, StringComparison.Ordinal) == false)
            {
                logger.LogError("Tenant mismatch. The tenant to be wiped is different from the current tenant.");
                return;
            }

            ISession session = await GetSessionAsync().ConfigureAwait(false);
            PreparedStatement statement = await _dropKeyspaceQuery.PrepareAsync(session).ConfigureAwait(false);

            logger.LogInformation("Wiping eventstore data for tenant {tenant} query {query}", tenant, statement.QueryString);

            var bs = statement.Bind().SetIdempotence(true);
            await session.ExecuteAsync(bs).ConfigureAwait(false);

            logger.LogInformation("Eventstore data for tenant {tenant} wiped", tenant);
        }
        catch (Exception ex)
        {
            logger.LogError(ex, "Failed to drop keyspace.");
            throw;
        }
    }

    class DropKeyspaceQuery : PreparedStatementCache
    {
        private const string Template = @"DROP KEYSPACE {0};";

        public DropKeyspaceQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }
}
