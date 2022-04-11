using System;
using Cassandra;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using System.Threading.Tasks;
using Elders.Cronus.AtomicAction;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        protected CassandraProviderOptions options;
        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly ICassandraReplicationStrategy replicationStrategy;
        protected readonly IInitializer initializer;
        private readonly ILock @lock;
        private readonly TimeSpan lockTtl;

        protected ICluster cluster;
        protected ISession session;

        private static object establishNewConnection = new object();

        private string baseConfigurationKeyspace;

        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ICassandraReplicationStrategy replicationStrategy, IInitializer initializer, ILock @lock)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));
            if (replicationStrategy is null) throw new ArgumentNullException(nameof(replicationStrategy));

            this.options = optionsMonitor.CurrentValue;
            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.replicationStrategy = replicationStrategy;
            this.initializer = initializer;
            this.@lock = @lock;

            this.lockTtl = TimeSpan.FromSeconds(2);
            if (lockTtl == TimeSpan.Zero) throw new ArgumentException("Lock ttl must be more than 0", nameof(lockTtl));
        }

        public async Task<ICluster> GetClusterAsync()
        {
            if (cluster is null == false)
                return cluster;

            Builder builder = initializer as Builder;
            if (builder is null)
            {
                builder = DataStax.Cluster.Builder();
                //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                string connectionString = options.ConnectionString;

                var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                await (cluster?.ShutdownAsync(30000)).ConfigureAwait(false);
                cluster = connStrBuilder
                    .ApplyToBuilder(builder)
                    .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                    .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                    .Build();

                await cluster.RefreshSchemaAsync().ConfigureAwait(false);
            }

            else
            {
                cluster = DataStax.Cluster.BuildFrom(initializer);
            }

            return cluster;
        }

        protected virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        public async Task<ISession> GetSessionAsync()
        {
            if (session is null || session.IsDisposed)
            {
                if (@lock.Lock(session.ToString(), lockTtl))
                {
                    try
                    {
                        ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                        session = await cluster.ConnectAsync(GetKeyspace());
                    }
                    catch (InvalidQueryException)
                    {
                        ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                        using (ISession schemaSession = await cluster.ConnectAsync().ConfigureAwait(false))
                        {
                            string createKeySpaceQuery = replicationStrategy.CreateKeySpaceTemplate(GetKeyspace());
                            IStatement createTableStatement = await GetKreateKeySpaceQuery(schemaSession).ConfigureAwait(false);
                            await schemaSession.ExecuteAsync(createTableStatement).ConfigureAwait(false);
                        }

                        ICluster server = await GetClusterAsync().ConfigureAwait(false);
                        session = await server.ConnectAsync(GetKeyspace()).ConfigureAwait(false);
                    }
                }
            }

            return session;
        }

        private async Task<IStatement> GetKreateKeySpaceQuery(ISession schemaSession)
        {
            PreparedStatement createEventsTableStatement = await schemaSession.PrepareAsync(replicationStrategy.CreateKeySpaceTemplate(GetKeyspace())).ConfigureAwait(false);
            createEventsTableStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);

            return createEventsTableStatement.Bind();
        }
    }

    class NoHintedHandOffRetryPolicy : IRetryPolicy
    {
        public RetryDecision OnReadTimeout(IStatement query, ConsistencyLevel cl, int requiredResponses, int receivedResponses, bool dataRetrieved, int nbRetry)
        {
            if (nbRetry != 0)
                return RetryDecision.Rethrow();

            return receivedResponses >= requiredResponses && !dataRetrieved
                       ? RetryDecision.Retry(cl)
                       : RetryDecision.Rethrow();
        }

        public RetryDecision OnUnavailable(IStatement query, ConsistencyLevel cl, int requiredReplica, int aliveReplica, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }

        public RetryDecision OnWriteTimeout(IStatement query, ConsistencyLevel cl, string writeType, int requiredAcks, int receivedAcks, int nbRetry)
        {
            return RetryDecision.Rethrow();
        }
    }
}
