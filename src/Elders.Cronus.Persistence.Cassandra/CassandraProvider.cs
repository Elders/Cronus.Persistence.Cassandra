using System;
using System.Threading;
using System.Threading.Tasks;
using Cassandra;
using Cassandra.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using DataStax = Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class CassandraProvider : ICassandraProvider
    {
        protected CassandraProviderOptions options;
        protected readonly IKeyspaceNamingStrategy keyspaceNamingStrategy;
        protected readonly IInitializer initializer;
        protected readonly ILogger<CassandraProvider> logger;

        protected ICluster cluster;
        protected ISession session;
        protected ISession sessionWithLongTimeout;

        private string baseConfigurationKeyspace;
        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
        {
            if (optionsMonitor is null) throw new ArgumentNullException(nameof(optionsMonitor));
            if (keyspaceNamingStrategy is null) throw new ArgumentNullException(nameof(keyspaceNamingStrategy));

            this.options = optionsMonitor.CurrentValue;
            this.keyspaceNamingStrategy = keyspaceNamingStrategy;
            this.initializer = initializer;
            this.logger = logger;
        }

        private static SemaphoreSlim clusterThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

        public async Task<ICluster> GetClusterAsync()
        {
            if (cluster is null == false)
                return cluster;

            bool lockSuccess = false;

            try
            {
                lockSuccess = await clusterThreadGate.WaitAsync(30000).ConfigureAwait(false);
                if (lockSuccess == false)
                    throw new TimeoutException("Timeout while waiting for cluster lock.");

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

                    int ThirthySeconds = 1000 * 30;
                    SocketOptions so = new SocketOptions();
                    so.SetReadTimeoutMillis(ThirthySeconds);
                    so.SetStreamMode(true);
                    so.SetTcpNoDelay(true);

                    cluster = connStrBuilder
                        .ApplyToBuilder(builder)
                        .WithSocketOptions(so)
                        .WithTypeSerializers(new TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                        .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                        .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                        .WithCompression(CompressionType.LZ4)
                        .WithPoolingOptions(new PoolingOptions()
                            .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                            .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                            .SetMaxRequestsPerConnection(options.MaxRequestsPerConnection))
                        .Build();

                    await cluster.RefreshSchemaAsync().ConfigureAwait(false);
                }
                else
                {
                    cluster = DataStax.Cluster.BuildFrom(initializer);
                }

                return cluster;
            }
            finally
            {
                if (lockSuccess)
                {
                    clusterThreadGate?.Release();
                }
            }
        }

        private static SemaphoreSlim longSessionThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

        internal async Task<ISession> GetSessionHighTimeoutAsync()
        {
            if (sessionWithLongTimeout is null || sessionWithLongTimeout.IsDisposed)
            {
                bool lockSuccess = false;

                try
                {
                    lockSuccess = await longSessionThreadGate.WaitAsync(30000).ConfigureAwait(false);
                    if (lockSuccess == false)
                        throw new TimeoutException("Timeout while waiting for session lock.");

                    int TenMinutes = 1000 * 60 * 10;
                    SocketOptions so = new SocketOptions();
                    so.SetConnectTimeoutMillis(TenMinutes);
                    so.SetReadTimeoutMillis(TenMinutes);
                    so.SetStreamMode(true);
                    so.SetTcpNoDelay(true);

                    Builder builder = DataStax.Cluster.Builder();
                    builder = builder.WithSocketOptions(so);

                    string connectionString = options.ConnectionString;

                    var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                    if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                        connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                    baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                    var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                    cluster = connStrBuilder
                        .ApplyToBuilder(builder)
                        .Build();

                    sessionWithLongTimeout = await cluster.ConnectAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (lockSuccess)
                    {
                        longSessionThreadGate?.Release();
                    }
                }
            }

            return sessionWithLongTimeout;

        }

        public virtual string GetKeyspace()
        {
            return keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        private static SemaphoreSlim sessionThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time


        public async Task<ISession> GetSessionAsync()
        {
            if (session is null || session.IsDisposed)
            {
                bool lockSuccess = false;
                try
                {
                    lockSuccess = await sessionThreadGate.WaitAsync(30000).ConfigureAwait(false);
                    if (lockSuccess == false)
                        throw new TimeoutException("Timeout while waiting for session lock.");

                    if (session is null || session.IsDisposed)
                    {
                        if (logger.IsEnabled(LogLevel.Information))
                            logger.LogInformation("Refreshing cassandra session...");

                        ICluster cluster = await GetClusterAsync().ConfigureAwait(false);
                        session = await cluster.ConnectAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (lockSuccess)
                    {
                        sessionThreadGate?.Release();
                    }
                }
            }

            return session;
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

    class ReadOnlyMemoryTypeSerializer : CustomTypeSerializer<ReadOnlyMemory<byte>>
    {
        public ReadOnlyMemoryTypeSerializer() : base("it doesn't matter") { }

        public override ReadOnlyMemory<byte> Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
            => buffer.AsMemory(offset, length); // we will never get here because the byte[] serializer kicks in

        public override byte[] Serialize(ushort protocolVersion, ReadOnlyMemory<byte> value) => value.ToArray();
    }
}
