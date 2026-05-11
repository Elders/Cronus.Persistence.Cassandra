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
        private readonly CassandraProviderOptions _options;
        private readonly IKeyspaceNamingStrategy _keyspaceNamingStrategy;
        private readonly IInitializer _initializer;
        private readonly ILogger<CassandraProvider> _logger;

        private ICluster _cluster;
        private ISession _session;
        private ISession _sessionWithLongTimeout;

        private string baseConfigurationKeyspace;

        private static readonly SemaphoreSlim ClusterThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time
        private static readonly SemaphoreSlim LongSessionThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time
        private static readonly SemaphoreSlim SessionThreadGate = new SemaphoreSlim(1, 1); // Instantiate a Singleton of the Semaphore with a value of 1. This means that only 1 thread can be granted access at a time

        public CassandraProvider(IOptionsMonitor<CassandraProviderOptions> optionsMonitor, IKeyspaceNamingStrategy keyspaceNamingStrategy, ILogger<CassandraProvider> logger, IInitializer initializer = null)
        {
            ArgumentNullException.ThrowIfNull(optionsMonitor);
            ArgumentNullException.ThrowIfNull(keyspaceNamingStrategy);

            _options = optionsMonitor.CurrentValue;
            _keyspaceNamingStrategy = keyspaceNamingStrategy;
            _initializer = initializer;
            _logger = logger;
        }


        public async Task<ICluster> GetClusterAsync()
        {
            if (_cluster is not null)
                return _cluster;

            bool lockSuccess = false;

            try
            {
                lockSuccess = await ClusterThreadGate.WaitAsync(30000).ConfigureAwait(false);
                if (lockSuccess is false)
                    throw new TimeoutException("Timeout while waiting for cluster lock.");

                if (_cluster is not null)
                    return _cluster;

                var builder = _initializer as Builder;
                if (builder == null)
                {
                    builder = DataStax.Cluster.Builder();
                    //  TODO: check inside the `cfg` (var cfg = builder.GetConfiguration();) if we already have connectionString specified

                    string connectionString = _options.ConnectionString;

                    var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                    if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                    {
                        connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                        baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;
                    }
                    else
                    {
                        baseConfigurationKeyspace = _options.DefaultKeyspace;
                    }

                    var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                    const int thirtySeconds = 1000 * 30;
                    SocketOptions so = new SocketOptions();
                    so.SetReadTimeoutMillis(thirtySeconds);
                    so.SetStreamMode(true);
                    so.SetTcpNoDelay(true);

                    _cluster = connStrBuilder
                        .ApplyToBuilder(builder)
                        .WithSocketOptions(so)
                        .WithTypeSerializers(new TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                        .WithReconnectionPolicy(new ExponentialReconnectionPolicy(100, 100000))
                        .WithRetryPolicy(new NoHintedHandOffRetryPolicy())
                        .WithCompression(CompressionType.LZ4)
                        .WithPoolingOptions(new PoolingOptions()
                            .SetCoreConnectionsPerHost(HostDistance.Local, 2)
                            .SetMaxConnectionsPerHost(HostDistance.Local, 8)
                            .SetMaxRequestsPerConnection(_options.MaxRequestsPerConnection))
                        .Build();

                    await _cluster.RefreshSchemaAsync().ConfigureAwait(false);
                }
                else
                {
                    _cluster = DataStax.Cluster.BuildFrom(_initializer);
                }

                return _cluster;
            }
            finally
            {
                if (lockSuccess)
                {
                    ClusterThreadGate?.Release();
                }
            }
        }

        public virtual string GetKeyspace()
        {
            return _keyspaceNamingStrategy.GetName(baseConfigurationKeyspace).ToLower();
        }

        public async Task<ISession> GetSessionAsync()
        {
            if (_session is null || _session.IsDisposed)
            {
                bool lockSuccess = false;
                try
                {
                    lockSuccess = await SessionThreadGate.WaitAsync(30000).ConfigureAwait(false);
                    if (lockSuccess == false)
                        throw new TimeoutException("Timeout while waiting for session lock.");

                    if (_session is null || _session.IsDisposed)
                    {
                        if (_logger.IsEnabled(LogLevel.Information))
                            _logger.LogInformation("Refreshing cassandra session...");

                        ICluster cassandraCluster = await GetClusterAsync().ConfigureAwait(false);
                        _session = await cassandraCluster.ConnectAsync().ConfigureAwait(false);
                    }
                }
                finally
                {
                    if (lockSuccess)
                    {
                        SessionThreadGate?.Release();
                    }
                }
            }

            return _session;
        }

        // TODO: check if this is really needed, otherwise it can be deleted.
        internal async Task<ISession> GetSessionHighTimeoutAsync()
        {
            if (_sessionWithLongTimeout is null || _sessionWithLongTimeout.IsDisposed)
            {
                bool lockSuccess = false;

                try
                {
                    lockSuccess = await LongSessionThreadGate.WaitAsync(30000).ConfigureAwait(false);
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

                    string connectionString = _options.ConnectionString;

                    var hackyBuilder = new CassandraConnectionStringBuilder(connectionString);
                    if (string.IsNullOrEmpty(hackyBuilder.DefaultKeyspace) == false)
                        connectionString = connectionString.Replace(hackyBuilder.DefaultKeyspace, string.Empty);
                    baseConfigurationKeyspace = hackyBuilder.DefaultKeyspace;

                    var connStrBuilder = new CassandraConnectionStringBuilder(connectionString);

                    _cluster = connStrBuilder
                        .ApplyToBuilder(builder)
                        .Build();

                    _sessionWithLongTimeout = await _cluster.ConnectAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (lockSuccess)
                    {
                        LongSessionThreadGate?.Release();
                    }
                }
            }

            return _sessionWithLongTimeout;
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
        public ReadOnlyMemoryTypeSerializer() : base("it doesn't matter")
        {
        }

        public override ReadOnlyMemory<byte> Deserialize(ushort protocolVersion, byte[] buffer, int offset, int length, IColumnInfo typeInfo)
            => buffer.AsMemory(offset, length); // we will never get here because the byte[] serializer kicks in

        public override byte[] Serialize(ushort protocolVersion, ReadOnlyMemory<byte> value) => value.ToArray();
    }
}
