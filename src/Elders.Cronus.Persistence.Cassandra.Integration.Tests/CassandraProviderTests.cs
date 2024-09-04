using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraProviderTests
{
    private readonly CassandraFixture cassandraFixture;
    private readonly NullLoggerFixture nullLoggerFixture;

    public CassandraProviderTests(CassandraFixture cassandraFixture, NullLoggerFixture nullLoggerFixture)
    {
        this.cassandraFixture = cassandraFixture;
        this.nullLoggerFixture = nullLoggerFixture;
    }

    [Fact]
    public async Task GetClusterAsync()
    {
        var options = new CassandraProviderOptions
        {
            ConnectionString = $"Contact Points={cassandraFixture.Container.Hostname};Port={cassandraFixture.Container.GetMappedPublicPort(9042)};Default Keyspace=test_containers",
            Datacenters = ["datacenter1"],
            ReplicationFactor = 1,
            ReplicationStrategy = "simple"
        };
        var keyspaceNaming = new NoKeyspaceNamingStrategy();
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        var nullLogger = nullLoggerFixture.CreateLogger<CassandraProvider>();
        var provider = new CassandraProvider(new CassandraProviderOptionsMonitorMock(options), keyspaceNaming, replicatoinStrategy, nullLogger);

        var cluster = await provider.GetClusterAsync();

        Assert.NotNull(cluster);

        var hosts = cluster.AllHosts();
        Assert.Single(hosts);
        Assert.True(hosts.First().IsUp);
        Assert.Equal(cassandraFixture.Container.Hostname, hosts.First().Address.Address.ToString());
    }

    [Fact]
    public async Task GetSessionWithNoKeyspaceNamingAsync()
    {
        var options = new CassandraProviderOptions
        {
            ConnectionString = $"Contact Points={cassandraFixture.Container.Hostname};Port={cassandraFixture.Container.GetMappedPublicPort(9042)};Default Keyspace=test_containers",
            Datacenters = ["datacenter1"],
            ReplicationFactor = 1,
            ReplicationStrategy = "simple"
        };
        var keyspaceNaming = new NoKeyspaceNamingStrategy();
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        var nullLogger = new NullLoggerFactory().CreateLogger<CassandraProvider>();
        var provider = new CassandraProvider(new CassandraProviderOptionsMonitorMock(options), keyspaceNaming, replicatoinStrategy, nullLogger);

        var session = await provider.GetSessionAsync();

        Assert.NotNull(session);
        Assert.False(session.IsDisposed);
        Assert.Equal(keyspaceNaming.GetName("test_containers"), session.Keyspace);
    }

    [Fact]
    public async Task GetSessionWithKeyspacePerTenantNamingAsync()
    {
        var options = new CassandraProviderOptions
        {
            ConnectionString = $"Contact Points={cassandraFixture.Container.Hostname};Port={cassandraFixture.Container.GetMappedPublicPort(9042)};Default Keyspace=test_containers",
            Datacenters = ["datacenter1"],
            ReplicationFactor = 1,
            ReplicationStrategy = "simple"
        };
        var accessor = new CronusContextAccessorMock
        {
            CronusContext = new CronusContext("tests", new NullServiceProviderMock())
        };
        var keyspaceNaming = new KeyspacePerTenantKeyspace(accessor);
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        var nullLogger = new NullLoggerFactory().CreateLogger<CassandraProvider>();
        var provider = new CassandraProvider(new CassandraProviderOptionsMonitorMock(options), keyspaceNaming, replicatoinStrategy, nullLogger);

        var session = await provider.GetSessionAsync();

        Assert.NotNull(session);
        Assert.False(session.IsDisposed);
        Assert.Equal(keyspaceNaming.GetName("test_containers"), session.Keyspace);
    }
}

class CassandraProviderOptionsMonitorMock : IOptionsMonitor<CassandraProviderOptions>
{
    private readonly CassandraProviderOptions options;

    public CassandraProviderOptionsMonitorMock(CassandraProviderOptions options)
    {
        this.options = options;
    }

    public CassandraProviderOptions CurrentValue => options;

    public CassandraProviderOptions Get(string name)
    {
        return options;
    }

    public IDisposable OnChange(Action<CassandraProviderOptions, string> listener)
    {
        listener(options, null);
        return null;
    }
}

class CronusContextAccessorMock : ICronusContextAccessor
{
    public CronusContext CronusContext { get; set; }
}

class NullServiceProviderMock : IServiceProvider
{
    public object GetService(Type serviceType)
    {
        return null;
    }
}
