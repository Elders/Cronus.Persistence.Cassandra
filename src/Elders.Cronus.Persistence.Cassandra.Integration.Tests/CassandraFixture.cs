using System.Collections.Concurrent;
using Cassandra;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[assembly: AssemblyFixture(typeof(CassandraFixture))]

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraFixture : ICassandraProvider, IAsyncDisposable, IAsyncLifetime
{
    private readonly ConcurrentDictionary<string, ISession> sessionPerKeyspace = [];
    private ICluster cluster;
    private static readonly object mutex = new();

    //public const string DefaultKeyspace = "test_containers";

    public async ValueTask InitializeAsync()
    {
        Container = new ContainerBuilder()
            .WithImage("cassandra:4.1")
            .WithPortBinding(7000, true)
            .WithPortBinding(7001, true)
            .WithPortBinding(7199, true)
            .WithPortBinding(9042, true)
            .WithPortBinding(9160, true)
            .WithWaitStrategy(Wait.ForUnixContainer().UntilPortIsAvailable(9042))
            .Build();

        await Container.StartAsync();

        if (Instance is null)
            Instance = this;
    }

    public IContainer Container { get; private set; }

    public static CassandraFixture Instance { get; private set; }

    public async ValueTask DisposeAsync()
    {
        cluster?.Dispose();
        foreach (var session in sessionPerKeyspace)
            session.Value?.Dispose();

        if (Container != null)
        {
            await Container.DisposeAsync();
        }
    }

    public Task<ICluster> GetClusterAsync()
    {
        if (cluster == null)
        {
            lock (mutex)
            {
                if (cluster == null)
                {
                    cluster = global::Cassandra.Cluster.Builder()
                        .AddContactPoint(Container.Hostname)
                        .WithTypeSerializers(new global::Cassandra.Serialization.TypeSerializerDefinitions().Define(new ReadOnlyMemoryTypeSerializer()))
                        .WithPort(Container.GetMappedPublicPort(9042))
                        .Build();
                }
            }
        }

        return Task.FromResult(cluster);
    }

    public Task<ISession> GetSessionAsync()
    {
        var className = TestContext.Current.TestClass.TestClassName
            .Skip(TestContext.Current.TestClass.TestClassName.LastIndexOf('.') + 1)
            .Take(48)
            .ToArray();

        return GetSessionAsync(new string(className));
    }

    public async Task<ISession> GetSessionAsync(string keyspace)
    {
        ISession session = null;
        if (sessionPerKeyspace.TryGetValue(keyspace, out session) == false)
        {
            var cluster = await GetClusterAsync();
            session = await cluster.ConnectAsync();
            session.CreateKeyspaceIfNotExists(keyspace, new Dictionary<string, string>
                {
                    { "class", "SimpleStrategy" },
                    { "replication_factor", "1" }
                });
            session.ChangeKeyspace(keyspace);

            sessionPerKeyspace.TryAdd(keyspace, session);
        }

        return session;
    }
}
