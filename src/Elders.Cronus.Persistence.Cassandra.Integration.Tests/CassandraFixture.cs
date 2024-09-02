using Cassandra;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[assembly: AssemblyFixture(typeof(CassandraFixture))]

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class CassandraFixture : ICassandraProvider, IAsyncDisposable
{
    private ISession session;
    private ICluster cluster;
    private static readonly object mutex = new();

    public CassandraFixture()
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

        Container.StartAsync().GetAwaiter().GetResult();
    }

    public IContainer Container { get; private set; }

    public async ValueTask DisposeAsync()
    {
        if (Container != null)
        {
            await Container.DisposeAsync();
        }

        cluster?.Dispose();
        session?.Dispose();
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
                        .WithPort(Container.GetMappedPublicPort(9042))
                        .Build();
                }
            }
        }

        return Task.FromResult(cluster);
    }

    public Task<ISession> GetSessionAsync()
    {
        if (session == null)
        {
            lock (mutex)
            {
                if (session == null)
                {
                    var cluster = GetClusterAsync().GetAwaiter().GetResult();
                    session = cluster.Connect();
                    session.CreateKeyspaceIfNotExists("test_containers", new Dictionary<string, string>
                    {
                        { "class", "SimpleStrategy" },
                        { "replication_factor", "1" }
                    });
                    session.ChangeKeyspace("test_containers");
                }
            }
        }

        return Task.FromResult(session);
    }
}
