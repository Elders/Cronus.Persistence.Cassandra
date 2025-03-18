using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Options;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

using System;
using Xunit;

public class PartitionCalculatorTests
{
    [Theory]
    [InlineData(2024074, 2024075)] // March 14, 2024 -> March 15, 2024
    [InlineData(2024075, 2024076)] // March 15, 2024 -> March 16, 2024
    [InlineData(2023364, 2023365)] // Dec 30, 2023 -> Dec 31, 2023 (Non-leap year)
    [InlineData(2023365, 2024001)] // Dec 31, 2023 -> Jan 1, 2024 (Year transition)
    [InlineData(2024365, 2024366)] // Dec 30, 2024 -> Dec 31, 2024 (Leap year)
    [InlineData(2024366, 2025001)] // Dec 31, 2024 (Leap year) -> Jan 1, 2025 (Year transition)
    public void GetNext_ShouldReturnNextDayPartition(int currentPartition, int expectedNextPartition)
    {
        // Act
        int nextPartition = PartitionCalculator.GetNext(currentPartition);

        // Assert
        Assert.Equal(expectedNextPartition, nextPartition);
    }
}

public class CassandraEventStoreSchemaTests
{
    private readonly CassandraFixture cassandraFixture;

    public CassandraEventStoreSchemaTests(CassandraFixture cassandraFixture)
    {
        this.cassandraFixture = cassandraFixture;
    }

    [Fact]
    public async Task CreateStorageWithNoTableNamingAsync()
    {
        var naming = new NoTableNamingStrategy();
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        var schema = new CassandraEventStoreSchemaFixture(cassandraFixture).GetEventStoreSchema(naming, replicatoinStrategy);

        await schema.CreateStorageAsync();

        var session = await cassandraFixture.GetSessionAsync();
        var cluster = await cassandraFixture.GetClusterAsync();
        var tables = cluster.Metadata.GetTables(session.Keyspace);

        Assert.Contains(naming.GetName(), tables);
        Assert.Contains("index_by_eventtype", tables);
        Assert.Contains("message_counter", tables);
    }

    [Fact]
    public async Task CreateStorageWithTablePerBoundedContextAsync()
    {
        var bc = new BoundedContext { Name = "tests" };
        var naming = new TablePerBoundedContext(new TablePerBoundedContextOptionsMonitorMock(bc));
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        var schema = new CassandraEventStoreSchemaFixture(cassandraFixture).GetEventStoreSchema(naming, replicatoinStrategy);

        await schema.CreateStorageAsync();

        var session = await cassandraFixture.GetSessionAsync();
        var cluster = await cassandraFixture.GetClusterAsync();
        var tables = cluster.Metadata.GetTables(session.Keyspace);

        Assert.Contains(naming.GetName(), tables);
        Assert.Contains("index_by_eventtype", tables);
        Assert.Contains("message_counter", tables);
    }
}

class TablePerBoundedContextOptionsMonitorMock : IOptionsMonitor<BoundedContext>
{
    private readonly BoundedContext boundedContext;

    public TablePerBoundedContextOptionsMonitorMock(BoundedContext boundedContext)
    {
        this.boundedContext = boundedContext;
    }

    public BoundedContext CurrentValue => boundedContext;

    public BoundedContext Get(string name)
    {
        return boundedContext;
    }

    public IDisposable OnChange(Action<BoundedContext, string> listener)
    {
        listener(boundedContext, null);
        return null;
    }
}
