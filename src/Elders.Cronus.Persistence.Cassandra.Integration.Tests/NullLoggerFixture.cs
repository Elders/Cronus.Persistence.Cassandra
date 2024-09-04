using Elders.Cronus.Persistence.Cassandra.Integration.Tests;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

[assembly: AssemblyFixture(typeof(NullLoggerFixture))]

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

public class NullLoggerFixture
{
    private ILoggerFactory loggerFactory;

    public NullLoggerFixture()
    {
        loggerFactory = new NullLoggerFactory();
    }

    public ILogger<T> CreateLogger<T>() => loggerFactory.CreateLogger<T>();
}
