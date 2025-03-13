using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using System.Reflection;
using Xunit.v3;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
public class EnsureEventStoreAttribute : BeforeAfterTestAttribute
{
    /// <summary>
    /// In xunit.v3 this method is called before the test method is called. In version 0.3-pre it was async,
    /// but in the stable version it is not. If we didn't wait to finish the schema creation, the test will start before the schema is created which causes issues.
    /// </summary>
    /// <param name="methodUnderTest"></param>
    /// <param name="test"></param>
    public override void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        var schemaFixture = new CassandraEventStoreSchemaFixture(CassandraFixture.Instance);
        var replicatoinStrategy = new SimpleReplicationStrategy(1);
        schemaFixture.GetEventStoreSchema(new NoTableNamingStrategy(), replicatoinStrategy).CreateStorageAsync().GetAwaiter().GetResult();
    }
}
