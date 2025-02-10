using System.Reflection;
using Xunit.v3;

namespace Elders.Cronus.Persistence.Cassandra.Integration.Tests;

[AttributeUsage(AttributeTargets.Assembly | AttributeTargets.Class | AttributeTargets.Method, AllowMultiple = true, Inherited = true)]
public class EnsureEventStoreAttribute : BeforeAfterTestAttribute
{
    public override async void Before(MethodInfo methodUnderTest, IXunitTest test)
    {
        var schemaFixture = new CassandraEventStoreSchemaFixture(CassandraFixture.Instance);
        await schemaFixture.GetEventStoreSchema(new NoTableNamingStrategy()).CreateStorageAsync();
    }
}
