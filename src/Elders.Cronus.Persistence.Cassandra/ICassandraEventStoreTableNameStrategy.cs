using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreTableNameStrategy
    {
        string GetEventsTableName(string boundedContext);
    }
}
