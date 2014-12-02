using Elders.Cronus.EventStore;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreTableNameStrategy
    {
        string GetEventsTableName(AggregateCommit aggregateCommit);
        string GetEventsTableName(string boundedContext);
        string[] GetAllTableNames();
    }
}