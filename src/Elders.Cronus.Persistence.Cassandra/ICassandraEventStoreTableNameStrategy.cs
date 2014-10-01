namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraEventStoreTableNameStrategy
    {
        string GetEventsTableName<AR>() where AR : DomainModeling.IAggregateRoot;
        string GetEventsTableName<AR>(AR aggregate) where AR : DomainModeling.IAggregateRoot;

        string[] GetAllTableNames();
    }
}