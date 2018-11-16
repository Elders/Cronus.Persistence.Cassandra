namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ITableNamingStrategy
    {
        string GetName();
    }

    public interface IKeyspaceNamingStrategy
    {
        string GetName(string baseConfigurationKeyspace);
    }
}
