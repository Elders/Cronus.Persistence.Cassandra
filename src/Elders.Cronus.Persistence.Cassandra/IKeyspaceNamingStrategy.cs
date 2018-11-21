namespace Elders.Cronus.Persistence.Cassandra
{
    public interface IKeyspaceNamingStrategy
    {
        string GetName(string baseConfigurationKeyspace);
    }
}
