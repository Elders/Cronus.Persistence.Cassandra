using System;
using Elders.Cronus.MessageProcessing;
using Elders.Cronus.Persistence.Cassandra.ReplicationStrategies;
using Microsoft.Extensions.Configuration;
using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public interface ICassandraProvider
    {
        Cluster GetCluster();
        ISession GetSession();
    }
}
