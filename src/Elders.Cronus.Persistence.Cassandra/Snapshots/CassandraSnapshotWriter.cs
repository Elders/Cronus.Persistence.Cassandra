using System;
using System.IO;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Snapshots;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Snapshots
{
    public sealed class CassandraSnapshotWriter : ISnapshotWriter
    {
        private const string WriteSnapshotQueryTemplate = @"INSERT INTO {0} (id,rev,data) VALUES (?,?,?);";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy;
        private readonly ISerializer serializer;
        private readonly ILogger<CassandraSnapshotWriter> logger;

        private PreparedStatement writeStatement;

        public CassandraSnapshotWriter(ICassandraProvider cassandraProvider, ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy, ISerializer serializer, ILogger<CassandraSnapshotWriter> logger)
        {
            this.cassandraProvider = cassandraProvider;
            this.snapshotsTableNamingStrategy = snapshotsTableNamingStrategy;
            this.serializer = serializer;
            this.logger = logger;
        }

        public async Task WriteAsync(IBlobId id, int revision, object state)
        {
            logger.Debug(() => "Writing snapshot for aggregate {id} and revision {revision}.", Convert.ToHexString(id.RawId), revision);

            try
            {
                var session = await cassandraProvider.GetSessionAsync().ConfigureAwait(false);
                var preparedStatement = await GetWriteStatementAsync(session).ConfigureAwait(false);
                var data = SerializeState(state);
                var boundStatement = preparedStatement.Bind(id.RawId, revision, data);
                await session.ExecuteAsync(boundStatement).ConfigureAwait(false);
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Write timeout while persisting a snapshot for aggregate {id}.", Convert.ToHexString(id.RawId));
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => "Failed persisting snapshot for aggregate {id}.", Convert.ToHexString(id.RawId));
            }
        }

        private byte[] SerializeState(object state)
        {
            using var stream = new MemoryStream();
            serializer.Serialize(stream, state);
            return stream.ToArray();
        }

        private async Task<PreparedStatement> GetWriteStatementAsync(ISession session)
        {
            if (writeStatement is null)
            {
                string tableName = snapshotsTableNamingStrategy.GetName();
                writeStatement = await session.PrepareAsync(string.Format(WriteSnapshotQueryTemplate, tableName)).ConfigureAwait(false);
                writeStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return writeStatement;
        }
    }
}
