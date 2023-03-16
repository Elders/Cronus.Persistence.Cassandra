using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Cassandra;
using Elders.Cronus.Persistence.Cassandra.Preview;
using Elders.Cronus.Snapshots;
using Microsoft.Extensions.Logging;

namespace Elders.Cronus.Persistence.Cassandra.Snapshots
{
    public sealed class CassandraSnapshotReader : ISnapshotReader
    {
        private const string ReadLastSnapshotQueryTemplate = @"SELECT rev,data FROM {0} WHERE id = ? ORDER BY rev DESC LIMIT 1;";
        private const string ReadSnapshotByRevisionQueryTemplate = @"SELECT data FROM {0} WHERE id = ? AND rev = ?;";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy;
        private readonly ISerializer serializer;
        private readonly ILogger<CassandraSnapshotReader> logger;

        private PreparedStatement readLastStatement;
        private PreparedStatement readByRevisionStatement;

        public CassandraSnapshotReader(ICassandraProvider cassandraProvider, ISnapshotsTableNamingStrategy snapshotsTableNamingStrategy, ISerializer serializer, ILogger<CassandraSnapshotReader> logger)
        {
            this.cassandraProvider = cassandraProvider;
            this.snapshotsTableNamingStrategy = snapshotsTableNamingStrategy;
            this.serializer = serializer;
            this.logger = logger;
        }

        public async Task<Snapshot> ReadAsync(IBlobId id)
        {
            logger.Debug(() => "Reading last snapshot for aggregate {id}.", Convert.ToHexString(id.RawId));

            try
            {
                var session = await cassandraProvider.GetSessionAsync().ConfigureAwait(false);
                var preparedStatement = await GetReadLastStatementAsync(session).ConfigureAwait(false);
                var boundStatement = preparedStatement.Bind(id.RawId);
                var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);

                var row = result.GetRows().FirstOrDefault();
                if (row is not null)
                {
                    int revision = row.GetValue<int>(CassandraColumn.Revision);
                    byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);
                    object state = DeserializeState(data);

                    return new Snapshot(id, revision, state);
                }

                return null;
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Read timeout while reading a snapshot for aggregate {id}.", Convert.ToHexString(id.RawId));
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => "Failed read snapshot for aggregate {id}.", Convert.ToHexString(id.RawId));
            }

            return null;
        }

        public async Task<Snapshot> ReadAsync(IBlobId id, int revision)
        {
            logger.Debug(() => "Reading snapshot for aggregate {id} and revision {revision}.", id, revision);

            try
            {
                var session = await cassandraProvider.GetSessionAsync().ConfigureAwait(false);
                var preparedStatement = await GetReadByRevisionStatementAsync(session).ConfigureAwait(false);
                var boundStatement = preparedStatement.Bind(id.RawId, revision);
                var result = await session.ExecuteAsync(boundStatement).ConfigureAwait(false);

                var row = result.GetRows().FirstOrDefault();
                if (row is not null)
                {
                    byte[] data = row.GetValue<byte[]>(CassandraColumn.Data);
                    object state = DeserializeState(data);

                    return new Snapshot(id, revision, (IAggregateRootState)state);
                }

                return null;
            }
            catch (WriteTimeoutException ex)
            {
                logger.WarnException(ex, () => "Read timeout while reading a snapshot for aggregate {id} and revision {revision}.", id, revision);
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => "Failed read snapshot for aggregate {id} and revision {revision}.", id, revision);
            }

            return null;
        }

        private object DeserializeState(byte[] data)
        {
            using var stream = new MemoryStream(data);
            var state = serializer.Deserialize(stream);
            return state;
        }

        private async Task<PreparedStatement> GetReadLastStatementAsync(ISession session)
        {
            if (readLastStatement is null)
            {
                string tableName = snapshotsTableNamingStrategy.GetName();
                readLastStatement = await session.PrepareAsync(string.Format(ReadLastSnapshotQueryTemplate, tableName)).ConfigureAwait(false);
                readLastStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readLastStatement;
        }

        private async Task<PreparedStatement> GetReadByRevisionStatementAsync(ISession session)
        {
            if (readByRevisionStatement is null)
            {
                string tableName = snapshotsTableNamingStrategy.GetName();
                readByRevisionStatement = await session.PrepareAsync(string.Format(ReadSnapshotByRevisionQueryTemplate, tableName)).ConfigureAwait(false);
                readByRevisionStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readByRevisionStatement;
        }
    }
}
