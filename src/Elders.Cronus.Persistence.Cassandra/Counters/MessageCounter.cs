using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Counters
{
    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public class MessageCounter : IMessageCounter
    {
        internal const string CreateTableTemplate = @"CREATE TABLE IF NOT EXISTS ""message_counter"" (cv counter, msgId varchar, PRIMARY KEY (msgid));";

        const string IncrementTemplate = @"UPDATE ""{0}"".""message_counter"" SET cv = cv + ? WHERE msgid=?;";
        const string DecrementTemplate = @"UPDATE ""{0}"".""message_counter"" SET cv = cv - ? WHERE msgid=?;";
        const string GetTemplate = @"SELECT * FROM ""{0}"".""message_counter"" WHERE msgid=?;";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<MessageCounter> logger;

        private PreparedStatement _incrementTemplatePreparedStatement;
        private PreparedStatement _decrementTemplatePreparedStatement;
        private PreparedStatement _getTempletePreparedStatement;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        public MessageCounter(ICassandraProvider cassandraProvider, ILogger<MessageCounter> logger)
        {
            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public async Task IncrementAsync(Type messageType, long incrementWith = 1)
        {
            try
            {
                string eventType = messageType.GetContractId();
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement incrementedStatement = await GetIncrementStatementAsync(session).ConfigureAwait(false);
                await session.ExecuteAsync(incrementedStatement.Bind(incrementWith, eventType)).ConfigureAwait(false);
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to increment {cronus_messageType} message counter.", messageType.Name))) { }
        }

        public async Task DecrementAsync(Type messageType, long decrementWith = 1)
        {
            try
            {
                string eventType = messageType.GetContractId();
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement decrementedStatement = await GetDecrementStatementAsync(session).ConfigureAwait(false);
                await session.ExecuteAsync(decrementedStatement.Bind(decrementWith, eventType)).ConfigureAwait(false);
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to decrement {cronus_messageType} message counter.", messageType.Name))) { }
        }

        public async Task<long> GetCountAsync(Type messageType)
        {
            try
            {
                string eventType = messageType.GetContractId();
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement ps = await GetReadStatementAsync(session).ConfigureAwait(false);
                BoundStatement bs = ps.Bind(eventType);
                RowSet result = await session.ExecuteAsync(bs).ConfigureAwait(false);
                Row row = result.GetRows().SingleOrDefault();
                if (row is null)
                {
                    return 0;
                }
                else
                {
                    long counterValue = row.GetValue<long>("cv");
                    return counterValue;
                }
            }
            catch (Exception ex) when (True(() => logger.LogError(ex, "Failed to get {cronus_messageType} message counter.", messageType.Name)))
            {
                return 0;
            }
        }

        public async Task ResetAsync(Type messageType)
        {
            long current = await GetCountAsync(messageType).ConfigureAwait(false);
            await DecrementAsync(messageType, current).ConfigureAwait(false);
        }

        private async Task<PreparedStatement> GetIncrementStatementAsync(ISession session)
        {
            if (_incrementTemplatePreparedStatement is null)
            {
                _incrementTemplatePreparedStatement = await session.PrepareAsync(string.Format(IncrementTemplate, session.Keyspace)).ConfigureAwait(false);
                _incrementTemplatePreparedStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }
            return _incrementTemplatePreparedStatement;
        }

        private async Task<PreparedStatement> GetDecrementStatementAsync(ISession session)
        {
            if (_decrementTemplatePreparedStatement is null)
            {
                _decrementTemplatePreparedStatement = await session.PrepareAsync(string.Format(DecrementTemplate, session.Keyspace)).ConfigureAwait(false);
                _decrementTemplatePreparedStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }
            return _decrementTemplatePreparedStatement;
        }

        private async Task<PreparedStatement> GetReadStatementAsync(ISession session)
        {
            if (_getTempletePreparedStatement is null)
            {
                _getTempletePreparedStatement = await session.PrepareAsync(string.Format(GetTemplate, session.Keyspace)).ConfigureAwait(false);
                _getTempletePreparedStatement.SetConsistencyLevel(ConsistencyLevel.One);
            }
            return _getTempletePreparedStatement;
        }
    }
}
