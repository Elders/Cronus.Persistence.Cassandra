using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Counters
{
    public class MessageCounter : IMessageCounter
    {
        internal const string CreateTableTemplate = @"CREATE TABLE IF NOT EXISTS ""message_counter"" (cv counter, msgId varchar, PRIMARY KEY (msgid));";
        const string IncrementTemplate = @"UPDATE ""message_counter"" SET cv = cv + ? WHERE msgid=?;";
        const string DecrementTemplate = @"UPDATE ""message_counter"" SET cv = cv - ? WHERE msgid=?;";
        const string GetTemplate = @"SELECT * FROM ""message_counter"" WHERE msgid=?;";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<MessageCounter> logger;

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
            catch (Exception ex) when(True(() => logger.LogError(ex, "Failed to get {cronus_messageType} message counter.", messageType.Name)))
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
            PreparedStatement incrementStatement = await session.PrepareAsync(IncrementTemplate).ConfigureAwait(false);
            incrementStatement.SetConsistencyLevel(ConsistencyLevel.One);

            return incrementStatement;
        }

        private async Task<PreparedStatement> GetDecrementStatementAsync(ISession session)
        {
            PreparedStatement decrementStatement = await session.PrepareAsync(DecrementTemplate).ConfigureAwait(false);
            decrementStatement.SetConsistencyLevel(ConsistencyLevel.One);

            return decrementStatement;
        }

        private async Task<PreparedStatement> GetReadStatementAsync(ISession session)
        {
            PreparedStatement readStatement = await session.PrepareAsync(GetTemplate).ConfigureAwait(false);
            readStatement.SetConsistencyLevel(ConsistencyLevel.One);

            return readStatement;
        }
    }
}
