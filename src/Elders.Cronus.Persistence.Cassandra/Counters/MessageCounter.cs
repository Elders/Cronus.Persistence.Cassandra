using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Text;
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
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement incrementedStatement = await GetIncrementStatementAsync().ConfigureAwait(false);
                await session.ExecuteAsync(incrementedStatement.Bind(incrementWith, eventType)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to increment {messageType.Name} message counter.");
            }
        }

        public async Task DecrementAsync(Type messageType, long decrementWith = 1)
        {
            try
            {
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement decrementedStatement = await GetIncrementStatementAsync().ConfigureAwait(false);
                await session.ExecuteAsync(decrementedStatement.Bind(decrementWith, eventType)).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to decrement {messageType.Name} message counter.");
            }
        }

        public async Task<long> GetCountAsync(Type messageType)
        {
            try
            {
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                PreparedStatement ps = await GetReadStatementAsync().ConfigureAwait(false);
                BoundStatement bs = ps.Bind(eventType);
                ISession session = await GetSessionAsync().ConfigureAwait(false);
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
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to get {messageType.Name} message counter.");
                return 0;
            }
        }

        public async Task ResetAsync(Type messageType)
        {
            long current = await GetCountAsync(messageType).ConfigureAwait(false);
            await DecrementAsync(messageType, current).ConfigureAwait(false);
        }

        PreparedStatement incrementStatement;
        private async Task<PreparedStatement> GetIncrementStatementAsync()
        {
            if (incrementStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                incrementStatement = await session.PrepareAsync(IncrementTemplate).ConfigureAwait(false);
                incrementStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return incrementStatement;
        }

        PreparedStatement decrementStatement;
        private async Task<PreparedStatement> GetDecrementStatementAsync()
        {
            if (decrementStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                decrementStatement = await session.PrepareAsync(DecrementTemplate).ConfigureAwait(false);
                decrementStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return decrementStatement;
        }

        PreparedStatement readStatement;
        private async Task<PreparedStatement> GetReadStatementAsync()
        {
            if (readStatement is null)
            {
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                readStatement = await session.PrepareAsync(GetTemplate).ConfigureAwait(false);
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }


    }
}
