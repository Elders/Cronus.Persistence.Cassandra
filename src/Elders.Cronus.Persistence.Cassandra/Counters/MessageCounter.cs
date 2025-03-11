using Cassandra;
using Elders.Cronus.EventStore;
using Elders.Cronus.MessageProcessing;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.Counters
{
    internal class CounterIncrementQuery : PreparedStatementCache
    {
        private const string Template = @"UPDATE {0}.message_counter SET cv = cv + ? WHERE msgid=?;";

        public CounterIncrementQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class CounterDecrementQuery : PreparedStatementCache
    {
        private const string Template = @"UPDATE {0}.message_counter SET cv = cv - ? WHERE msgid=?;";

        public CounterDecrementQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }

    internal class CounterLoadQuery : PreparedStatementCache
    {
        private const string Template = @"SELECT * FROM {0}.message_counter WHERE msgid=?;";

        public CounterLoadQuery(ICronusContextAccessor context, ICassandraProvider cassandraProvider) : base(context, cassandraProvider) { }

        internal override string GetQueryTemplate() => Template;
    }

    /// We tried to use <see cref="ISession.PrepareAsync(string, string)"/> where we wanted to specify the keyspace (we use [cqlsh 6.2.0 | Cassandra 5.0.2 | CQL spec 3.4.7 | Native protocol v5] cassandra)
    /// it seems like the driver does not have YET support for protocol v5 (still in beta). In code the driver is using protocol v4 (which is preventing us from using the above mentioned method)
    /// https://datastax-oss.atlassian.net/jira/software/c/projects/CSHARP/issues/CSHARP-856 as of 01.23.25 this epic is still in todo.
    public class MessageCounter : IMessageCounter
    {
        internal const string CreateTableTemplate = @"CREATE TABLE IF NOT EXISTS {0}.message_counter (cv counter, msgId varchar, PRIMARY KEY (msgid));";

        private readonly ICassandraProvider cassandraProvider;
        private readonly ILogger<MessageCounter> logger;

        private CounterIncrementQuery _incrementTemplatePreparedStatement;
        private CounterDecrementQuery _decrementTemplatePreparedStatement;
        private CounterLoadQuery _getTempletePreparedStatement;

        private Task<ISession> GetSessionAsync() => cassandraProvider.GetSessionAsync();

        public MessageCounter(ICronusContextAccessor cronusContextAccessor, ICassandraProvider cassandraProvider, ILogger<MessageCounter> logger)
        {
            this.cassandraProvider = cassandraProvider;
            this.logger = logger;

            _incrementTemplatePreparedStatement = new CounterIncrementQuery(cronusContextAccessor, cassandraProvider);
            _decrementTemplatePreparedStatement = new CounterDecrementQuery(cronusContextAccessor, cassandraProvider);
            _getTempletePreparedStatement = new CounterLoadQuery(cronusContextAccessor, cassandraProvider);
        }

        public async Task IncrementAsync(Type messageType, long incrementWith = 1)
        {
            try
            {
                string eventType = messageType.GetContractId();
                ISession session = await GetSessionAsync().ConfigureAwait(false);
                PreparedStatement incrementedStatement = await _incrementTemplatePreparedStatement.PrepareAsync(session).ConfigureAwait(false);
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
                PreparedStatement decrementedStatement = await _decrementTemplatePreparedStatement.PrepareAsync(session).ConfigureAwait(false);
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
                PreparedStatement ps = await _getTempletePreparedStatement.PrepareAsync(session).ConfigureAwait(false);
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
    }
}
