using Cassandra;
using Elders.Cronus.EventStore;
using Microsoft.Extensions.Logging;
using System;
using System.Linq;
using System.Text;

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

        private ISession GetSession() => cassandraProvider.GetSession();

        public MessageCounter(ICassandraProvider cassandraProvider, ILogger<MessageCounter> logger)
        {
            this.cassandraProvider = cassandraProvider;
            this.logger = logger;
        }

        public void Increment(Type messageType, long incrementWith = 1)
        {
            try
            {
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                GetSession().Execute(GetIncrementStatement().Bind(incrementWith, eventType));
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to increment {messageType.Name} message counter.");
            }
        }

        public void Decrement(Type messageType, long decrementWith = 1)
        {
            try
            {
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                GetSession().Execute(GetDecrementStatement().Bind(decrementWith, eventType));
            }
            catch (Exception ex)
            {
                logger.ErrorException(ex, () => $"Failed to decrement {messageType.Name} message counter.");
            }
        }

        public long GetCount(Type messageType)
        {
            try
            {
                string eventType = Convert.ToBase64String(Encoding.UTF8.GetBytes(messageType.GetContractId()));
                BoundStatement bs = GetReadStatement().Bind(eventType);
                var result = GetSession().Execute(bs);
                var row = result.GetRows().SingleOrDefault();
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

        public void Reset(Type messageType)
        {
            long current = GetCount(messageType);
            Decrement(messageType, current);
        }

        PreparedStatement incrementStatement;
        private PreparedStatement GetIncrementStatement()
        {
            if (incrementStatement is null)
            {
                incrementStatement = GetSession().Prepare(IncrementTemplate);
                incrementStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return incrementStatement;
        }

        PreparedStatement decrementStatement;
        private PreparedStatement GetDecrementStatement()
        {
            if (decrementStatement is null)
            {
                decrementStatement = GetSession().Prepare(DecrementTemplate);
                decrementStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return decrementStatement;
        }

        PreparedStatement readStatement;
        private PreparedStatement GetReadStatement()
        {
            if (readStatement is null)
            {
                readStatement = GetSession().Prepare(GetTemplate);
                readStatement.SetConsistencyLevel(ConsistencyLevel.LocalQuorum);
            }

            return readStatement;
        }


    }
}
