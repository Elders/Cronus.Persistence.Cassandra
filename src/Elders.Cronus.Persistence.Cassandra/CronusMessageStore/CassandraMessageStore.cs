using Cassandra;
using Elders.Cronus.Persistence.Cassandra.Logging;
using Elders.Cronus.Serializer;
using System;
using System.Collections.Generic;
using System.IO;

namespace Elders.Cronus.Persistence.Cassandra.CronusMessageStore
{
    public interface ICronusMessageStore
    {
        void Append(CronusMessage msg);
        IEnumerable<CronusMessage> LoadMessages(int batchSize);
    }

    public class CassandraMessageStore : ICronusMessageStore
    {
        private const string MESSAGE_STORE_TABLE_NAME = "Message_Store";
        private const string INSERT_MESSAGE_QUERY_TEMPLATE = @"INSERT INTO ""{0}"" (date,ts,data) VALUES (?,?,?);";
        private const string LOAD_MESSAGES_QUERY_TEMPLATE = @"SELECT data FROM {0};";

        private readonly ILog log;

        private readonly ISerializer serializer;
        private readonly ISession session;

        public CassandraMessageStore(ISession session, ISerializer serializer)
        {
            this.session = session;
            this.serializer = serializer;
            this.log = LogProvider.GetLogger(typeof(CassandraEventStorePlayer));
        }

        public void Append(CronusMessage msg)
        {
            var date = DateTime.UtcNow;
            var cutDownDate = Convert.ToDateTime(date.ToString("yyyyMMdd"));
            var dateTimeStamp = cutDownDate.ToFileTimeUtc();


            long resultTimestamp = DateTime.UtcNow.ToFileTimeUtc();

            string publishTime;
            if (msg.Headers.TryGetValue(MessageHeader.PublishTimestamp, out publishTime))
                if (long.TryParse(publishTime, out resultTimestamp)) { }

            byte[] data = SerializeEvent(msg);

            PreparedStatement insertPreparedStatement = session.Prepare(String.Format(INSERT_MESSAGE_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME));

            session
                .Execute(insertPreparedStatement
                .Bind(dateTimeStamp, resultTimestamp, data));
        }

        public IEnumerable<CronusMessage> LoadMessages(int batchSize)
        {
            PreparedStatement loadMessagesPreparedStatement = session.Prepare(String.Format(LOAD_MESSAGES_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME));

            var queryStatement = loadMessagesPreparedStatement.Bind().SetPageSize(batchSize);
            var result = session.Execute(queryStatement);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                using (var stream = new MemoryStream(data))
                {
                    CronusMessage commit;
                    try
                    {
                        commit = (CronusMessage)serializer.Deserialize(stream);
                    }
                    catch (Exception ex)
                    {
                        string error = "Failed to deserialize an AggregateCommit. EventBase64bytes: " + Convert.ToBase64String(data);
                        log.ErrorException(error, ex);
                        continue;
                    }
                    yield return commit;
                }
            }
        }

        private byte[] SerializeEvent(CronusMessage msg)
        {
            using (var stream = new MemoryStream())
            {
                serializer.Serialize(stream, msg);
                return stream.ToArray();
            }
        }
    }
}
