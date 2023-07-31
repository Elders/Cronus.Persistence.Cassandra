using Cassandra;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;

namespace Elders.Cronus.Persistence.Cassandra.CronusMessageStore
{
    public interface ICronusMessageStore
    {
        Task AppendAsync(CronusMessage msg);
        IAsyncEnumerable<CronusMessage> LoadMessagesAsync(int batchSize);
    }

    public class CassandraMessageStore : ICronusMessageStore
    {
        private static readonly ILogger logger = CronusLogger.CreateLogger(typeof(CassandraMessageStore));

        private const string MESSAGE_STORE_TABLE_NAME = "Message_Store";
        private const string INSERT_MESSAGE_QUERY_TEMPLATE = @"INSERT INTO ""{0}"" (date,ts,data) VALUES (?,?,?);";
        private const string LOAD_MESSAGES_QUERY_TEMPLATE = @"SELECT data FROM {0};";

        private readonly ISerializer serializer;
        private readonly ISession session;

        public CassandraMessageStore(ISession session, ISerializer serializer)
        {
            this.session = session;
            this.serializer = serializer;
        }

        public async Task AppendAsync(CronusMessage msg)
        {
            var date = DateTime.UtcNow;
            var cutDownDate = Convert.ToDateTime(date.ToString("yyyyMMdd"));
            var dateTimeStamp = cutDownDate.ToFileTimeUtc();


            long resultTimestamp = DateTime.UtcNow.ToFileTimeUtc();

            string publishTime;
            if (msg.Headers.TryGetValue(MessageHeader.PublishTimestamp, out publishTime))
                if (long.TryParse(publishTime, out resultTimestamp)) { }

            byte[] data = serializer.SerializeToBytes(msg);

            PreparedStatement insertPreparedStatement = await session.PrepareAsync(string.Format(INSERT_MESSAGE_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME)).ConfigureAwait(false);

            await session
                .ExecuteAsync(insertPreparedStatement
                .Bind(dateTimeStamp, resultTimestamp, data))
                .ConfigureAwait(false);
        }

        public async IAsyncEnumerable<CronusMessage> LoadMessagesAsync(int batchSize)
        {
            PreparedStatement loadMessagesPreparedStatement = await session.PrepareAsync(string.Format(LOAD_MESSAGES_QUERY_TEMPLATE, MESSAGE_STORE_TABLE_NAME)).ConfigureAwait(false);

            var queryStatement = loadMessagesPreparedStatement.Bind().SetPageSize(batchSize);
            var result = await session.ExecuteAsync(queryStatement).ConfigureAwait(false);
            foreach (var row in result.GetRows())
            {
                var data = row.GetValue<byte[]>("data");
                CronusMessage commit = serializer.DeserializeFromBytes<CronusMessage>(data);

                yield return commit;
            }
        }
    }
}
