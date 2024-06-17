using System;
using System.Text;
using System.Text.Json;
using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal sealed class PagingInfo
    {
        public PagingInfo()
        {
            HasMore = true;
        }

        public byte[] Token { get; set; }

        public DateTime? PartitionDate { get; set; }

        public bool HasMore { get; set; }

        public bool HasToken() => Token is null == false;

        public override string ToString()
        {
            return Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(this));
        }

        public static PagingInfo From(RowSet result, DateTime partitionDate)
        {
            return new PagingInfo()
            {
                Token = result.PagingState,
                HasMore = result.PagingState is null == false,
                PartitionDate = partitionDate
            };
        }

        public static PagingInfo From(RowSet result)
        {
            return new PagingInfo()
            {
                Token = result.PagingState,
                HasMore = result.PagingState is null == false
            };
        }

        public static PagingInfo Parse(string paginationToken)
        {
            PagingInfo pagingInfo = new PagingInfo();
            if (string.IsNullOrEmpty(paginationToken) == false)
            {
                string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
                pagingInfo = JsonSerializer.Deserialize<PagingInfo>(paginationJson);
            }
            return pagingInfo;
        }
    }
}
