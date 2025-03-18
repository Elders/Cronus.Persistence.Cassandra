using System;
using System.Text;
using System.Text.Json;
using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    internal sealed class IndexPagingInfo
    {
        public int PartitionId { get; set; }

        IndexPagingInfo() { }

        public IndexPagingInfo(int partitionId)
        {
            HasMore = true;
            PartitionId = partitionId;
        }

        public byte[] Token { get; set; }

        public bool HasMore { get; set; }

        public bool HasToken() => Token is null == false;

        public override string ToString()
        {
            return Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(this));
        }

        public static IndexPagingInfo From(RowSet result, int partitionId)
        {
            return new IndexPagingInfo(partitionId)
            {
                Token = result.PagingState,
                HasMore = result.PagingState is null == false
            };
        }

        public static IndexPagingInfo Parse(string paginationToken)
        {
            IndexPagingInfo pagingInfo = new IndexPagingInfo();
            if (string.IsNullOrEmpty(paginationToken) == false)
            {
                string paginationJson = Encoding.UTF8.GetString(Convert.FromBase64String(paginationToken));
                pagingInfo = JsonSerializer.Deserialize<IndexPagingInfo>(paginationJson);
            }
            return pagingInfo;
        }
    }

    internal sealed class PagingInfo
    {
        public PagingInfo()
        {
            HasMore = true;
        }

        public byte[] Token { get; set; }

        public bool HasMore { get; set; }

        public bool HasToken() => Token is null == false;

        public override string ToString()
        {
            return Convert.ToBase64String(JsonSerializer.SerializeToUtf8Bytes(this));
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
