using System;
using System.Text;
using System.Text.Json;
using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public sealed class PagingInfo
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
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this)));
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
