using System;
using System.Text;
using System.Text.Json;
using Cassandra;

namespace Elders.Cronus.Persistence.Cassandra
{
    public class PagingInfo
    {
        public byte[] Token { get; set; }

        public bool HasMore { get; set; } = true;

        public bool HasToken() => Token is null == false;

        public static PagingInfo From(RowSet result)
        {
            return new PagingInfo()
            {
                HasMore = result.PagingState is null == false,
                Token = result.PagingState
            };
        }

        public override string ToString()
        {
            return Convert.ToBase64String(Encoding.UTF8.GetBytes(JsonSerializer.Serialize(this)));
        }
    }
}
