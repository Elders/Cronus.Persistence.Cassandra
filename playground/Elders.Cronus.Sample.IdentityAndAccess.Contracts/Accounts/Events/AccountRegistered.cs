using System;
using System.Runtime.Serialization;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Sample.IdentityAndAccess.Accounts.Events
{
    [DataContract(Name = "594c1ff2-07b1-42ac-b622-bc9d5045057a")]
    public class AccountRegistered : IEvent
    {
        AccountRegistered() { }

        public AccountRegistered(AccountId id, string email)
        {
            Id = id;
            Email = email;
            IdOOps = new AccountId(Guid.Parse("06ec38d1-db16-428c-9c6c-d291733bf689"));
        }

        [DataMember(Order = 1)]
        public AccountId Id { get; private set; }

        [DataMember(Order = 2)]
        public string Email { get; private set; }

        [DataMember(Order = 3)]
        public AccountId IdOOps { get; private set; }

        public override string ToString()
        {
            return this.ToString($"New user registered with email '{Email}'. {Id}");
        }
    }
}
