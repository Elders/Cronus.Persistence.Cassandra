using System;
using System.Runtime.Serialization;
using Elders.Cronus.DomainModeling;

namespace Elders.Cronus.Sample.IdentityAndAccess.Accounts
{
    [DataContract(Name = "68cb3c79-0d0e-40d4-8dd5-0a49a361ecdd")]
    public class AccountId : GuidId
    {
        AccountId() { }
        public AccountId(Guid id) : base(id, "account") { }
    }
}
