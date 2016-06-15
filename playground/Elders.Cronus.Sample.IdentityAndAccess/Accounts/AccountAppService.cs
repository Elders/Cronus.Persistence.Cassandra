using Elders.Cronus.DomainModeling;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;

namespace Elders.Cronus.Sample.IdentityAndAccess.Accounts
{
    public class AccountAppService : AggregateRootApplicationService<Account>,
        ICommandHandler<RegisterAccount>,
        ICommandHandler<ChangeAccountEmail>
    {

        public void Handle(RegisterAccount command)
        {
            Repository.Save(new Account(command.Id, command.Email));
        }

        public void Handle(ChangeAccountEmail command)
        {
            //  Explicit
            var account = Repository.Load<Account>(command.Id);
            account.ChangeEmail(command.OldEmail, command.NewEmail);
            Repository.Save(account);

            //  OR

            //  Implicit
            //Update(command.Id, user => user.ChangeEmail(command.OldEmail, command.NewEmail));
        }
    }
}
