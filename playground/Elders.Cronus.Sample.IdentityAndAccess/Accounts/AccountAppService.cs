using Elders.Cronus;
using Elders.Cronus.Sample.IdentityAndAccess.Accounts.Commands;

namespace Elders.Cronus.Sample.IdentityAndAccess.Accounts
{
    public class AccountAppService : AggregateRootApplicationService<Account>,
        ICommandHandler<RegisterAccount>,
        ICommandHandler<ChangeAccountEmail>
    {
        static int counter = 0;
        static System.DateTime last = System.DateTime.UtcNow;

        public void Handle(RegisterAccount command)
        {
            Repository.Save(new Account(command.Id, command.Email));

            ++counter;
            if ((System.DateTime.UtcNow - last).TotalSeconds > 1)
            {
                last = System.DateTime.UtcNow;
                System.Console.WriteLine(counter);
                counter = 0;
            }
        }

        public void Handle(ChangeAccountEmail command)
        {
            //  Explicit
            var account = Repository.Load<Account>(command.Id);
            account.ChangeEmail(command.OldEmail, command.NewEmail);
            //Repository.Save(account);

            //  OR

            //  Implicit
            Update(command.Id, user => user.ChangeEmail(command.OldEmail, command.NewEmail));
        }
    }
}


//1+1+1
