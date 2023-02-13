using System;
using System.Text;
using System.Threading;
using Woodstar.SqlServer;

namespace Woodstar.Tds.Tds33;

class Tds33CommandWriter: CommandWriter
{
    readonly Encoding _encoding;

    public Tds33CommandWriter(SqlServerDatabaseInfo sqlServerDatabaseInfo, Encoding encoding)
    {
        _encoding = encoding;
    }

    public override CommandContext WriteAsync<TCommand>(OperationSlot slot, in TCommand command, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        throw new System.NotImplementedException();
    }

    static void ThrowInvalidSlot()
        => throw new ArgumentException($"Cannot use a slot for a different protocol type, expected: {nameof(Tds33Protocol)}.", "slot");

    static void ThrowInvalidCommand()
        => throw new ArgumentException($"Cannot use a command for a different protocol type, expected: {nameof(ISqlCommand)}.", "command");

}
