using System;
using System.Text;
using System.Threading;
using Woodstar.SqlServer;
using Woodstar.Tds.Messages;

namespace Woodstar.Tds.Tds33;

class Tds33CommandWriter : CommandWriter
{
    readonly Encoding _encoding;

    public Tds33CommandWriter(SqlServerDatabaseInfo sqlServerDatabaseInfo, Encoding encoding)
    {
        _encoding = encoding;
    }

    public override CommandContext WriteAsync<TCommand>(OperationSlot slot, in TCommand command, bool flushHint = true, CancellationToken cancellationToken = default)
    {
        if (slot.Protocol is not Tds33Protocol protocol)
        {
            ThrowInvalidSlot();
            return default;
        }

        var values = command.GetValues();

        // We need to create the command execution before writing to prevent any races, as the read slot could already be completed.
        var commandExecution = command.BeginExecution(values);
        var completionPair = protocol.WriteMessageAsync(slot, new SqlBatchMessage(new AllHeaders(null, new TransactionDescriptorHeader(0, 1), null), values.StatementText));
        return CommandContext.Create(completionPair, commandExecution);
    }

    static void ThrowInvalidSlot()
        => throw new ArgumentException($"Cannot use a slot for a different protocol type, expected: {nameof(Tds33Protocol)}.", "slot");

} 
