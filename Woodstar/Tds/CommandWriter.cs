using System;
using System.Buffers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;
using Woodstar.Tds.Messages;
using Woodstar.Tds.Tds33;

namespace Woodstar.Tds;

abstract class CommandWriter
{
    public abstract CommandContext WriteAsync<TCommand>(OperationSlot slot, in TCommand command, bool flushHint = true, CancellationToken cancellationToken = default) where TCommand : ISqlCommand;
}

class TdsCommandWriter : CommandWriter
{
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
