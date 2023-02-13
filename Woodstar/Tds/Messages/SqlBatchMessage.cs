using System.Text;
using Woodstar.Buffers;
using Woodstar.Tds.Packets;

namespace Woodstar.Tds.Messages;

readonly struct SqlBatchMessage
{
    readonly AllHeaders _allHeaders;
    readonly string _commandText;

    public SqlBatchMessage(AllHeaders allHeaders, string commandText)
    {
        _allHeaders = allHeaders;
        _commandText = commandText;
    }

    public PacketHeader Header => PacketHeader.CreateType(PacketType.SQLBatch, MessageStatus.ResetConnection);

    public void Write<TWriter>(StreamingWriter<TWriter> writer) where TWriter : IStreamingWriter<byte>
    {
        _allHeaders.Write(writer);
        writer.WriteString(_commandText, Encoding.Unicode);
    }
}
