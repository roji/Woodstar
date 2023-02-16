using System;
using System.Buffers;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;
using Woodstar.Tds.Packets;

namespace Woodstar.Tds;

static class FrontendMessage
{
    public static readonly bool DebugEnabled = false;

    class BufferedMessage: IFrontendMessage
    {
        readonly ICopyableBuffer<byte> _buffer;

        public BufferedMessage(ICopyableBuffer<byte> buffer) => _buffer = buffer;

        public static PacketHeader MessageType => throw new NotImplementedException();
        public bool CanWriteSynchronously => true;
        public void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
            => _buffer.CopyTo(buffer.Output);
    }

    class StreamingMessage: IFrontendMessage
    {
        readonly Stream _stream;

        public StreamingMessage(Stream stream) => _stream = stream;

        public static PacketHeader MessageType => throw new NotImplementedException();
        public bool CanWriteSynchronously => false;
        public async ValueTask<FlushResult> WriteAsync<T>(MessageWriter<T> writer, CancellationToken cancellationToken = default) where T : IStreamingWriter<byte>
        {
            var read = 0;
            var flushResult = default(FlushResult);
            do
            {
                if (read > 7 * 1024)
                    writer.Writer.Ensure(8 * 1024);
                read = await _stream.ReadAsync(writer.Writer.Memory, cancellationToken).ConfigureAwait(false);
                writer.Writer.Advance(read);
                if (read > writer.AdvisoryFlushThreshold)
                    flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);
            } while (read != 0);

            if (writer.BytesPending != 0)
                flushResult = await writer.FlushAsync(cancellationToken).ConfigureAwait(false);

            return flushResult;
        }
    }

    public static IFrontendMessage Create(Stream buffer) => new StreamingMessage(buffer);
    public static IFrontendMessage Create(ICopyableBuffer<byte> buffer) => new BufferedMessage(buffer);
}

interface IFrontendHeader<THeader> where THeader: struct, IFrontendHeader<THeader>
{
    /// Number of bytes the header consists of.
    int HeaderLength { get; }
    void Write(Span<byte> buffer);
}

interface IFrontendMessage
{
    static abstract PacketHeader MessageType { get; }

    bool CanWriteSynchronously { get; }

    void Write<T>(ref BufferWriter<T> buffer) where T : IBufferWriter<byte>
        => throw new NotSupportedException();

    ValueTask WriteAsync<T>(StreamingWriter<T> writer, CancellationToken cancellationToken = default) where T : IStreamingWriter<byte>
        => throw new NotSupportedException();
}
