using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;

namespace Woodstar.SqlServer;

readonly struct BufferedOutput: IDisposable
{
    readonly ReadOnlySequence<byte> _sequence;

    public BufferedOutput(ReadOnlySequence<byte> sequence)
    {
        _sequence = sequence;
        Length = (int)_sequence.Length;
    }

    public int Length { get; }

    public void Write<TWriter>(StreamingWriter<TWriter> writer) where TWriter : IStreamingWriter<byte>
        => throw new NotImplementedException();

    public ValueTask WriteAsync<TWriter>(StreamingWriter<TWriter> writer, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
        => throw new NotImplementedException();

    // TODO
    public void Dispose()
    {
        var position = default(SequencePosition);
        while (_sequence.TryGet(ref position, out _))
        {
            var obj = position.GetObject();
        }
    }
}
