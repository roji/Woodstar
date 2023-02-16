using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Tds.Packets;

namespace Woodstar.Tds;

/// <summary>
/// A read-only stream that gets rid of the TDS packet layer.
/// </summary>
public class TdsPacketStream : Stream
{
    readonly Stream _stream;
    byte[] _buf;
    int _pos, _count, _packetRemaining;

    public TdsPacketStream(Stream stream)
    {
        _stream = stream;
    }

    public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        var zeroByteRead = buffer.Length == 0;
        var totalCopied = 0;

        while (true)
        {
            var copied = Math.Min(buffer.Length, Math.Min(_packetRemaining, _count - _pos));
            _buf.AsMemory(_pos, copied).CopyTo(buffer);
            _pos += copied;
            _count -= copied;
            _packetRemaining -= copied;
            totalCopied += copied;

            if (copied == buffer.Length && !zeroByteRead)
                return totalCopied;

            buffer = buffer.Slice(0, copied);

            if (_count == 0)
            {
                _pos = 0;
                _count = await _stream.ReadAsync(_buf, 0, _buf.Length, cancellationToken);

                if (!zeroByteRead)
                    continue;
                return 0;
            }

            Debug.Assert(_packetRemaining == 0);

            // We're now at the start of a new packet. Make sure we have a full header buffered.
            if (_count < PacketHeader.ByteCount)
            {
                Array.Copy(_buf, _pos, _buf, 0, _count);
                _pos = 0;
                _count += await _stream.ReadAtLeastAsync(
                    _buf.AsMemory(_count),
                    PacketHeader.ByteCount - _count,
                    throwOnEndOfStream: true,
                    cancellationToken);
            }

            Debug.Assert(_count >= PacketHeader.ByteCount);

            if (!PacketHeader.TryParse(_buf.AsSpan(_pos), out var header))
                throw new InvalidOperationException("Couldn't parse TDS packet header");

            _packetRemaining = header.PacketSize - PacketHeader.ByteCount;
            _pos += PacketHeader.ByteCount;
            _count -= PacketHeader.ByteCount;

            if (zeroByteRead && _count > 0)
                return 0;
        }
    }

    public override int Read(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin)
        => throw new NotSupportedException();

    public override void SetLength(long value)
        => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
        => throw new NotSupportedException();

    public override void Flush()
        => throw new NotSupportedException();

    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }
}
