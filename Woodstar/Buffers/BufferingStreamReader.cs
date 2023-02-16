using System;
using System.Buffers.Binary;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Woodstar.Buffers;

class BufferingStreamReader
{
    Stream _stream;
    readonly byte[] _buf;
    int _pos;
    int _count;

    public BufferingStreamReader(Stream stream, int bufferSize = 8192)
    {
        _stream = stream;
        _buf = new byte[bufferSize];
    }

    public void Advance(int count)
    {
        if (count <= _count - _pos || count > _count)
            throw new ArgumentOutOfRangeException(nameof(count));

        _pos += count;
        _count -= count;
    }

    public ValueTask<BufferReader> ReadAtLeastAsync(int minimumSize, CancellationToken cancellationToken = default)
    {
        if (minimumSize <= _count - _pos)
            return new(new BufferReader(this, _buf, _pos, _count - _pos));

        return Core(minimumSize, cancellationToken);

        async ValueTask<BufferReader> Core(int minimumSize, CancellationToken cancellationToken = default)
        {
            if (minimumSize > _buf.Length)
                throw new ArgumentOutOfRangeException(nameof(minimumSize));
            Debug.Assert(minimumSize > _count);

            if (minimumSize > _buf.Length - _pos - _count)
            {
                Array.Copy(_buf, _pos, _buf, 0, _count);
                _pos = 0;
            }

            _count += await _stream.ReadAtLeastAsync(
                _buf.AsMemory(_pos + _count), minimumSize - _count, throwOnEndOfStream: true, cancellationToken);

            return new BufferReader(this, _buf, _pos, _count - _pos);
        }
    }
}

struct BufferReader
{
    readonly BufferingStreamReader _reader;
    readonly byte[] _buf;
    int _start, _pos;
    int _count;

    public BufferReader(BufferingStreamReader reader, byte[] buffer, int start, int count)
    {
        _reader = reader;
        _buf = buffer;
        _start = _pos = start;
        _count = count;
    }

    public int Remaining => _count - _pos;

    public void Advance(int length)
        => _pos += length;

    public void Commit()
    {

    }

    public bool TryRead(out byte value)
    {
        var span = _buf.AsSpan(_pos);
        if (span.Length == 0)
        {
            value = default;
            return false;
        }

        value = span[0];
        Advance(sizeof(byte));
        return true;
    }

    public bool TryReadLittleEndian(out ushort value)
    {
        var span = _buf.AsSpan(_pos);
        Advance(sizeof(ushort));
        return BinaryPrimitives.TryReadUInt16LittleEndian(span, out value);
    }

    public bool TryReadLittleEndian(out int value)
    {
        var span = _buf.AsSpan(_pos);
        Advance(sizeof(int));
        return BinaryPrimitives.TryReadInt32LittleEndian(span, out value);
    }

    public bool TryReadLittleEndian(out uint value)
    {
        var span = _buf.AsSpan(_pos);
        Advance(sizeof(uint));
        return BinaryPrimitives.TryReadUInt32LittleEndian(span, out value);
    }

    public bool TryReadLittleEndian(out long value)
    {
        var span = _buf.AsSpan(_pos);
        Advance(sizeof(long));
        return BinaryPrimitives.TryReadInt64LittleEndian(span, out value);
    }

    public bool TryReadLittleEndian(out ulong value)
    {
        var span = _buf.AsSpan(_pos);
        Advance(sizeof(ulong));
        return BinaryPrimitives.TryReadUInt64LittleEndian(span, out value);
    }
}

