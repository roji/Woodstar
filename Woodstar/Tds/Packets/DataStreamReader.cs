using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;
using Woodstar.Buffers;

namespace Woodstar.Tds.Packets;

ref struct DataStreamReader
{
    SequenceReader<byte> _reader;
    int _packetRemaining;
    public DataStreamReader(ReadOnlySequence<byte> sequence) => _reader = new SequenceReader<byte>(sequence);

    public long Consumed { get; set; }

    public void Advance(int count)
    {
        var totalAdvanced = 0;

        var packetRemaining = _packetRemaining;
        var unreadSequence = _reader.UnreadSequence;

        while (true)
        {
            var advanced = Math.Min(count, packetRemaining);
            if (advanced > unreadSequence.Length)
                throw new ArgumentOutOfRangeException();

            unreadSequence = unreadSequence.Slice(advanced);
            packetRemaining -= advanced;
            totalAdvanced += advanced;
            if (count == 0)
                break;

            // Consumed the current packet, parse the next packet's header etc.
            Debug.Assert(packetRemaining == 0);
            if (!PacketHeader.TryParse(unreadSequence, out var header))
                throw new ArgumentOutOfRangeException();
            unreadSequence = unreadSequence.Slice(PacketHeader.ByteCount);
            packetRemaining = header.PacketSize - PacketHeader.ByteCount;
        }

        _reader.Advance(totalAdvanced);
        _packetRemaining = packetRemaining;
    }

    public bool HasAtLeast(int length)
        => length <= _packetRemaining && length <= _reader.Remaining || HasAtLeastSlow(length);

    bool HasAtLeastSlow(int length)
    {
        var packetRemaining = _packetRemaining;
        var totalRemainingLength = _packetRemaining;
        var unreadSequence = _reader.UnreadSequence;

        while (length > totalRemainingLength)
        {
            if (packetRemaining > unreadSequence.Length)
            {
                return false;
            }

            var headerStart = unreadSequence.Slice(packetRemaining);
            if (!PacketHeader.TryParse(headerStart, out var header))
                return false;
            packetRemaining = header.PacketSize - PacketHeader.ByteCount;
            totalRemainingLength += packetRemaining;
            unreadSequence = headerStart.Slice(PacketHeader.ByteCount);
        }

        return true;
    }

    bool EnsurePacketData(scoped Span<byte> scratchBuffer, out bool useScratchBuffer)
    {
        if (_packetRemaining >= scratchBuffer.Length)
        {
            useScratchBuffer = false;
            return true;
        }

        return EnsureSlow(scratchBuffer, out useScratchBuffer);
    }

    bool EnsureSlow(scoped Span<byte> scratchBuffer, out bool useScratchBuffer)
    {
        if (_reader.Remaining < PacketHeader.ByteCount + scratchBuffer.Length)
        {
            useScratchBuffer = default;
            return false;
        }

        var unreadSequence = _reader.UnreadSequence;
        var headerStart = unreadSequence.Slice(_packetRemaining);
        PacketHeader.TryParse(headerStart, out var header);

        if (_packetRemaining > 0)
        {
            unreadSequence.Slice(0, _packetRemaining).TryCopyTo(scratchBuffer);
            headerStart.Slice(PacketHeader.ByteCount, scratchBuffer.Length - _packetRemaining)
                .TryCopyTo(scratchBuffer.Slice(_packetRemaining));

            useScratchBuffer = true;
        }
        else
        {
            useScratchBuffer = false;
        }

        _reader.Advance(PacketHeader.ByteCount + _packetRemaining);
        _packetRemaining = header.PacketSize;

        return true;
    }

    public bool TryRead(out byte value)
    {
        Span<byte> scratchBuffer = stackalloc byte[1];

        if (!EnsurePacketData(scratchBuffer, out var useScratchBuffer))
        {
            value = default;
            return false;
        }

        Debug.Assert(!useScratchBuffer);

        return _reader.TryRead(out value);
    }

    public bool TryReadLittleEndian(out ushort value)
    {
        Span<byte> scratchBuffer = stackalloc byte[sizeof(ushort)];

        if (!EnsurePacketData(scratchBuffer, out var useScratchBuffer))
        {
            value = default;
            return false;
        }

        return useScratchBuffer
            ? BinaryPrimitives.TryReadUInt16LittleEndian(scratchBuffer, out value)
            : _reader.TryReadLittleEndian(out value);
    }

    public bool TryReadLittleEndian(out int value)
    {
        Span<byte> scratchBuffer = stackalloc byte[sizeof(int)];

        if (!EnsurePacketData(scratchBuffer, out var useScratchBuffer))
        {
            value = default;
            return false;
        }

        return useScratchBuffer
            ? BinaryPrimitives.TryReadInt32LittleEndian(scratchBuffer, out value)
            : _reader.TryReadLittleEndian(out value);
    }

    public bool TryReadLittleEndian(out uint value)
    {
        Unsafe.SkipInit(out value);
        return TryReadLittleEndian(out Unsafe.As<uint, int>(ref value));
    }

    public bool TryReadLittleEndian(out long value)
    {
        Span<byte> scratchBuffer = stackalloc byte[sizeof(long)];

        if (!EnsurePacketData(scratchBuffer, out var useScratchBuffer))
        {
            value = default;
            return false;
        }

        return useScratchBuffer
            ? BinaryPrimitives.TryReadInt64LittleEndian(scratchBuffer, out value)
            : _reader.TryReadLittleEndian(out value);
    }

    public bool TryReadLittleEndian(out ulong value)
    {
        Unsafe.SkipInit(out value);
        return TryReadLittleEndian(out Unsafe.As<ulong, long>(ref value));
    }

    public bool TryReadBVarchar([NotNullWhen(true)] out string value)
    {
        value = "";
        if (TryRead(out var len))
        {
            if (HasAtLeast(len) && len <= _packetRemaining)
            {
                if (len > 0)
                {
                    var sequence = _reader.UnreadSequence.Slice(0, 2 * len);
                    value = Encoding.Unicode.GetString(sequence);
                    _reader.Advance(2 * len);
                }
                return true;
            }

            throw new NotImplementedException();
        }

        return false;
    }

    public bool TryReadUsVarchar([NotNullWhen(true)] out string value)
    {
        value = "";
        if (TryReadLittleEndian(out ushort len))
        {
            if (HasAtLeast(len) && len <= _packetRemaining)
            {
                if (len > 0)
                {
                    var sequence = _reader.UnreadSequence.Slice(0, 2 * len);
                    value = Encoding.Unicode.GetString(sequence);
                    _reader.Advance(2 * len);
                }
                return true;
            }

            throw new NotImplementedException();
        }

        return false;
    }

    public bool TryCopyTo(scoped Span<byte> destination)
    {
        if (_packetRemaining >= destination.Length)
        {
            return _reader.TryCopyTo(destination);
        }

        throw new NotImplementedException();
    }
}
