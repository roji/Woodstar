using System;
using System.Buffers;
using System.Diagnostics;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace Woodstar.Tds.Packets;

class DataStreamSegment : ReadOnlySequenceSegment<byte>
{
    public PacketHeader PacketHeader { get; set; }
    public void SetMemory(ReadOnlyMemory<byte> memory) => Memory = memory;
    public int Length => Memory.Length;

    public new DataStreamSegment? Next
    {
        get
        {
            if (base.Next is { } next)
                return Unsafe.As<ReadOnlySequenceSegment<byte>, DataStreamSegment>(ref next);

            return null;
        }
        set => base.Next = value;
    }

    public void SetNext(DataStreamSegment segment)
    {
        Debug.Assert(segment != null);
        Debug.Assert(Next == null);

        Next = segment;

        segment = this;

        while (segment.Next != null)
        {
            Debug.Assert(segment.Next != null);
            segment.Next.RunningIndex = segment.RunningIndex + segment.Length;
            segment = segment.Next;
        }
    }
}

public class DataStreamReader : PipeReader
{
    int _packetConsumed;
    readonly PipeReader _pipeReader;

    public DataStreamReader(PipeReader pipeReader)
    {
        // TODO validate minimumSegmentSize is > PacketHeader.ByteCount.
        _pipeReader = pipeReader;
    }

    public override bool TryRead(out ReadResult result)
    {
        if (_pipeReader.TryRead(out result))
        {
            result = TransformReadResult(result);
            return true;
        }

        return false;
    }

    public override async ValueTask<ReadResult> ReadAsync(CancellationToken cancellationToken = default)
        => TransformReadResult(await ReadAsync(cancellationToken));

    ReadResult TransformReadResult(ReadResult readResult)
    {
        var roSeq = readResult.Buffer;
        DataStreamSegment? segmentHead = null;
        DataStreamSegment? segmentTail = null;
        var position = default(SequencePosition);
        var consumedSegmentBytes = _packetConsumed;
        var packetStart = _packetConsumed;
        while (roSeq.TryGet(ref position, out var segment))
        {
            var segSpan = segment.Span.Slice(packetStart);
            while (consumedSegmentBytes <= segment.Length)
            {
                if (PacketHeader.TryParse(segSpan, out var header))
                {
                    packetStart += header.PacketSize;
                    consumedSegmentBytes += header.PacketSize;
                    var adjustedSegment = GetSegment(segment.Slice(8), header);
                    segmentHead ??= adjustedSegment;
                    if (segmentTail is not null)
                        segmentTail.SetNext(adjustedSegment);
                    segmentTail = adjustedSegment;
                }
            }

            packetStart -= segment.Length;
            consumedSegmentBytes = 0;
        }

        Debug.Assert(segmentHead is not null);
        return new ReadResult(
            new(segmentHead, 0, segmentTail ?? segmentHead, (segmentTail ?? segmentHead).Length),
            readResult.IsCompleted,
            readResult.IsCanceled
        );

        DataStreamSegment GetSegment(ReadOnlyMemory<byte> memory, PacketHeader header)
        {
            var segment = new DataStreamSegment();
            segment.SetMemory(memory);
            segment.PacketHeader = header;
            return segment;
        }
    }

    public override void AdvanceTo(SequencePosition consumed, SequencePosition examined)
    {
        throw new NotImplementedException();
    }
    public override void AdvanceTo(SequencePosition consumed) => AdvanceTo(consumed, consumed);
    public override void CancelPendingRead() => _pipeReader.CancelPendingRead();
    public override void Complete(Exception? exception = null) => Complete(exception);
}
