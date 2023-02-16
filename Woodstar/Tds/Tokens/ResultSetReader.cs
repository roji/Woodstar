using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Woodstar.Buffers;

namespace Woodstar.Tds.Tokens;

class ResultSetReader
{
    readonly BufferingStreamReader _streamReader;
    List<ColumnData> _columnData = null!;
    private ReadOnlySequence<byte> _buf;
    readonly List<int> _columnStartPositions = new();
    int _currentColumn;

    internal ResultSetReader(BufferingStreamReader streamReader)
        => _streamReader = streamReader;

    internal void Initialize(List<ColumnData> columnData)
    {
        _columnData = columnData;
        _currentColumn = 0;
        _buf = ReadOnlySequence<byte>.Empty;
    }

    internal async Task<bool> MoveToNextRow()
    {
        throw new NotImplementedException();
    }

    internal async ValueTask<T> Get<T>(int? column = null)
    {
        var columnIndex = column ?? _currentColumn + 1;

        ReadOnlySequence<byte> columnStartSlice;

        if (columnIndex < _columnStartPositions.Count)
        {
            Debug.Assert(!_buf.IsEmpty);
            columnStartSlice = _buf.Slice(_columnStartPositions[columnIndex]);
        }
        else
        {
            if (_columnStartPositions.Count is 0)
            {
                Debug.Assert(_buf.IsEmpty);
                Debug.Assert(_currentColumn is 0);
            }
            else
            {
                Debug.Assert(!_buf.IsEmpty);
                columnStartSlice = _buf.Slice(_columnStartPositions[^1]);
                _currentColumn = _columnStartPositions.Count - 1;
            }
            //
            // var totalLength =
            // while (_currentColumn < columnIndex)
            // {
            //     var dataType = _columnData[_currentColumn].Type; 
            //     switch (dataType.LengthKind)
            //     {
            //         case DataTypeLengthKind.Fixed:
            //             _buf dataType.Length
            //             break;
            //         case DataTypeLengthKind.VariableByte:
            //             break;
            //         case DataTypeLengthKind.VariableUShort:
            //             break;
            //         case DataTypeLengthKind.VariableInt:
            //             break;
            //         case DataTypeLengthKind.PartiallyLengthPrefixed:
            //             break;
            //         case DataTypeLengthKind.Zero:
            //         default:
            //             throw new ArgumentOutOfRangeException();
            //     }
            // }
        }

        switch (_columnData[columnIndex].Type.Code)
        {
            
        }

        throw new NotImplementedException();
    }

    void ConsumeColumn()
    {

    }

    internal void Reset()
    {
        _currentColumn = 0;
        _columnData = null;
        _columnStartPositions.Clear();
    }
}
