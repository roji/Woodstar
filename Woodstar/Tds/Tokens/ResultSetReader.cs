using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;

namespace Woodstar.Tds.Tokens;

class ResultSetReader
{
    readonly TokenReader _tokenReader;
    readonly BufferingStreamReader _streamReader;
    List<ColumnData> _columnData = null!;
    BufferReader _reader;
    readonly List<int> _columnStartPositions = new();
    int _currentColumn;

    internal ResultSetReader(TokenReader tokenReader, BufferingStreamReader streamReader)
    {
        _tokenReader = tokenReader;
        _streamReader = streamReader;
    }

    internal void Initialize(List<ColumnData> columnData)
    {
        _columnData = columnData;
        _currentColumn = -1;
        _reader = BufferReader.Empty;
    }

    internal async Task<bool> MoveToNextRow(CancellationToken cancellationToken = default)
    {
        _reader.Commit();

        var token = await _tokenReader.ReadAsync(cancellationToken);

        return token switch
        {
            RowToken => true,
            DoneToken => false,
            _ => throw new ArgumentOutOfRangeException()
        };
    }

    internal async ValueTask<T> GetAsync<T>(int? column = null, CancellationToken cancellationToken = default)
    {
        var columnIndex = column ?? _currentColumn + 1;

        ReadOnlySequence<byte> columnStartSlice;

        // if (columnIndex < _columnStartPositions.Count)
        // {
        //     Debug.Assert(!_reader.IsEmpty);
        //     columnStartSlice = _reader.Slice(_columnStartPositions[columnIndex]);
        // }
        // else
        // {
        //     if (_columnStartPositions.Count is 0)
        //     {
        //         Debug.Assert(_reader.IsEmpty);
        //         Debug.Assert(_currentColumn is 0);
        //     }
        //     else
        //     {
        //         Debug.Assert(!_reader.IsEmpty);
        //         columnStartSlice = _reader.Slice(_columnStartPositions[^1]);
        //         _currentColumn = _columnStartPositions.Count - 1;
        //     }

            // _columnData[0].Type.LengthKind ?
            // var totalMinimumSeekLength = 0;

        while (_currentColumn < columnIndex - 1)
        {
            var dataType = _columnData[_currentColumn + 1].Type;
            switch (dataType.LengthKind)
            {
                case DataTypeLengthKind.Fixed:
                {
                    if (_reader.Remaining < dataType.Length)
                        _reader = await _streamReader.ReadAtLeastAsync(dataType.Length, cancellationToken);

                    if (_currentColumn < columnIndex - 1)
                        _reader.Advance(dataType.Length);
                    break;
                }
                case DataTypeLengthKind.VariableByte:
                {
                    if (!_reader.TryRead(out var length) || _reader.Remaining < length)
                        _reader = await _streamReader.ReadAtLeastAsync(length + sizeof(byte), cancellationToken);

                    if (_currentColumn < columnIndex - 1)
                        _reader.Advance(length + sizeof(byte));
                    break;
                }
                case DataTypeLengthKind.VariableUShort:
                {
                    if (!_reader.TryReadLittleEndian(out ushort length) || _reader.Remaining < length)
                        _reader = await _streamReader.ReadAtLeastAsync(length + sizeof(ushort), cancellationToken);

                    if (_currentColumn < columnIndex - 1)
                        _reader.Advance(length + sizeof(ushort));
                    break;
                }
                case DataTypeLengthKind.VariableInt:
                {
                    if (!_reader.TryReadLittleEndian(out int length) || _reader.Remaining < length)
                        _reader = await _streamReader.ReadAtLeastAsync(length + sizeof(int), cancellationToken);

                    if (_currentColumn < columnIndex - 1)
                        _reader.Advance(length + sizeof(int));
                    break;
                }
                case DataTypeLengthKind.PartiallyLengthPrefixed:
                    throw new NotImplementedException();
                case DataTypeLengthKind.Zero:
                default:
                    throw new ArgumentOutOfRangeException();
            }

            _currentColumn++;
        }

        T? result = default;
        var typeCode = _columnData[columnIndex].Type.Code;
        switch (typeCode)
        {
            // Fixed-Length
            case DataTypeCode.INT1TYPE:
            case DataTypeCode.BITTYPE:
            case DataTypeCode.INT2TYPE:
                throw new NotImplementedException();
            case DataTypeCode.INT4TYPE:
            {
                if (typeof(T) == typeof(int))
                {
                    _reader.TryReadLittleEndian(out int value);
                    result = (T)(object)value;
                }

                break;
            }
            case DataTypeCode.FLT8TYPE:
            case DataTypeCode.INT8TYPE:
                throw new NotImplementedException();

            // Variable-Length
            // ByteLen
            case DataTypeCode.GUIDTYPE:
            case DataTypeCode.INTNTYPE:
            case DataTypeCode.BITNTYPE:
            case DataTypeCode.DECIMALNTYPE:
            case DataTypeCode.NUMERICNTYPE:
            case DataTypeCode.FLTNTYPE:
            case DataTypeCode.MONEYNTYPE:
            case DataTypeCode.DATETIMNTYPE:
            case DataTypeCode.DATENTYPE:
            case DataTypeCode.TIMENTYPE:
            case DataTypeCode.DATETIME2NTYPE:
            case DataTypeCode.DATETIMEOFFSETNTYPE:

            // UShortLen
            case DataTypeCode.BIGVARBINARYTYPE:
            case DataTypeCode.BIGBINARYTYPE:
            case DataTypeCode.BIGCHARTYPE:
                throw new NotSupportedException();

            case DataTypeCode.BIGVARCHARTYPE:
            case DataTypeCode.NVARCHARTYPE:
            {
                if (_reader.Remaining < sizeof(ushort))
                    _reader = await _streamReader.ReadAtLeastAsync(sizeof(ushort), cancellationToken);

                _reader.TryReadLittleEndian(out ushort length);
                if (length is 0xFFFF)
                    result = (T?)(object?)null;

                if (typeCode is DataTypeCode.NVARCHARTYPE)
                    result = (T)(object)Encoding.Unicode.GetString(_reader.UnreadSpan.Slice(0, length));
                else
                {
                    // If UTF8 flag not set then look at codepage or default to ansi/ascii?
                    result = (T)(object)Encoding.UTF8.GetString(_reader.UnreadSpan.Slice(0, length));
                }
                _reader.Advance(length);
                break;
            }
            case DataTypeCode.NCHARTYPE:

            // LongLen
            case DataTypeCode.TEXTTYPE:
            case DataTypeCode.IMAGETYPE:
            case DataTypeCode.NTEXTTYPE:
            case DataTypeCode.SSVARIANTTYPE:
            case DataTypeCode.XMLTYPE:

            // PartLen
            // Also Includes XML/VarChar/VarBinary/NVarChar
            case DataTypeCode.UDTTYPE:
            default:
                throw new ArgumentOutOfRangeException();

            case DataTypeCode.FLT4TYPE:
            case DataTypeCode.DATETIM4TYPE:
            case DataTypeCode.MONEYTYPE:
            case DataTypeCode.DATETIMETYPE:
            case DataTypeCode.MONEY4TYPE:
            case DataTypeCode.DECIMALTYPE:
            case DataTypeCode.NUMERICTYPE:
            case DataTypeCode.CHARTYPE:
            case DataTypeCode.VARCHARTYPE:
            case DataTypeCode.BINARYTYPE:
            case DataTypeCode.VARBINARYTYPE:
                throw new NotSupportedException();
        }

        return result;
    }

    void ConsumeColumn()
    {

    }

    internal void Reset()
    {
        _currentColumn = -1;
        _columnData = null;
        _columnStartPositions.Clear();
    }
}
