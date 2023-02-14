using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading.Tasks;
using Woodstar.Pipelines;
using Woodstar.Buffers;
using Woodstar.Tds.Packets;

namespace Woodstar.Tds.Tokens;

class TokenReader
{
    readonly SimplePipeReader _pipeReader;

    public TokenReader(SimplePipeReader pipeReader)
    {
        _pipeReader = pipeReader;
    }

    public Token Current { get; private set; }

    public async ValueTask MoveNextAsync()
    {
        ReadOnlySequence<byte> result;
        long consumed;
        Token? token;
        TokenType? tokenType = default;
        ReadStatus status;
        do
        {
            result = await _pipeReader.ReadAtLeastAsync(1);
        }
        while ((status = MoveNext(result, ref tokenType, out token, out consumed)) is ReadStatus.NeedMoreData);

        if (status is ReadStatus.InvalidData)
            throw new InvalidOperationException();

        _pipeReader.Advance(consumed);
        Debug.Assert(token is not null);
        Current = token;


        static ReadStatus MoveNext(ReadOnlySequence<byte> result, ref TokenType? tokenType, out Token? token, out long consumed)
        {
            token = null;
            consumed = 0;
            var reader = new DataStreamReader(result);

            if (tokenType is null)
            {
                var tokenTypeReadResult = reader.TryRead(out var tokenTypeByte);
                Debug.Assert(tokenTypeReadResult);
                tokenType = (TokenType)tokenTypeByte;
                if (BackendMessage.DebugEnabled && !Enum.IsDefined(tokenType.Value))
                    throw new ArgumentOutOfRangeException();
            }

            switch (tokenType)
            {
                case TokenType.LOGINACK:
                {
                    if (!reader.TryReadLittleEndian(out ushort length) && !reader.HasAtLeast(length))
                        return ReadStatus.NeedMoreData;

                    reader.TryRead(out var @interface);
                    Span<byte> tdsVersion = stackalloc byte[4];
                    reader.TryCopyTo(tdsVersion);
                    reader.Advance(4);
                    reader.TryReadBVarchar(out var programName);
                    Span<byte> versionBytes = stackalloc byte[4];
                    reader.TryCopyTo(versionBytes);
                    reader.Advance(4);
                    var version = new Version(versionBytes[0], versionBytes[1], (versionBytes[2] << 8) | versionBytes[3]);

                    token = new LoginAckToken(@interface, tdsVersion.ToArray(), programName, version);
                    consumed = reader.Consumed;
                    return ReadStatus.Done;
                }
                case TokenType.INFO:
                {
                    if (!reader.TryReadLittleEndian(out ushort length) && !reader.HasAtLeast(length))
                        return ReadStatus.NeedMoreData;

                    reader.TryReadLittleEndian(out int number);
                    reader.TryRead(out var state);
                    reader.TryRead(out var @class);
                    reader.TryReadUsVarchar(out var msgText);
                    reader.TryReadBVarchar(out var serverName);
                    reader.TryReadBVarchar(out var procName);
                    reader.TryReadLittleEndian(out int lineNumber);

                    token = new InfoToken(number, state, @class, msgText, serverName, procName, lineNumber);
                    consumed = reader.Consumed;
                    return ReadStatus.Done;
                }
                case TokenType.ENVCHANGE:
                {
                    if (!reader.TryReadLittleEndian(out ushort length) && !reader.HasAtLeast(length))
                        return ReadStatus.NeedMoreData;

                    reader.TryRead(out var envTypeByte);
                    var envType = (EnvChangeType)envTypeByte;
                    if (BackendMessage.DebugEnabled && !Enum.IsDefined(envType))
                        throw new ArgumentOutOfRangeException();

                    switch (envType)
                    {
                        case EnvChangeType.Language:
                        case EnvChangeType.PacketSize:
                        case EnvChangeType.ResetAck:
                            reader.TryReadBVarchar(out var newValue);
                            reader.TryReadBVarchar(out var oldValue);
                            token = new EnvChangeToken(envType, newValue, oldValue);
                            break;
                        default:
                            throw new ArgumentOutOfRangeException();
                    }

                    consumed = reader.Consumed;
                    return ReadStatus.Done;
                }
                case TokenType.DONE:
                {
                    if (!reader.HasAtLeast(sizeof(ushort) + sizeof(ushort) + sizeof(ulong)))
                        return ReadStatus.NeedMoreData;

                    reader.TryReadLittleEndian(out ushort statusBytes);
                    var status = (DoneStatus)statusBytes;
                    if (BackendMessage.DebugEnabled && !Enum.IsDefined(status))
                        throw new ArgumentOutOfRangeException();

                    reader.TryReadLittleEndian(out ushort curCmd);
                    reader.TryReadLittleEndian(out ulong doneRowCount);
                    token = new DoneToken(status, curCmd, doneRowCount);
                    consumed = reader.Consumed;
                    return ReadStatus.Done;
                }
                case TokenType.COLMETADATA:
                {
                    if (!reader.TryReadLittleEndian(out ushort count))
                        return ReadStatus.NeedMoreData;

                    count = (ushort)(count is 0xFF ? 0 : count);

                    if (count is 0)
                    {
                        reader.Advance(sizeof(ushort) * 2);
                        token = new ColumnMetadataToken(new());
                        return ReadStatus.Done;
                    }

                    var columns = new List<ColumnData>();
                    for (var i = 0; i < count; i++)
                    {
                        if (!reader.TryReadLittleEndian(out uint userType))
                            return ReadStatus.NeedMoreData;
                        if (!reader.TryReadLittleEndian(out ushort flagsBytes))
                            return ReadStatus.NeedMoreData;
                        var flags = (ColumnDataFlags)flagsBytes;
                        if (BackendMessage.DebugEnabled && !Enum.IsDefined(flags))
                            throw new ArgumentOutOfRangeException();

                        if (!reader.TryRead(out var typeByte))
                            return ReadStatus.NeedMoreData;
                        var type = (DataType)typeByte;
                        if (BackendMessage.DebugEnabled && !Enum.IsDefined(type))
                            throw new ArgumentOutOfRangeException();

                        if (!reader.TryReadBVarchar(out var columnName))
                            return ReadStatus.NeedMoreData;
                        columns.Add(new ColumnData(userType, flags, type, columnName));
                    }

                    consumed = reader.Consumed;
                    token = new ColumnMetadataToken(columns);
                    return ReadStatus.Done;
                }

                case TokenType.ROW:
                    token = new RowToken();
                    return ReadStatus.Done;

                case TokenType.TVP_ROW:
                case TokenType.RETURNSTATUS:
                case TokenType.ALTMETADATA:
                case TokenType.DATACLASSIFICATION:
                case TokenType.TABNAME:
                case TokenType.COLINFO:
                case TokenType.ORDER:
                case TokenType.ERROR:
                case TokenType.RETURNVALUE:
                case TokenType.FEATUREEXTACK:
                case TokenType.NBCROW:
                case TokenType.ALTROW:
                case TokenType.SESSIONSTATE:
                case TokenType.SSPI:
                case TokenType.FEDAUTHINFO:
                case TokenType.DONEPROC:
                case TokenType.DONEINPROC:
                default:
                    throw new ArgumentOutOfRangeException();
            }
        }
    }

    enum TokenType : byte
    {
        TVP_ROW = 0x01,
        RETURNSTATUS = 0x79,
        COLMETADATA = 0x81,
        ALTMETADATA = 0x88,
        DATACLASSIFICATION = 0xA3,
        TABNAME = 0xA4,
        COLINFO = 0xA5,
        ORDER = 0xA9,
        ERROR = 0xAA,
        INFO = 0xAB,
        RETURNVALUE = 0xAC,
        LOGINACK = 0xAD,
        FEATUREEXTACK = 0xAE,
        ROW = 0xD1,
        NBCROW = 0xD2,
        ALTROW = 0xD3,
        ENVCHANGE = 0xE3,
        SESSIONSTATE = 0xE4,
        SSPI = 0xED,
        FEDAUTHINFO = 0xEE,
        DONE = 0xFD,
        DONEPROC = 0xFE,
        DONEINPROC = 0xFF,
        // OFFSET - removed in 7.2
    }
}

enum DataType : byte
{
    // Fixed-Length
    INT1TYPE = 0x30,            // TinyInt
    BITTYPE = 0x32,             // Bit
    INT2TYPE = 0x34,            // SmallInt
    INT4TYPE = 0x38,            // Int
    DATETIM4TYPE = 0x3A,        // SmallDateTime
    FLT4TYPE = 0x3B,            // Real
    MONEYTYPE = 0x3C,           // Money
    DATETIMETYPE = 0x3D,        // DateTime
    FLT8TYPE = 0x3E,            // Float
    MONEY4TYPE = 0x7A,          // SmallMoney
    INT8TYPE = 0x7F,            // BigInt
    DECIMALTYPE = 0x37,         // Decimal
    NUMERICTYPE = 0x3F,         // Numeric

    // Variable-Length
    // ByteLen
    GUIDTYPE = 0x24,            // UniqueIdentifier
    INTNTYPE = 0x26,            // Integer
    BITNTYPE = 0x68,            // Bit
    DECIMALNTYPE = 0x6A,        // Decimal
    NUMERICNTYPE = 0x6C,        // Numeric
    FLTNTYPE = 0x6D,            // Float
    MONEYNTYPE = 0x6E,          // Money
    DATETIMNTYPE = 0x6F,        // DateTime
    DATENTYPE = 0x28,           // Date
    TIMENTYPE = 0x29,           // Time
    DATETIME2NTYPE = 0x2A,      // DataTime2
    DATETIMEOFFSETNTYPE = 0x2B, // DateTimeOffset
    CHARTYPE = 0x2F,            // Char
    VARCHARTYPE = 0x27,         // VarChar
    BINARYTYPE = 0x2D,          // Binary
    VARBINARYTYPE = 0x25,       // VarBinary

    // UShortLen
    BIGVARBINARYTYPE = 0xA5,    // VarBinary
    BIGVARCHARTYPE = 0xA7,      // VarChar
    BIGBINARYTYPE = 0xAD,       // Binary
    BIGCHARTYPE = 0xAF,         // Char
    NVARCHARTYPE = 0xE7,        // NVarChar
    NCHARTYPE = 0xEF,           // NChar

    // LongLen
    TEXTTYPE = 0x23,            // Text
    IMAGETYPE = 0x22,           // Image
    NTEXTTYPE = 0x63,           // NText
    SSVARIANTTYPE = 0x62,       // sql_variant
    XMLTYPE = 0xF1,             // XML

    // PartLen
    // Also Includes XML/VarChar/VarBinary/NVarChar
    UDTTYPE = 0xF0,             // CLR UDT
}
