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

enum EnvType : byte
{
    Language = 2,
    PacketSize = 4,
    ResetAck = 18
}

[Flags]
enum DoneStatus : ushort
{
     Final = 0x00,
     More =  0x1,
     Error = 0x2,
     InTransaction = 0x4,
     Count = 0x10,
     Attention = 0x20,
     ServerError = 0x100
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

[Flags]
enum ColumnDataFlags : ushort
{
    Nullable = 1,
    CaseSensitive = 2,
    Updatable = 4,
    Identity = 16,
    Computed = 32,
    FixedLengthClrType = 128,
    SparseColumnSet = 512,
    Encrypted = 1024,
    Hidden = 4096,
    Key = 8192,
    NullableUnknown = 16384
}

abstract class Token
{

}

class LoginAckToken : Token
{
    public byte Interface { get; }
    public byte[] TdsVersion { get; }
    public string ProgramName { get; }
    public Version ProgramVersion { get; }

    public LoginAckToken(byte @interface, byte[] tdsVersion, string programName, Version programVersion)
    {
        Interface = @interface;
        TdsVersion = tdsVersion;
        ProgramName = programName;
        ProgramVersion = programVersion;
    }
}

class InfoToken : Token
{
    public InfoToken(int number, byte state, byte @class, string msgText, string serverName, string procName, int lineNumber)
    {
        Number = number;
        State = state;
        Class = @class;
        MsgText = msgText;
        ServerName = serverName;
        ProcName = procName;
        LineNumber = lineNumber;
    }

    public int Number { get; }
    public byte State { get; }
    public byte Class { get; }
    public string MsgText { get; }
    public string ServerName { get; }
    public string ProcName { get; }
    public int LineNumber { get; }
}

class EnvChangeToken : Token
{
    public EnvChangeToken(EnvType type, object newValue, object oldValue)
    {
        Type = type;
        NewValue = newValue;
        OldValue = oldValue;
    }

    public EnvType Type { get; }
    public object NewValue { get; }
    public object OldValue { get; }
}

class DoneToken : Token
{
    public DoneToken(DoneStatus status, ushort currentCommand, ulong doneRowCount)
    {
        Status = status;
        CurrentCommand = currentCommand;
        DoneRowCount = doneRowCount;
    }

    public DoneStatus Status { get; }
    public ushort CurrentCommand { get; }
    public ulong DoneRowCount { get; }

}

class ColumnMetadataToken : Token
{
    public List<ColumnData> ColumnDatums { get; }

    public ColumnMetadataToken(List<ColumnData> columnDatums)
    {
        ColumnDatums = columnDatums;
    }
}

class RowToken : Token
{
    
}

ref struct MessageReader
{
    SequenceReader<byte> _reader;
    int _packetRemaining;
    public MessageReader(ReadOnlySequence<byte> sequence) => _reader = new SequenceReader<byte>(sequence);

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

struct ColumnData
{
    public uint UserType { get; }
    public ColumnDataFlags Flags { get; }
    public DataType Type { get; }
    public string Name { get; }

    public ColumnData(uint userType, ColumnDataFlags flags, DataType type, string name)
    {
        UserType = userType;
        Flags = flags;
        Type = type;
        Name = name;
    }
}

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
            var reader = new MessageReader(result);

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
                    var envType = (EnvType)envTypeByte;
                    if (BackendMessage.DebugEnabled && !Enum.IsDefined(envType))
                        throw new ArgumentOutOfRangeException();

                    switch (envType)
                    {
                        case EnvType.Language:
                        case EnvType.PacketSize:
                        case EnvType.ResetAck:
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
}
