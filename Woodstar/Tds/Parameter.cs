using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;
using Woodstar.SqlServer;

namespace Woodstar.Tds;

readonly struct Parameter
{
    public Parameter(object? value, SqlServerConverterOptions converterOptions, SqlServerTypeId typeId, SizeResult? size, bool isValueDependent, object? writeState = null)
    {
        Value = value;
        ConverterOptions = converterOptions;
        TypeId = typeId;
        Size = size;
        IsValueDependent = isValueDependent;
        WriteState = writeState;
    }

    [MemberNotNullWhen(false, nameof(Size))]
    public bool IsDbNull => Size is null;

    /// Value can be an instance of IParameterSession or a direct parameter value.
    public object? Value { get; init; }
    /// Size set to null represents a db null.
    public SizeResult? Size { get; init; }
    public SqlServerTypeId TypeId { get; init; }

    public SqlServerConverterOptions ConverterOptions { get; init; }
    public object? WriteState { get; init; }
    public bool IsValueDependent { get; }

    public bool TryGetParameterSession([NotNullWhen(true)]out IParameterSession? value)
    {
        if (Value is IParameterSession session)
        {
            value = session;
            return true;
        }

        value = null;
        return false;
    }
}

interface IParameterValueReader : IValueReader
{
    void ReadAsObject(object? value);
}

static class ParameterValueReaderExtensions
{
    public static void ReadParameterValue<TReader>(this ref TReader reader, object? value) where TReader : struct, IParameterValueReader
    {
        if (value is IParameterSession session)
        {
            if (session.IsBoxedValue)
                reader.ReadAsObject(session.Value); // Just avoid the GVM call.
            else
                session.ApplyReader(ref reader);
        }
        else
            reader.ReadAsObject(value);
    }
}

static class SqlServerConverterOptionsExtensions
{
    public static Parameter CreateParameter(this SqlServerConverterOptions converterOptions, object? parameterValue, SqlServerTypeId? typeId, bool nullStructValueIsDbNull = true)
    {
        var reader = new ValueReader(converterOptions, typeId, nullStructValueIsDbNull);
        reader.ReadParameterValue(parameterValue);
        return new Parameter(parameterValue, converterOptions, reader.TypeId, reader.Size, isValueDependent: false, reader.WriteState);
    }

    struct ValueReader: IParameterValueReader
    {
        readonly SqlServerConverterOptions _converterOptions;
        readonly SqlServerTypeId? _typeId;
        readonly bool _nullStructValueIsDbNull;
        object? _writeState;
        public SqlServerTypeId TypeId { get; private set; }
        public SizeResult? Size { get; private set; }
        public object? WriteState => _writeState;

        public ValueReader(SqlServerConverterOptions converterOptions, SqlServerTypeId? typeId, bool nullStructValueIsDbNull)
        {
            _converterOptions = converterOptions;
            _typeId = typeId;
            _nullStructValueIsDbNull = nullStructValueIsDbNull;
            Size = null;
        }

        public void Read<T>(T? value)
        {
            var converterOptions = _converterOptions;
            TypeId = _typeId ??converterOptions.GetTypeId(value);
            if (!converterOptions.IsDbNullValue(value))
                Size = converterOptions.GetSize(value, out _writeState);
        }

        public void ReadAsObject(object? value)
        {
            var converterOptions = _converterOptions;
            TypeId = _typeId ?? converterOptions.GetTypeIdAsObject(value);
            if ((!_nullStructValueIsDbNull || value is not null) && !converterOptions.IsDbNullValueAsObject(value))
                Size = converterOptions.GetSizeAsObject(value, out _writeState);
        }
    }
}

static class ParameterExtensions
{
    public static void Write<TWriter>(this Parameter parameter, StreamingWriter<TWriter> writer) where TWriter : IStreamingWriter<byte>
    {
        if (parameter.IsDbNull)
            return;

        var reader = new ValueWriter<TWriter>(async: false, writer, parameter.WriteState, parameter.ConverterOptions, CancellationToken.None);
        reader.ReadParameterValue(parameter.Value);

    }

    public static ValueTask WriteAsync<TWriter>(this Parameter parameter, StreamingWriter<TWriter> writer, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        if (parameter.IsDbNull)
            return new ValueTask();

        var reader = new ValueWriter<TWriter>(async: true, writer, parameter.WriteState, parameter.ConverterOptions, cancellationToken);
        reader.ReadParameterValue(parameter.Value);

        return reader.Result;
    }

    public static BufferedOutput GetBufferedOutput(this Parameter parameter)
    {
        // TODO some array pool backed thing
        var writer = parameter.ConverterOptions.GetBufferedWriter();
        var reader = new ValueWriter<IStreamingWriter<byte>>(async: false, writer, parameter.WriteState, parameter.ConverterOptions, CancellationToken.None);
        return new BufferedOutput(default);
    }

    struct ValueWriter<TWriter> : IParameterValueReader where TWriter : IStreamingWriter<byte>
    {
        readonly bool _async;
        readonly StreamingWriter<TWriter> _writer;
        readonly object? _writeState;
        readonly SqlServerConverterOptions _converterOptions;
        readonly CancellationToken _cancellationToken;

        public ValueWriter(bool async, StreamingWriter<TWriter> writer, object? writeState, SqlServerConverterOptions converterOptions, CancellationToken cancellationToken)
        {
            _async = async;
            _writer = writer;
            _writeState = writeState;
            _converterOptions = converterOptions;
            _cancellationToken = cancellationToken;
        }

        public ValueTask Result { get; private set; }

        public void Read<T>(T? value)
        {
            Debug.Assert(value is not null);
            if (_async)
            {
                try
                {
                    Result = SqlServerConverter.WriteAsync(_writer, value, _writeState, _converterOptions, _cancellationToken);
                }
                catch (Exception ex)
                {
                    Result = new ValueTask(Task.FromException(ex));
                }
            }
            else
            {
                SqlServerConverter.Write(_writer, value, _writeState, _converterOptions);
            }
        }

        public void ReadAsObject(object? value)
        {
            Debug.Assert(value is not null);
            if (_async)
            {
                try
                {
                    Result = SqlServerConverter.WriteAsObjectAsync(_writer, value, _writeState, _converterOptions, _cancellationToken);
                }
                catch (Exception ex)
                {
                    Result = new ValueTask(Task.FromException(ex));
                }
            }
            else
            {
                SqlServerConverter.Write(_writer, value, _writeState, _converterOptions);
            }
        }
    }
}
