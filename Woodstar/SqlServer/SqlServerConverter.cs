using System;
using System.Threading;
using System.Threading.Tasks;
using Woodstar.Buffers;

namespace Woodstar.SqlServer;

static class SqlServerConverter
{
    public static void Write<T, TWriter>(StreamingWriter<TWriter> writer, T value, object? writeState, SqlServerConverterOptions options) where TWriter : IStreamingWriter<byte>
    {
        if (typeof(T) == typeof(int))
        {

        }
        else if (typeof(T) == typeof(string))
        {

        }
        else
            throw new NotSupportedException($"{typeof(T)}");
    }

    public static async ValueTask WriteAsync<T, TWriter>(StreamingWriter<TWriter> writer, T value, object? writeState, SqlServerConverterOptions options, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        if (typeof(T) == typeof(int))
        {

        }
        else if (typeof(T) == typeof(string))
        {

        }
        else
            throw new NotSupportedException($"{typeof(T)}");
    }

    public static void WriteAsObject<TWriter>(StreamingWriter<TWriter> writer, object value, object? writeState, SqlServerConverterOptions options) where TWriter : IStreamingWriter<byte>
    {
        if (value is int)
        {

        }
        else if (value is string)
        {

        }
        else
            throw new NotSupportedException($"{value.GetType()}");
    }

    public static async ValueTask WriteAsObjectAsync<TWriter>(StreamingWriter<TWriter> writer, object value, object? writeState, SqlServerConverterOptions options, CancellationToken cancellationToken) where TWriter : IStreamingWriter<byte>
    {
        if (value is int)
        {

        }
        else if (value is string)
        {

        }
        else
            throw new NotSupportedException($"{value.GetType()}");
    }
}
