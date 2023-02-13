using System.Text;
using Woodstar.Buffers;

namespace Woodstar.SqlServer;

class SqlServerConverterOptions
{
    public SqlServerTypeId GetTypeId<T>(T value)
    {
        throw new System.NotImplementedException();
    }

    public bool IsDbNullValue<T>(T value)
    {
        throw new System.NotImplementedException();
    }

    public SizeResult? GetSize<T>(T value, out object? writeState)
    {
        throw new System.NotImplementedException();
    }

    public SqlServerTypeId GetTypeIdAsObject(object? value)
    {
        throw new System.NotImplementedException();
    }

    public bool IsDbNullValueAsObject(object? value)
    {
        throw new System.NotImplementedException();
    }

    public SizeResult? GetSizeAsObject(object? value, out object? writeState)
    {
        throw new System.NotImplementedException();
    }

    public StreamingWriter<IStreamingWriter<byte>> GetBufferedWriter()
    {
        throw new System.NotImplementedException();
    }

    public Encoding TextEncoding { get; set; }
}
