using Microsoft.Data.SqlClient;
using Woodstar.Buffers;
using Woodstar.Pipelines;
using Woodstar.Tds;
using Woodstar.Tds.Messages;
using Woodstar.Tds.Packets;
using Woodstar.Tds.Tokens;
using Xunit;

namespace Woodstar.FunctionalTests;

[Collection("Database")]
public class DebugTests
{
    readonly DatabaseService _databaseService;

    public DebugTests(DatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    [Fact]
    public async Task SqlClient()
    {
        const string ConnectionString = $"Server=127.0.0.1;User ID={DatabaseService.Username};Password={DatabaseService.Password};Initial Catalog={DatabaseService.Database};Integrated Security=False;TrustServerCertificate=true;";

        var builder = new SqlConnectionStringBuilder(ConnectionString);
        builder.Encrypt = false;
        builder.Authentication = SqlAuthenticationMethod.SqlPassword;
        await using var conn = new SqlConnection(builder.ToString());
        await conn.OpenAsync();
        var command = $"SELECT value FROM GENERATE_SERIES(1, {10});";
        //
        // //Warmup
        // for (int i = 0; i < 1000; i++)
        // {
        using var cmd = new SqlCommand(command, conn);
        await using var reader = await cmd.ExecuteReaderAsync();
        //     while (await reader.ReadAsync())
        //     {
        //     }
        // }
    }

    [Fact]
    public async Task PreloginTest()
    {
        var stream = await _databaseService.OpenConnectionAsync();

        var dataStreamWriter = new DataStreamWriter(new PipeStreamingWriter(stream.Writer), 4088);
        var message = new PreloginMessage();
        var output = dataStreamWriter.StartMessage(message.Header.Type, message.Header.Status);
        var writer = new StreamingWriter<IStreamingWriter<byte>>(output);
        message.Write(writer);
        writer.Commit();
        dataStreamWriter.EndMessage();
        // await dataStreamWriter.FlushAsync();

        var loginMessage = new Login7Message(DatabaseService.Username, DatabaseService.Password, DatabaseService.Database, new byte[6]);
        output = dataStreamWriter.StartMessage(loginMessage.Header.Type, loginMessage.Header.Status);
        writer = new StreamingWriter<IStreamingWriter<byte>>(output);
        loginMessage.Write(writer);
        writer.Commit();
        dataStreamWriter.EndMessage();

        var sqlBatch = new SqlBatchMessage(new AllHeaders(null, new TransactionDescriptorHeader(0, 1), null), "Select 1;");
        output = dataStreamWriter.StartMessage(sqlBatch.Header.Type, sqlBatch.Header.Status);
        writer = new StreamingWriter<IStreamingWriter<byte>>(output);
        sqlBatch.Write(writer);
        writer.Commit();
        dataStreamWriter.EndMessage();

        await dataStreamWriter.FlushAsync();

        var pipeReader = new SimplePipeReader(stream.Reader, Timeout.InfiniteTimeSpan);
        await pipeReader.ReadAtLeastAsync(43 + 8);
        pipeReader.Advance(43);
        var tokenReader = new TokenReader(pipeReader);
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();

        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
        await tokenReader.MoveNextAsync();
    }
}
