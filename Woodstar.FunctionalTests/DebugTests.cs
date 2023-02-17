using System.Diagnostics;
using System.Net;
using System.Text;
using Microsoft.Data.SqlClient;
using Woodstar.Buffers;
using Woodstar.Pipelines;
using Woodstar.SqlServer;
using Woodstar.Tds;
using Woodstar.Tds.Messages;
using Woodstar.Tds.Packets;
using Woodstar.Tds.SqlServer;
using Woodstar.Tds.Tds33;
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
    public async Task Multiplexing()
    {
        var dataSource = new WoodstarDataSource(new WoodstarDataSourceOptions
        {
            EndPoint = IPEndPoint.Parse(DatabaseService.EndPoint),
            Username = DatabaseService.Username,
            Password = DatabaseService.Password,
            Database = DatabaseService.Database
        }, new TdsProtocolOptions());

        var cmd = new LowLevelSqlCommand();
        var readerTasks = new Task[1000];
        for (var i = 0; i < readerTasks.Length; i++)
        {
            readerTasks[i] = Execute(dataSource);
        }

        for (var i = 0; i < readerTasks.Length; i++)
        {
            await readerTasks[i];
        }

        async Task Execute(WoodstarDataSource dataSource)
        {
            var batch = await dataSource.WriteMultiplexingCommand(cmd);
            var op = await batch.Single.GetOperation();
            var reader = ((TdsProtocol)op.Protocol).Reader;
            var execution = batch.Single.GetCommandExecution();
            await reader.ReadAndExpectAsync<EnvChangeToken>();
            var metadata = await reader.ReadAndExpectAsync<ColumnMetadataToken>();
            var resultSetReader = await reader.GetResultSetReaderAsync(metadata.ColumnData);
            var value = await resultSetReader.GetAsync<int>();
            // var value2 = await resultSetReader.GetAsync<int>();
            await resultSetReader.MoveToNextRow();
            await reader.ReadAndExpectAsync<DoneToken>();
            op.Complete();
        }
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
        // using var cmd = new LowLevelSqlCommand(command, conn);
        // await using var reader = await cmd.ExecuteReaderAsync();
        //     while (await reader.ReadAsync())
        //     {
        //     }
        // }
    }

    [Fact]
    public async Task PreloginTest()
    {
        var connection = await _databaseService.OpenConnectionAsync();
        //
        // var protocol = await TdsProtocol.StartAsync(connection.Writer, connection.Stream, new SqlServerOptions
        // {
        //     EndPoint = IPEndPoint.Parse(DatabaseService.EndPoint),
        //     Username = DatabaseService.Username,
        //     Password = DatabaseService.Password,
        //     Database = DatabaseService.Database
        // }, null);
        //
        // if (!protocol.TryStartOperation(out var slot, OperationBehavior.None, CancellationToken.None))
        //     throw new InvalidOperationException();
        // var completionPair = protocol.WriteMessageAsync(slot, new SqlBatchMessage(new AllHeaders(null, new TransactionDescriptorHeader(0, 1), null), "SELECT 1"));
        //
        // var commandWriter = new TdsCommandWriter(new SqlServerDatabaseInfo(), Encoding.Unicode);
        // var commandContext = commandWriter.WriteAsync(slot, new LowLevelSqlCommand());

        var dataSource = new WoodstarDataSource(new WoodstarDataSourceOptions
        {
            EndPoint = IPEndPoint.Parse(DatabaseService.EndPoint),
            Username = DatabaseService.Username,
            Password = DatabaseService.Password,
            Database = DatabaseService.Database
        }, new TdsProtocolOptions());

        // while (true)
        // {
            var cmd = new LowLevelSqlCommand();
            var slot = await dataSource.GetSlotAsync(exclusiveUse: true, Timeout.InfiniteTimeSpan);
            var batch = await dataSource.WriteCommandAsync(slot, cmd);
            batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);
            // batch = await dataSource.WriteCommandAsync(slot, cmd);

            // DataSource.WriteMultiplexingCommand(cmd);
            var op = await batch.Single.GetOperation();
            var reader = ((TdsProtocol)op.Protocol).Reader;
            var execution = batch.Single.GetCommandExecution();
            await reader.ReadAndExpectAsync<EnvChangeToken>();
            var metadata = await reader.ReadAndExpectAsync<ColumnMetadataToken>();
            var resultSetReader = await reader.GetResultSetReaderAsync(metadata.ColumnData);
            var value = await resultSetReader.GetAsync<int>();
            // var value2 = await resultSetReader.GetAsync<int>();
            await resultSetReader.MoveToNextRow();
            await reader.ReadAndExpectAsync<DoneToken>();
            op.Complete();
        // }
    }

    struct LowLevelSqlCommand : ISqlCommand
    {
        static readonly ISqlCommand.BeginExecutionDelegate BeginExecutionCoreDelegate = BeginExecutionCore;

        public ISqlCommand.Values GetValues()
        {
            return new ISqlCommand.Values
            {
                StatementText = (SizedString)"SELECT 1",
                ExecutionFlags = ExecutionFlags.Default,
                ExecutionTimeout = Timeout.InfiniteTimeSpan,
                ParameterContext = ParameterContext.Empty,
            };
        }

        public CommandExecution BeginExecution(in ISqlCommand.Values values) => BeginExecutionCore(values);
        public ISqlCommand.BeginExecutionDelegate BeginExecutionMethod => BeginExecutionCoreDelegate;

        static CommandExecution BeginExecutionCore(in ISqlCommand.Values values)
        {
            return CommandExecution.Create(values.ExecutionFlags, values.CommandFlags);
        }
    }
}
