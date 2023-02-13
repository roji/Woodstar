using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Columns;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using Microsoft.Data.SqlClient;

namespace Woodstar.Benchmark;

[Config(typeof(Config))]
public class Benchmarks
{
    const int Connections = 10;
    class Config : ManualConfig
    {
        public Config()
        {
            Add(new SimpleJobAttribute(targetCount: 20).Config);
            AddDiagnoser(MemoryDiagnoser.Default);
            AddDiagnoser(ThreadingDiagnoser.Default);
            AddColumn(new TagColumn("Connections", name => (name.EndsWith("Pipelined") ? 1 : Connections).ToString()));
        }
    }

    const string EndPoint = "127.0.0.1";
    const string Username = "sa";
    const string Password = "Password1234";
    const string Database = "msdb";

    const string ConnectionString = $"Data Source={EndPoint};User ID={Username};Password={Password};Initial Catalog={Database};Integrated Security=False;TrustServerCertificate=true;";
    // static WoodstarDataSourceOptions Options { get; } = new()
    // {
    //     EndPoint = IPEndPoint.Parse(EndPoint),
    //     Username = Username,
    //     Password = Password,
    //     Database = Database,
    //     PoolSize = Connections
    // };


    string _commandText = string.Empty;

    SqlConnection _sqlclientConn;
    //
    // Woodstar.WoodstarConnection _conn;
    // Woodstar.WoodstarCommand _pipeliningCommand;
    // Woodstar.WoodstarCommand _multiplexedPipeliningCommand;
    // Woodstar.WoodstarCommand _multiplexingCommand;
    //
    // [GlobalSetup(Targets = new[] { nameof(PipelinesPipelined), nameof(PipelinesMultiplexingPipelined), nameof(PipelinesMultiplexing) })]
    // public async ValueTask SetupPipelines()
    // {
    //     _commandText = $"SELECT generate_series(1, {RowsPer})";
    //
    //     // Pipelining
    //     var dataSource = new Woodstar.WoodstarDataSource(Options with {PoolSize = 1}, ProtocolOptions);
    //     _conn = new Woodstar.WoodstarConnection(dataSource);
    //     await _conn.OpenAsync();
    //     _pipeliningCommand = new Woodstar.WoodstarCommand(_commandText, _conn);
    //
    //     var dataSource2 = new Woodstar.WoodstarDataSource(Options with {PoolSize = 1}, ProtocolOptions);
    //     _multiplexedPipeliningCommand = dataSource2.CreateCommand(_commandText);
    //
    //     // Multiplexing
    //     var dataSource3 = new Woodstar.WoodstarDataSource(Options, ProtocolOptions);
    //     _multiplexingCommand = dataSource3.CreateCommand(_commandText);
    // }
    //
    [GlobalSetup(Targets = new []{ nameof(SqlClient) })]
    public async ValueTask SetupSqlClient()
    {
        var builder = new SqlConnectionStringBuilder(ConnectionString);
        builder.MinPoolSize = Connections;
        builder.MaxPoolSize = Connections;
        _sqlclientConn = new SqlConnection(builder.ToString());
        await _sqlclientConn.OpenAsync();
        _commandText = $"SELECT value FROM GENERATE_SERIES(1, {RowsPer});";

        //Warmup
        for (int i = 0; i < 1000; i++)
        {
            using var cmd = new SqlCommand(_commandText, _sqlclientConn);
            await using var reader = await cmd.ExecuteReaderAsync();
            while (await reader.ReadAsync())
            {
            }
        }
    }

    [Params(1,10,100,1000)]
    public int RowsPer { get; set; }

    [Benchmark(Baseline = true)]
    public async ValueTask SqlClient()
    {
        using var cmd = new SqlCommand(_commandText, _sqlclientConn);
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
        }
    }
}
