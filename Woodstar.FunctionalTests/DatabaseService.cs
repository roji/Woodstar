using System.Collections.Concurrent;
using System.Net;
using Woodstar.Tds;
using Xunit;

namespace Woodstar.FunctionalTests;

[CollectionDefinition("Database")]
public class DatabaseService : ICollectionFixture<DatabaseService>
{
    // public const string EndPoint = "127.0.0.1:1433";
    public const string EndPoint = "192.168.2.105:1433";
    public const string Username = "sa";
    public const string Password = "Abcd5678";
    public const string Database = "test";

    internal ValueTask<SqlServerStreamConnection> OpenConnectionAsync(CancellationToken cancellationToken = default)
    {
        return SqlServerStreamConnection.ConnectAsync(IPEndPoint.Parse(EndPoint), cancellationToken);
    }
}
