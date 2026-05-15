using Iciclecreek.Azure.Storage.SQLite.Internal;

namespace Iciclecreek.Azure.Storage.SQLite;

/// <summary>
/// Represents a single storage account backed by a SQLite database file.
/// </summary>
public sealed class SqliteStorageAccount
{
    internal SqliteStorageAccount(SqliteStorageProvider provider, string name)
    {
        Provider = provider;
        Name = name;
        DbPath = Path.Combine(provider.RootPath, $"{name}.db");
        Db = new SqliteDb(DbPath);
        BlobServiceUri = new Uri($"https://{name}.blob.{provider.HostnameSuffix}/");
        TableServiceUri = new Uri($"https://{name}.table.{provider.HostnameSuffix}/");
        QueueServiceUri = new Uri($"https://{name}.queue.{provider.HostnameSuffix}/");
    }

    public SqliteStorageProvider Provider { get; }
    public string Name { get; }
    public string DbPath { get; }
    internal SqliteDb Db { get; }
    public Uri BlobServiceUri { get; }
    public Uri TableServiceUri { get; }
    public Uri QueueServiceUri { get; }

    public string GetConnectionString()
    {
        const string fakeKey = "U3FsaXRlRmFrZUFjY291bnRLZXlOb3RSZWFs";
        return $"DefaultEndpointsProtocol=https;"
             + $"AccountName={Name};"
             + $"AccountKey={fakeKey};"
             + $"BlobEndpoint={BlobServiceUri};"
             + $"TableEndpoint={TableServiceUri};"
             + $"QueueEndpoint={QueueServiceUri};"
             + $"EndpointSuffix={Provider.HostnameSuffix}";
    }
}
