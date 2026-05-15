using Iciclecreek.Azure.Storage.SQLite;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

public sealed class TempDb : IDisposable
{
    public TempDb()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            "sqlite-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
        Provider = new SqliteStorageProvider(Path);
        Account = Provider.AddAccount("testacct");
    }

    public string Path { get; }
    public SqliteStorageProvider Provider { get; }
    public SqliteStorageAccount Account { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
