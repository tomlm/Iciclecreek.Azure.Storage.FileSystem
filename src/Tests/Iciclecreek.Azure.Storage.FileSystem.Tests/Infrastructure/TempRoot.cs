using Iciclecreek.Azure.Storage.FileSystem;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

public sealed class TempRoot : IDisposable
{
    public TempRoot()
    {
        Path = System.IO.Path.Combine(
            System.IO.Path.GetTempPath(),
            "fs-tests",
            Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(Path);
        Provider = new FileStorageProvider(Path);
        Account = Provider.AddAccount("testacct");
    }

    public string Path { get; }
    public FileStorageProvider Provider { get; }
    public FileStorageAccount Account { get; }

    public void Dispose()
    {
        try { Directory.Delete(Path, true); } catch { }
    }
}
