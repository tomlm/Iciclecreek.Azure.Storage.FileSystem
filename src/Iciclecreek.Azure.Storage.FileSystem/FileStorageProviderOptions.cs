using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem;

public sealed class FileStorageProviderOptions
{
    public string HostnameSuffix { get; set; } = "storage.file.local";

    public bool CreateRootIfMissing { get; set; } = true;

    public int LockRetryCount { get; set; } = 50;

    public TimeSpan LockRetryDelay { get; set; } = TimeSpan.FromMilliseconds(20);

    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = null,
    };
}
