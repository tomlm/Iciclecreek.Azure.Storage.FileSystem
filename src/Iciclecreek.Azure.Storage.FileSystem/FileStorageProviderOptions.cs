using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem;

/// <summary>
/// Configuration options for <see cref="FileStorageProvider"/>.
/// </summary>
public sealed class FileStorageProviderOptions
{
    /// <summary>
    /// Hostname suffix used to build synthetic blob/table service URIs.
    /// Default is <c>"storage.file.local"</c>, producing URIs like
    /// <c>https://myaccount.blob.storage.file.local/</c>.
    /// </summary>
    public string HostnameSuffix { get; set; } = "storage.file.local";

    /// <summary>
    /// When <c>true</c> (the default), the root directory is created automatically if it does not exist.
    /// </summary>
    public bool CreateRootIfMissing { get; set; } = true;

    /// <summary>Number of retries when acquiring an exclusive file lock. Default is 50.</summary>
    public int LockRetryCount { get; set; } = 50;

    /// <summary>Delay between file-lock retry attempts. Default is 20 ms.</summary>
    public TimeSpan LockRetryDelay { get; set; } = TimeSpan.FromMilliseconds(20);

    /// <summary>
    /// JSON serializer options used for sidecar metadata files and table entity files.
    /// </summary>
    public JsonSerializerOptions JsonSerializerOptions { get; set; } = new()
    {
        WriteIndented = false,
        PropertyNamingPolicy = null,
    };
}
