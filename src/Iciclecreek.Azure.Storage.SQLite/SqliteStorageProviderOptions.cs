namespace Iciclecreek.Azure.Storage.SQLite;

/// <summary>Configuration options for <see cref="SqliteStorageProvider"/>.</summary>
public sealed class SqliteStorageProviderOptions
{
    /// <summary>Hostname suffix for synthetic service URIs. Default: <c>storage.sqlite.local</c>.</summary>
    public string HostnameSuffix { get; set; } = "storage.sqlite.local";

    /// <summary>Whether to create the root directory if it doesn't exist. Default: <c>true</c>.</summary>
    public bool CreateRootIfMissing { get; set; } = true;
}
