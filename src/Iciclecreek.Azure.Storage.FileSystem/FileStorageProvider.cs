using System.Collections.Concurrent;
using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem;

/// <summary>
/// Top-level entry point for the filesystem-backed Azure Storage test fake.
/// Manages storage accounts backed by subdirectories under a root path.
/// Existing accounts are automatically rehydrated when the provider is created
/// against a directory that already contains account folders.
/// </summary>
public sealed class FileStorageProvider
{
    private readonly ConcurrentDictionary<string, FileStorageAccount> _accounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly FileStorageProviderOptions _options;

    /// <summary>
    /// Creates a new <see cref="FileStorageProvider"/> rooted at <paramref name="rootPath"/>.
    /// The directory is created if <see cref="FileStorageProviderOptions.CreateRootIfMissing"/> is true (the default).
    /// Any subdirectories already present are treated as previously-created accounts.
    /// </summary>
    /// <param name="rootPath">Absolute or relative path to the storage root directory.</param>
    /// <param name="options">Optional configuration. Pass <c>null</c> for defaults.</param>
    public FileStorageProvider(string rootPath, FileStorageProviderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("Root path is required.", nameof(rootPath));

        _options = options ?? new FileStorageProviderOptions();
        RootPath = Path.GetFullPath(rootPath);

        if (_options.CreateRootIfMissing)
            Directory.CreateDirectory(RootPath);
        else if (!Directory.Exists(RootPath))
            throw new DirectoryNotFoundException($"Root path does not exist: {RootPath}");

        foreach (var dir in Directory.EnumerateDirectories(RootPath))
        {
            var name = Path.GetFileName(dir);
            if (!string.IsNullOrEmpty(name))
                _accounts.TryAdd(name, new FileStorageAccount(this, name));
        }
    }

    /// <summary>Absolute path to the root storage directory.</summary>
    public string RootPath { get; }

    /// <summary>Hostname suffix used to construct synthetic blob/table service URIs (e.g. <c>storage.file.local</c>).</summary>
    public string HostnameSuffix => _options.HostnameSuffix;

    /// <summary>Number of retries when acquiring an exclusive file lock.</summary>
    public int LockRetryCount => _options.LockRetryCount;

    /// <summary>Delay between file-lock retry attempts.</summary>
    public TimeSpan LockRetryDelay => _options.LockRetryDelay;

    /// <summary>JSON serializer options used for sidecar and entity files.</summary>
    public JsonSerializerOptions JsonSerializerOptions => _options.JsonSerializerOptions;

    /// <summary>
    /// Registers a storage account and creates its <c>blobs/</c> and <c>tables/</c> subdirectories.
    /// If the account already exists it is returned without modification.
    /// </summary>
    /// <param name="accountName">A short, filesystem-safe name (e.g. <c>"devaccount"</c>).</param>
    /// <returns>The <see cref="FileStorageAccount"/> for this name.</returns>
    public FileStorageAccount AddAccount(string accountName)
    {
        if (string.IsNullOrWhiteSpace(accountName))
            throw new ArgumentException("Account name is required.", nameof(accountName));

        var account = _accounts.GetOrAdd(accountName, n => new FileStorageAccount(this, n));
        Directory.CreateDirectory(account.BlobsRootPath);
        Directory.CreateDirectory(account.TablesRootPath);
        return account;
    }

    /// <summary>Attempts to retrieve a previously-registered account by name.</summary>
    public bool TryGetAccount(string name, out FileStorageAccount? account)
        => _accounts.TryGetValue(name, out account);

    /// <summary>
    /// Retrieves a registered account by name.
    /// Throws <see cref="InvalidOperationException"/> if no account with this name exists.
    /// </summary>
    public FileStorageAccount GetAccount(string name)
        => _accounts.TryGetValue(name, out var a)
            ? a
            : throw new InvalidOperationException($"No account named '{name}' is registered with this provider.");

    /// <summary>Returns a snapshot of all registered accounts.</summary>
    public IReadOnlyCollection<FileStorageAccount> GetAccounts() => _accounts.Values.ToArray();
}
