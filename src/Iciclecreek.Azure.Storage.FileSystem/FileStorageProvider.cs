using System.Collections.Concurrent;
using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem;

public sealed class FileStorageProvider
{
    private readonly ConcurrentDictionary<string, FileStorageAccount> _accounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly FileStorageProviderOptions _options;

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

        // Rehydrate existing accounts from disk so restarts see prior state.
        foreach (var dir in Directory.EnumerateDirectories(RootPath))
        {
            var name = Path.GetFileName(dir);
            if (!string.IsNullOrEmpty(name))
                _accounts.TryAdd(name, new FileStorageAccount(this, name));
        }
    }

    public string RootPath { get; }

    public string HostnameSuffix => _options.HostnameSuffix;

    public int LockRetryCount => _options.LockRetryCount;

    public TimeSpan LockRetryDelay => _options.LockRetryDelay;

    public JsonSerializerOptions JsonSerializerOptions => _options.JsonSerializerOptions;

    public FileStorageAccount AddAccount(string accountName)
    {
        if (string.IsNullOrWhiteSpace(accountName))
            throw new ArgumentException("Account name is required.", nameof(accountName));

        var account = _accounts.GetOrAdd(accountName, n => new FileStorageAccount(this, n));
        Directory.CreateDirectory(account.BlobsRootPath);
        Directory.CreateDirectory(account.TablesRootPath);
        return account;
    }

    public bool TryGetAccount(string name, out FileStorageAccount? account)
        => _accounts.TryGetValue(name, out account);

    public FileStorageAccount GetAccount(string name)
        => _accounts.TryGetValue(name, out var a)
            ? a
            : throw new InvalidOperationException($"No account named '{name}' is registered with this provider.");

    public IReadOnlyCollection<FileStorageAccount> GetAccounts() => _accounts.Values.ToArray();
}
