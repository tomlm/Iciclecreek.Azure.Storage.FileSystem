using System.Collections.Concurrent;

namespace Iciclecreek.Azure.Storage.SQLite;

/// <summary>
/// Top-level entry point for the SQLite-backed Azure Storage provider.
/// Each account is stored as a separate <c>.db</c> file under the root path.
/// </summary>
public sealed class SqliteStorageProvider
{
    private readonly ConcurrentDictionary<string, SqliteStorageAccount> _accounts = new(StringComparer.OrdinalIgnoreCase);
    private readonly SqliteStorageProviderOptions _options;

    public SqliteStorageProvider(string rootPath, SqliteStorageProviderOptions? options = null)
    {
        if (string.IsNullOrWhiteSpace(rootPath))
            throw new ArgumentException("Root path is required.", nameof(rootPath));

        _options = options ?? new SqliteStorageProviderOptions();
        RootPath = Path.GetFullPath(rootPath);

        if (_options.CreateRootIfMissing)
            Directory.CreateDirectory(RootPath);
        else if (!Directory.Exists(RootPath))
            throw new DirectoryNotFoundException($"Root path does not exist: {RootPath}");

        // Rehydrate existing accounts from .db files
        foreach (var dbFile in Directory.EnumerateFiles(RootPath, "*.db"))
        {
            var name = Path.GetFileNameWithoutExtension(dbFile);
            if (!string.IsNullOrEmpty(name))
                _accounts.TryAdd(name, new SqliteStorageAccount(this, name));
        }
    }

    public string RootPath { get; }
    public string HostnameSuffix => _options.HostnameSuffix;

    public SqliteStorageAccount AddAccount(string accountName)
    {
        if (string.IsNullOrWhiteSpace(accountName))
            throw new ArgumentException("Account name is required.", nameof(accountName));

        return _accounts.GetOrAdd(accountName, n => new SqliteStorageAccount(this, n));
    }

    public bool TryGetAccount(string name, out SqliteStorageAccount? account)
        => _accounts.TryGetValue(name, out account);

    public SqliteStorageAccount GetAccount(string name)
        => _accounts.TryGetValue(name, out var a)
            ? a
            : throw new InvalidOperationException($"No account named '{name}' is registered.");

    public IReadOnlyCollection<SqliteStorageAccount> GetAccounts() => _accounts.Values.ToArray();
}
