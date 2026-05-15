using System.Collections.Concurrent;

namespace Iciclecreek.Azure.Storage.Memory;

public sealed class MemoryStorageProvider
{
    private readonly ConcurrentDictionary<string, MemoryStorageAccount> _accounts = new(StringComparer.OrdinalIgnoreCase);

    public MemoryStorageProvider(MemoryStorageProviderOptions? options = null)
    {
        HostnameSuffix = options?.HostnameSuffix ?? "storage.memory.local";
    }

    public string HostnameSuffix { get; }

    public MemoryStorageAccount AddAccount(string accountName)
    {
        return _accounts.GetOrAdd(accountName, name => new MemoryStorageAccount(this, name));
    }

    public bool TryGetAccount(string name, out MemoryStorageAccount? account)
        => _accounts.TryGetValue(name, out account);

    public MemoryStorageAccount GetAccount(string name)
        => _accounts.TryGetValue(name, out var account)
            ? account
            : throw new InvalidOperationException($"Account '{name}' not found.");

    public IReadOnlyCollection<MemoryStorageAccount> GetAccounts()
        => _accounts.Values.ToArray();
}
