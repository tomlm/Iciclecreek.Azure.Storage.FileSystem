using System.Collections.Concurrent;
using Iciclecreek.Azure.Storage.Memory.Internal;

namespace Iciclecreek.Azure.Storage.Memory;

public sealed class MemoryStorageAccount
{
    internal MemoryStorageAccount(MemoryStorageProvider provider, string name)
    {
        Provider = provider;
        Name = name;
        BlobServiceUri = new Uri($"https://{name}.blob.{provider.HostnameSuffix}/");
        TableServiceUri = new Uri($"https://{name}.table.{provider.HostnameSuffix}/");
        QueueServiceUri = new Uri($"https://{name}.queue.{provider.HostnameSuffix}/");
    }

    public MemoryStorageProvider Provider { get; }
    public string Name { get; }
    public Uri BlobServiceUri { get; }
    public Uri TableServiceUri { get; }
    public Uri QueueServiceUri { get; }

    // Internal stores
    internal readonly ConcurrentDictionary<string, ContainerStore> Containers = new();
    internal readonly ConcurrentDictionary<string, TableStore> Tables = new();
    internal readonly ConcurrentDictionary<string, QueueStore> Queues = new();

    public string GetConnectionString()
    {
        const string fakeKey = "TWVtb3J5RmFrZUFjY291bnRLZXlOb3RSZWFs";
        return $"DefaultEndpointsProtocol=https;"
             + $"AccountName={Name};"
             + $"AccountKey={fakeKey};"
             + $"BlobEndpoint={BlobServiceUri};"
             + $"TableEndpoint={TableServiceUri};"
             + $"QueueEndpoint={QueueServiceUri};"
             + $"EndpointSuffix={Provider.HostnameSuffix}";
    }
}
