namespace Iciclecreek.Azure.Storage.FileSystem;

/// <summary>
/// Represents a single storage account backed by a subdirectory under the provider's root path.
/// Provides connection strings and service URIs that can be used to construct
/// <see cref="Blobs.FileBlobServiceClient"/>, <see cref="Tables.FileTableServiceClient"/>, and related clients.
/// </summary>
public sealed class FileStorageAccount
{
    internal FileStorageAccount(FileStorageProvider provider, string name)
    {
        Provider = provider;
        Name = name;
        RootPath = Path.Combine(provider.RootPath, name);
        BlobsRootPath = Path.Combine(RootPath, "blobs");
        TablesRootPath = Path.Combine(RootPath, "tables");
        QueuesRootPath = Path.Combine(RootPath, "queues");
        BlobServiceUri = new Uri($"https://{name}.blob.{provider.HostnameSuffix}/");
        TableServiceUri = new Uri($"https://{name}.table.{provider.HostnameSuffix}/");
        QueueServiceUri = new Uri($"https://{name}.queue.{provider.HostnameSuffix}/");
    }

    /// <summary>The <see cref="FileStorageProvider"/> that owns this account.</summary>
    public FileStorageProvider Provider { get; }

    /// <summary>Short name of this account (e.g. <c>"devaccount"</c>).</summary>
    public string Name { get; }

    /// <summary>Absolute path to the account's root directory.</summary>
    public string RootPath { get; }

    /// <summary>Absolute path to the <c>blobs/</c> subdirectory.</summary>
    public string BlobsRootPath { get; }

    /// <summary>Absolute path to the <c>tables/</c> subdirectory.</summary>
    public string TablesRootPath { get; }

    /// <summary>Absolute path to the <c>queues/</c> subdirectory.</summary>
    public string QueuesRootPath { get; }

    /// <summary>Synthetic blob-service URI (e.g. <c>https://devaccount.blob.storage.file.local/</c>).</summary>
    public Uri BlobServiceUri { get; }

    /// <summary>Synthetic table-service URI (e.g. <c>https://devaccount.table.storage.file.local/</c>).</summary>
    public Uri TableServiceUri { get; }

    /// <summary>Synthetic queue-service URI (e.g. <c>https://devaccount.queue.storage.file.local/</c>).</summary>
    public Uri QueueServiceUri { get; }

    /// <summary>
    /// Returns a fabricated connection string that can be passed to any <c>File*</c> client constructor.
    /// The account key is fake and should never be used for signature validation.
    /// </summary>
    public string GetConnectionString()
    {
        const string fakeKey = "RmlsZUZha2VBY2NvdW50S2V5Tm90UmVhbA==";
        return $"DefaultEndpointsProtocol=https;"
             + $"AccountName={Name};"
             + $"AccountKey={fakeKey};"
             + $"BlobEndpoint={BlobServiceUri};"
             + $"TableEndpoint={TableServiceUri};"
             + $"QueueEndpoint={QueueServiceUri};"
             + $"EndpointSuffix={Provider.HostnameSuffix}";
    }
}
