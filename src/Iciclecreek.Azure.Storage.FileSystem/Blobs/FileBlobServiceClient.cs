using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

public class FileBlobServiceClient : BlobServiceClient
{
    internal readonly FileStorageAccount _account;

    public FileBlobServiceClient(string connectionString, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
    }

    public FileBlobServiceClient(Uri serviceUri, FileStorageProvider provider) : base()
    {
        var name = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ExtractAccountName(serviceUri, provider.HostnameSuffix)
            ?? throw new ArgumentException("Cannot determine account name from URI.", nameof(serviceUri));
        _account = provider.GetAccount(name);
    }

    internal FileBlobServiceClient(FileStorageAccount account) : base()
    {
        _account = account;
    }

    public static FileBlobServiceClient FromAccount(FileStorageAccount account) => new(account);

    public override string AccountName => _account.Name;
    public override Uri Uri => _account.BlobServiceUri;

    // ---- GetBlobContainerClient ----

    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
        => new FileBlobContainerClient(_account, blobContainerName);

    // ---- CreateBlobContainer ----

    public override Response<BlobContainerClient> CreateBlobContainer(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobContainerClient(_account, blobContainerName);
        client.Create(publicAccessType, metadata, cancellationToken);
        return Response.FromValue<BlobContainerClient>(client, StubResponse.Created());
    }

    public override async Task<Response<BlobContainerClient>> CreateBlobContainerAsync(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateBlobContainer(blobContainerName, publicAccessType, metadata, cancellationToken); }

    // ---- DeleteBlobContainer ----

    public override Response DeleteBlobContainer(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobContainerClient(_account, blobContainerName);
        return client.Delete(conditions, cancellationToken);
    }

    public override async Task<Response> DeleteBlobContainerAsync(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteBlobContainer(blobContainerName, conditions, cancellationToken); }

    // ---- GetBlobContainers ----

    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        var items = new List<BlobContainerItem>();
        if (!Directory.Exists(_account.BlobsRootPath))
            return new StaticPageable<BlobContainerItem>(items);

        foreach (var dir in Directory.EnumerateDirectories(_account.BlobsRootPath))
        {
            var name = Path.GetFileName(dir);
            if (name.StartsWith('.') || name.StartsWith('_')) continue;
            if (prefix is not null && !name.StartsWith(prefix, StringComparison.Ordinal)) continue;
            items.Add(BlobsModelFactory.BlobContainerItem(name, BlobsModelFactory.BlobContainerProperties(lastModified: Directory.GetLastWriteTimeUtc(dir), eTag: new ETag("\"0x0\""))));
        }
        return new StaticPageable<BlobContainerItem>(items);
    }

    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => GetBlobContainers(traits, default, prefix, cancellationToken);

    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, states, prefix, cancellationToken));

    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, default, prefix, cancellationToken));
}
