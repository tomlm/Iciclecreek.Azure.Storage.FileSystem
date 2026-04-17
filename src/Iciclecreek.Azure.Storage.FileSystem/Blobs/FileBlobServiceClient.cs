using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Storage.Blobs.BlobServiceClient"/>. Manages blob containers as subdirectories.</summary>
public class FileBlobServiceClient : BlobServiceClient
{
    internal readonly FileStorageAccount _account;

    /// <summary>Initializes a new <see cref="FileBlobServiceClient"/> from a connection string and provider.</summary>
    /// <param name="connectionString">The storage connection string.</param>
    /// <param name="provider">The <see cref="FileStorageProvider"/> that resolves accounts.</param>
    public FileBlobServiceClient(string connectionString, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
    }

    /// <summary>Initializes a new <see cref="FileBlobServiceClient"/> by extracting the account name from a service URI.</summary>
    /// <param name="serviceUri">The blob service URI.</param>
    /// <param name="provider">The <see cref="FileStorageProvider"/> that resolves accounts.</param>
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

    /// <summary>Creates a new <see cref="FileBlobServiceClient"/> from an existing <see cref="FileStorageAccount"/>.</summary>
    /// <param name="account">The filesystem-backed storage account.</param>
    public static FileBlobServiceClient FromAccount(FileStorageAccount account) => new(account);

    /// <inheritdoc/>
    public override string AccountName => _account.Name;
    /// <inheritdoc/>
    public override Uri Uri => _account.BlobServiceUri;

    // ---- GetBlobContainerClient ----

    /// <inheritdoc/>
    public override BlobContainerClient GetBlobContainerClient(string blobContainerName)
        => new FileBlobContainerClient(_account, blobContainerName);

    // ---- CreateBlobContainer ----

    /// <inheritdoc/>
    public override Response<BlobContainerClient> CreateBlobContainer(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobContainerClient(_account, blobContainerName);
        client.Create(publicAccessType, metadata, cancellationToken);
        return Response.FromValue<BlobContainerClient>(client, StubResponse.Created());
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContainerClient>> CreateBlobContainerAsync(string blobContainerName, PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateBlobContainer(blobContainerName, publicAccessType, metadata, cancellationToken); }

    // ---- DeleteBlobContainer ----

    /// <inheritdoc/>
    public override Response DeleteBlobContainer(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobContainerClient(_account, blobContainerName);
        return client.Delete(conditions, cancellationToken);
    }

    /// <inheritdoc/>
    public override async Task<Response> DeleteBlobContainerAsync(string blobContainerName, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteBlobContainer(blobContainerName, conditions, cancellationToken); }

    // ---- GetBlobContainers ----

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    public override Pageable<BlobContainerItem> GetBlobContainers(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => GetBlobContainers(traits, default, prefix, cancellationToken);

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits = default, BlobContainerStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, states, prefix, cancellationToken));

    /// <inheritdoc/>
    public override AsyncPageable<BlobContainerItem> GetBlobContainersAsync(BlobContainerTraits traits, string? prefix, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobContainerItem>(GetBlobContainers(traits, default, prefix, cancellationToken));

    // ---- NotSupported sweep — BlobServiceClient ----
    /// <inheritdoc/>
    public override Response<BlobServiceProperties> GetProperties(CancellationToken ct = default) => NotSupported.Throw<Response<BlobServiceProperties>>();
    /// <inheritdoc/>
    public override Task<Response<BlobServiceProperties>> GetPropertiesAsync(CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobServiceProperties>>>();
    /// <inheritdoc/>
    public override Response SetProperties(BlobServiceProperties properties, CancellationToken ct = default) => NotSupported.Throw<Response>();
    /// <inheritdoc/>
    public override Task<Response> SetPropertiesAsync(BlobServiceProperties properties, CancellationToken ct = default) => NotSupported.Throw<Task<Response>>();
    /// <inheritdoc/>
    public override Response<BlobServiceStatistics> GetStatistics(CancellationToken ct = default) => NotSupported.Throw<Response<BlobServiceStatistics>>();
    /// <inheritdoc/>
    public override Task<Response<BlobServiceStatistics>> GetStatisticsAsync(CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobServiceStatistics>>>();
    /// <inheritdoc/>
    public override Response<UserDelegationKey> GetUserDelegationKey(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken ct = default) => NotSupported.Throw<Response<UserDelegationKey>>();
    /// <inheritdoc/>
    public override Task<Response<UserDelegationKey>> GetUserDelegationKeyAsync(DateTimeOffset? startsOn, DateTimeOffset expiresOn, CancellationToken ct = default) => NotSupported.Throw<Task<Response<UserDelegationKey>>>();
    /// <inheritdoc/>
    public override Response<AccountInfo> GetAccountInfo(CancellationToken ct = default) => NotSupported.Throw<Response<AccountInfo>>();
    /// <inheritdoc/>
    public override Task<Response<AccountInfo>> GetAccountInfoAsync(CancellationToken ct = default) => NotSupported.Throw<Task<Response<AccountInfo>>>();
    /// <inheritdoc/>
    public override Pageable<TaggedBlobItem> FindBlobsByTags(string tagFilterSqlExpression, CancellationToken ct = default) => NotSupported.Throw<Pageable<TaggedBlobItem>>();
    /// <inheritdoc/>
    public override AsyncPageable<TaggedBlobItem> FindBlobsByTagsAsync(string tagFilterSqlExpression, CancellationToken ct = default) => NotSupported.Throw<AsyncPageable<TaggedBlobItem>>();
    /// <inheritdoc/>
    public override Response<BlobContainerClient> UndeleteBlobContainer(string deletedContainerName, string deletedContainerVersion, CancellationToken ct = default) => NotSupported.Throw<Response<BlobContainerClient>>();
    /// <inheritdoc/>
    public override Task<Response<BlobContainerClient>> UndeleteBlobContainerAsync(string deletedContainerName, string deletedContainerVersion, CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobContainerClient>>>();
    /// <inheritdoc/>
    public override Response<BlobContainerClient> UndeleteBlobContainer(string deletedContainerName, string deletedContainerVersion, string destinationContainerName, CancellationToken ct = default) => NotSupported.Throw<Response<BlobContainerClient>>();
    /// <inheritdoc/>
    public override Task<Response<BlobContainerClient>> UndeleteBlobContainerAsync(string deletedContainerName, string deletedContainerVersion, string destinationContainerName, CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobContainerClient>>>();
}
