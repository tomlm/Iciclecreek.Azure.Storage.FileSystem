using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

public class FileBlobContainerClient : BlobContainerClient
{
    internal readonly BlobStore _store;
    internal readonly FileStorageAccount _account;

    public FileBlobContainerClient(string connectionString, string containerName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new BlobStore(_account, containerName);
    }

    public FileBlobContainerClient(Uri containerUri, FileStorageProvider provider) : base()
    {
        var (acctName, container, _) = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ParseBlobUri(containerUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new BlobStore(_account, container);
    }

    internal FileBlobContainerClient(FileStorageAccount account, string containerName) : base()
    {
        _account = account;
        _store = new BlobStore(account, containerName);
    }

    public static FileBlobContainerClient FromAccount(FileStorageAccount account, string containerName)
        => new(account, containerName);

    public override string Name => _store.ContainerName;
    public override string AccountName => _account.Name;
    public override Uri Uri => new($"{_account.BlobServiceUri}{_store.ContainerName}");

    // ---- Create / Delete / Exists ----

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        if (_store.ContainerExists())
            throw new RequestFailedException(409, "Container already exists.", "ContainerAlreadyExists", null);
        _store.CreateContainer();
        var info = BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobContainerInfo> Create(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => Create(publicAccessType, metadata, cancellationToken);

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    {
        if (_store.ContainerExists())
            return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Ok());
        _store.CreateContainer();
        return Response.FromValue(BlobsModelFactory.BlobContainerInfo(new ETag("\"0x0\""), DateTimeOffset.UtcNow), StubResponse.Created());
    }

    public override Response<BlobContainerInfo> CreateIfNotExists(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
        => CreateIfNotExists(publicAccessType, metadata, cancellationToken);

    public override Response Delete(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.DeleteContainer())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        return StubResponse.Accepted();
    }

    public override Response<bool> DeleteIfExists(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => Response.FromValue(_store.DeleteContainer(), StubResponse.Ok());

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.ContainerExists(), StubResponse.Ok());

    // ---- Async Create / Delete / Exists ----

    public override async Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(publicAccessType, metadata, cancellationToken); }

    public override async Task<Response<BlobContainerInfo>> CreateAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(publicAccessType, metadata, encryptionScopeOptions, cancellationToken); }

    public override async Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType = default, IDictionary<string, string>? metadata = default, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(publicAccessType, metadata, cancellationToken); }

    public override async Task<Response<BlobContainerInfo>> CreateIfNotExistsAsync(PublicAccessType publicAccessType, IDictionary<string, string>? metadata, BlobContainerEncryptionScopeOptions encryptionScopeOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(publicAccessType, metadata, encryptionScopeOptions, cancellationToken); }

    public override async Task<Response> DeleteAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(conditions, cancellationToken); }

    public override async Task<Response<bool>> DeleteIfExistsAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(conditions, cancellationToken); }

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    // ---- GetBlobClient ----

    public override BlobClient GetBlobClient(string blobName) => new FileBlobClient(_account, _store.ContainerName, blobName);

    public FileBlockBlobClient GetFileBlockBlobClient(string blobName) => new FileBlockBlobClient(_account, _store.ContainerName, blobName);

    public FileAppendBlobClient GetFileAppendBlobClient(string blobName) => new FileAppendBlobClient(_account, _store.ContainerName, blobName);

    // ---- UploadBlob ----

    public override Response<BlobContentInfo> UploadBlob(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    public override Response<BlobContentInfo> UploadBlob(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return client.Upload(content, false, cancellationToken);
    }

    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, Stream content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> UploadBlobAsync(string blobName, BinaryData content, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return await client.UploadAsync(content, false, cancellationToken).ConfigureAwait(false);
    }

    // ---- DeleteBlob ----

    public override Response DeleteBlob(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return client.Delete(snapshotsOption, conditions, cancellationToken);
    }

    public override Response<bool> DeleteBlobIfExists(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return client.DeleteIfExists(snapshotsOption, conditions, cancellationToken);
    }

    public override async Task<Response> DeleteBlobAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return await client.DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<bool>> DeleteBlobIfExistsAsync(string blobName, DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var client = new FileBlobClient(_account, _store.ContainerName, blobName);
        return await client.DeleteIfExistsAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
    }

    // ---- GetBlobs / GetBlobsByHierarchy ----

    public override Pageable<BlobItem> GetBlobs(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
    {
        var items = new List<BlobItem>();
        foreach (var (blobName, fullPath, fi) in _store.EnumerateBlobs(prefix))
        {
            BlobSidecar? sidecar = null;
            IDictionary<string, string>? metadata = null;
            if ((traits & BlobTraits.Metadata) != 0)
            {
                sidecar = _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();
                metadata = sidecar?.Metadata;
            }
            sidecar ??= _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();

            var props = BlobsModelFactory.BlobItemProperties(
                accessTierInferred: true,
                contentLength: fi.Length,
                contentType: sidecar?.ContentType,
                eTag: sidecar is not null ? new ETag(sidecar.ETag) : default,
                lastModified: sidecar?.LastModifiedUtc ?? fi.LastWriteTimeUtc,
                blobType: sidecar?.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
                createdOn: sidecar?.CreatedOnUtc ?? fi.CreationTimeUtc);

            items.Add(BlobsModelFactory.BlobItem(
                name: blobName,
                deleted: false,
                properties: props,
                metadata: metadata));
        }
        return new StaticPageable<BlobItem>(items);
    }

    public override AsyncPageable<BlobItem> GetBlobsAsync(BlobTraits traits = default, BlobStates states = default, string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobItem>(GetBlobs(traits, states, prefix, cancellationToken));

    public override Pageable<BlobHierarchyItem> GetBlobsByHierarchy(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
    {
        delimiter ??= "/";
        var items = new List<BlobHierarchyItem>();
        var prefixes = new HashSet<string>(StringComparer.Ordinal);

        foreach (var (blobName, fullPath, fi) in _store.EnumerateBlobs(prefix))
        {
            var relative = prefix is not null ? blobName[prefix.Length..] : blobName;
            var delimIndex = relative.IndexOf(delimiter, StringComparison.Ordinal);
            if (delimIndex >= 0)
            {
                var pfx = (prefix ?? "") + relative[..(delimIndex + delimiter.Length)];
                if (prefixes.Add(pfx))
                    items.Add(BlobsModelFactory.BlobHierarchyItem(pfx, null!));
            }
            else
            {
                var sidecar = _store.ReadSidecarAsync(blobName).GetAwaiter().GetResult();
                var props = BlobsModelFactory.BlobItemProperties(
                    accessTierInferred: true,
                    contentLength: fi.Length,
                    contentType: sidecar?.ContentType,
                    eTag: sidecar is not null ? new ETag(sidecar.ETag) : default,
                    lastModified: sidecar?.LastModifiedUtc ?? fi.LastWriteTimeUtc,
                    blobType: sidecar?.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block);

                var blob = BlobsModelFactory.BlobItem(name: blobName, deleted: false, properties: props);
                items.Add(BlobsModelFactory.BlobHierarchyItem(null!, blob));
            }
        }
        return new StaticPageable<BlobHierarchyItem>(items);
    }

    public override AsyncPageable<BlobHierarchyItem> GetBlobsByHierarchyAsync(BlobTraits traits = default, BlobStates states = default, string? delimiter = "/", string? prefix = default, CancellationToken cancellationToken = default)
        => new StaticAsyncPageable<BlobHierarchyItem>(GetBlobsByHierarchy(traits, states, delimiter, prefix, cancellationToken));

    // ---- Container Properties ----

    public override Response<BlobContainerProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.ContainerExists())
            throw new RequestFailedException(404, "Container not found.", "ContainerNotFound", null);
        var props = BlobsModelFactory.BlobContainerProperties(lastModified: DateTimeOffset.UtcNow, eTag: new ETag("\"0x0\""));
        return Response.FromValue(props, StubResponse.Ok());
    }

    public override async Task<Response<BlobContainerProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(conditions, cancellationToken); }
}

// ---- Pageable helpers ----

internal sealed class StaticPageable<T> : Pageable<T> where T : notnull
{
    private readonly IReadOnlyList<T> _items;
    public StaticPageable(IReadOnlyList<T> items) => _items = items;
    public override IEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        yield return Page<T>.FromValues(_items.ToList(), null, StubResponse.Ok());
    }
}

internal sealed class StaticAsyncPageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly Pageable<T> _inner;
    public StaticAsyncPageable(Pageable<T> inner) => _inner = inner;
    public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        foreach (var page in _inner.AsPages(continuationToken, pageSizeHint))
            yield return page;
    }
}

internal sealed class AsyncEnumerablePageable<T> : AsyncPageable<T> where T : notnull
{
    private readonly IAsyncEnumerable<global::Azure.Data.Tables.TableEntity> _source;
    private readonly Func<global::Azure.Data.Tables.TableEntity, T?> _transform;

    public AsyncEnumerablePageable(IAsyncEnumerable<global::Azure.Data.Tables.TableEntity> source, Func<global::Azure.Data.Tables.TableEntity, T?> transform)
    {
        _source = source;
        _transform = transform;
    }

    public override async IAsyncEnumerable<Page<T>> AsPages(string? continuationToken = default, int? pageSizeHint = default)
    {
        var items = new List<T>();
        await foreach (var entity in _source.ConfigureAwait(false))
        {
            var result = _transform(entity);
            if (result is not null) items.Add(result);
        }
        yield return Page<T>.FromValues(items, null, StubResponse.Ok());
    }
}
