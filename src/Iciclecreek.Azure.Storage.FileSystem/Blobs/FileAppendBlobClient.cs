using System.Security.Cryptography;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

public class FileAppendBlobClient : AppendBlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileStorageAccount _account;

    public FileAppendBlobClient(string connectionString, string containerName, string blobName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new BlobStore(_account, containerName);
        _blobName = blobName;
    }

    public FileAppendBlobClient(Uri blobUri, FileStorageProvider provider) : base()
    {
        var (acctName, container, blob) = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ParseBlobUri(blobUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new BlobStore(_account, container);
        _blobName = blob ?? throw new ArgumentException("URI must include a blob name.", nameof(blobUri));
    }

    internal FileAppendBlobClient(FileStorageAccount account, string containerName, string blobName) : base()
    {
        _account = account;
        _store = new BlobStore(account, containerName);
        _blobName = blobName;
    }

    public static FileAppendBlobClient FromAccount(FileStorageAccount account, string containerName, string blobName)
        => new(account, containerName, blobName);

    public override string Name => _blobName;
    public override string BlobContainerName => _store.ContainerName;
    public override string AccountName => _account.Name;
    public override Uri Uri => new($"{_account.BlobServiceUri}{_store.ContainerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ---- Create (async = primary) ----

    public override async Task<Response<BlobContentInfo>> CreateAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        var conditions = options?.Conditions;
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        if (conditions is not null)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: false, nameof(Create));

        var path = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        await File.WriteAllBytesAsync(path, [], cancellationToken).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(0, now, ReadOnlySpan<byte>.Empty);
        var newSidecar = new BlobSidecar
        {
            BlobType = BlobKind.Append,
            Length = 0,
            ETag = etag.ToString()!,
            CreatedOnUtc = now,
            LastModifiedUtc = now,
            ContentType = options?.HttpHeaders?.ContentType ?? "application/octet-stream",
            ContentEncoding = options?.HttpHeaders?.ContentEncoding,
        };
        if (options?.Metadata is { } meta)
            newSidecar.Metadata = new Dictionary<string, string>(meta, StringComparer.Ordinal);
        await _store.WriteSidecarAsync(_blobName, newSidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobContentInfo(etag, now, null, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override async Task<Response<BlobContentInfo>> CreateAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await CreateAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        if (_store.Exists(_blobName))
            return Response.FromValue<BlobContentInfo>(null!, StubResponse.Ok());
        return await CreateAsync(options, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
        => await CreateIfNotExistsAsync(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata }, cancellationToken).ConfigureAwait(false);

    public override Response<BlobContentInfo> Create(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
        => CreateAsync(options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Create(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => CreateAsync(httpHeaders, metadata, conditions, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> CreateIfNotExists(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
        => CreateIfNotExistsAsync(options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> CreateIfNotExists(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
        => CreateIfNotExistsAsync(httpHeaders, metadata, cancellationToken).GetAwaiter().GetResult();

    // ---- AppendBlock (async = primary) ----

    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Append blob not found. Call Create first.", "BlobNotFound", null);

        if (options?.Conditions is { } conditions)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: true, nameof(AppendBlock));

        var blobPath = _store.BlobPath(_blobName);
        long newLength;

        await using (var fs = new FileStream(blobPath, FileMode.Append, FileAccess.Write, FileShare.None, 81920, useAsync: true))
        {
            // Check append position if specified.
            if (options?.Conditions?.IfAppendPositionEqual is { } expectedPos && fs.Position != expectedPos)
                throw new RequestFailedException(412, $"AppendPositionOffset mismatch. Expected {expectedPos}, actual {fs.Position}.", "AppendPositionConditionNotMet", null);

            await content.CopyToAsync(fs, cancellationToken).ConfigureAwait(false);
            await fs.FlushAsync(cancellationToken).ConfigureAwait(false);
            newLength = fs.Length;
        }

        // Check max size.
        if (options?.Conditions?.IfMaxSizeLessThanOrEqual is { } maxSize && newLength > maxSize)
            throw new RequestFailedException(412, $"Blob size {newLength} exceeds MaxSize {maxSize}.", "MaxBlobSizeConditionNotMet", null);

        var md5 = MD5.HashData(await File.ReadAllBytesAsync(blobPath, cancellationToken).ConfigureAwait(false));
        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(newLength, now, md5);

        sidecar.Length = newLength;
        sidecar.ContentHashBase64 = Convert.ToBase64String(md5);
        sidecar.ETag = etag.ToString()!;
        sidecar.LastModifiedUtc = now;
        await _store.WriteSidecarAsync(_blobName, sidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobAppendInfo(etag, now, md5, null, newLength.ToString(), 0, false, null!, null!);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => await AppendBlockAsync(content, new AppendBlobAppendBlockOptions { Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    public override Response<BlobAppendInfo> AppendBlock(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
        => AppendBlockAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobAppendInfo> AppendBlock(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => AppendBlockAsync(content, transactionalContentHash, conditions, progressHandler, cancellationToken).GetAwaiter().GetResult();

    // ---- Shared blob operations (async = primary) ----

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.Exists(_blobName), StubResponse.Ok());

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => ExistsAsync(cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(Delete));
        _store.Delete(_blobName);
        return StubResponse.Accepted();
    }

    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => DeleteAsync(snapshotsOption, conditions, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_account, _store.ContainerName, _blobName);
        return await blobClient.GetPropertiesAsync(conditions, cancellationToken).ConfigureAwait(false);
    }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetPropertiesAsync(conditions, cancellationToken).GetAwaiter().GetResult();

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_account, _store.ContainerName, _blobName);
        return await blobClient.DownloadContentAsync(cancellationToken).ConfigureAwait(false);
    }

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default)
        => DownloadContentAsync(cancellationToken).GetAwaiter().GetResult();
}
