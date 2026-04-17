using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

public class FileBlobClient : BlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileStorageAccount _account;

    public FileBlobClient(string connectionString, string containerName, string blobName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new BlobStore(_account, containerName);
        _blobName = blobName;
    }

    public FileBlobClient(Uri blobUri, FileStorageProvider provider) : base()
    {
        var (acctName, container, blob) = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ParseBlobUri(blobUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new BlobStore(_account, container);
        _blobName = blob ?? throw new ArgumentException("URI must include a blob name.", nameof(blobUri));
    }

    internal FileBlobClient(FileStorageAccount account, string containerName, string blobName) : base()
    {
        _account = account;
        _store = new BlobStore(account, containerName);
        _blobName = blobName;
    }

    public static FileBlobClient FromAccount(FileStorageAccount account, string containerName, string blobName)
        => new(account, containerName, blobName);

    public override string Name => _blobName;
    public override string BlobContainerName => _store.ContainerName;
    public override string AccountName => _account.Name;
    public override Uri Uri => new($"{_account.BlobServiceUri}{_store.ContainerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ==== Async Upload (primary) ====

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, options, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, null, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), options, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), null, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, options, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, null, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, bool overwrite, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);
    }

    // ==== Sync Upload (delegates) ====

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(string path, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(path, options, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(string path, CancellationToken ct = default) => UploadAsync(path, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(string path, bool overwrite, CancellationToken ct = default) => UploadAsync(path, overwrite, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(content, h, m, c, p, a, t, ct).GetAwaiter().GetResult();
    public override Response<BlobContentInfo> Upload(string path, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(path, h, m, c, p, a, t, ct).GetAwaiter().GetResult();

    // ==== Async Download (primary) ====

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(null, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions options, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions options = default!, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    // ==== Sync Download (delegates) ====

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken ct = default) => DownloadContentAsync(ct).GetAwaiter().GetResult();
    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken ct = default) => DownloadContentAsync(conditions, ct).GetAwaiter().GetResult();
    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions options, CancellationToken ct = default) => DownloadContentAsync(options, ct).GetAwaiter().GetResult();
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions options = default!, CancellationToken ct = default) => DownloadStreamingAsync(options, ct).GetAwaiter().GetResult();

    // ==== Async Exists / Delete / GetProperties / SetMetadata / SetHttpHeaders (primary) ====

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.Exists(_blobName), StubResponse.Ok());

    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(Delete));
        _store.Delete(_blobName);
        return StubResponse.Accepted();
    }

    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.Exists(_blobName))
            return Response.FromValue(false, StubResponse.Ok());
        await DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(true, StubResponse.Ok());
    }

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(GetProperties));

        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;
        var props = BlobsModelFactory.BlobProperties(
            lastModified: sidecar.LastModifiedUtc, createdOn: sidecar.CreatedOnUtc,
            metadata: sidecar.Metadata,
            blobType: sidecar.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
            contentLength: sidecar.Length,
            contentType: sidecar.ContentType ?? "application/octet-stream",
            eTag: new ETag(sidecar.ETag), contentHash: md5,
            contentEncoding: sidecar.ContentEncoding, contentDisposition: sidecar.ContentDisposition,
            contentLanguage: sidecar.ContentLanguage, cacheControl: sidecar.CacheControl);
        return Response.FromValue(props, StubResponse.Ok());
    }

    public override async Task<Response<BlobInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(SetMetadata));

        sidecar.Metadata = new Dictionary<string, string>(metadata, StringComparer.Ordinal);
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        var fi = new FileInfo(_store.BlobPath(_blobName));
        sidecar.ETag = ETagCalculator.Compute(fi.Length, sidecar.LastModifiedUtc, ReadOnlySpan<byte>.Empty).ToString()!;
        await _store.WriteSidecarAsync(_blobName, sidecar, cancellationToken).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc), StubResponse.Ok());
    }

    public override async Task<Response<BlobInfo>> SetHttpHeadersAsync(BlobHttpHeaders httpHeaders, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(SetHttpHeaders));

        if (httpHeaders.ContentType is not null) sidecar.ContentType = httpHeaders.ContentType;
        if (httpHeaders.ContentEncoding is not null) sidecar.ContentEncoding = httpHeaders.ContentEncoding;
        if (httpHeaders.ContentLanguage is not null) sidecar.ContentLanguage = httpHeaders.ContentLanguage;
        if (httpHeaders.ContentDisposition is not null) sidecar.ContentDisposition = httpHeaders.ContentDisposition;
        if (httpHeaders.CacheControl is not null) sidecar.CacheControl = httpHeaders.CacheControl;
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        var fi = new FileInfo(_store.BlobPath(_blobName));
        sidecar.ETag = ETagCalculator.Compute(fi.Length, sidecar.LastModifiedUtc, ReadOnlySpan<byte>.Empty).ToString()!;
        await _store.WriteSidecarAsync(_blobName, sidecar, cancellationToken).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc), StubResponse.Ok());
    }

    // ==== Sync delegates ====

    public override Response<bool> Exists(CancellationToken ct = default) => ExistsAsync(ct).GetAwaiter().GetResult();
    public override Response Delete(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteAsync(s, c, ct).GetAwaiter().GetResult();
    public override Response<bool> DeleteIfExists(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteIfExistsAsync(s, c, ct).GetAwaiter().GetResult();
    public override Response<BlobProperties> GetProperties(BlobRequestConditions c = default!, CancellationToken ct = default) => GetPropertiesAsync(c, ct).GetAwaiter().GetResult();
    public override Response<BlobInfo> SetMetadata(IDictionary<string, string> m, BlobRequestConditions c = default!, CancellationToken ct = default) => SetMetadataAsync(m, c, ct).GetAwaiter().GetResult();
    public override Response<BlobInfo> SetHttpHeaders(BlobHttpHeaders h, BlobRequestConditions c = default!, CancellationToken ct = default) => SetHttpHeadersAsync(h, c, ct).GetAwaiter().GetResult();

    // ==== Core async helpers ====

    internal async Task<Response<BlobContentInfo>> UploadCoreAsync(Stream content, BlobUploadOptions? options, CancellationToken ct = default)
    {
        var conditions = options?.Conditions;
        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: false, nameof(Upload));

        var (length, md5) = await _store.WriteContentFromStreamAsync(_blobName, content, ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(length, now, md5);

        var newSidecar = sidecar ?? new BlobSidecar();
        newSidecar.BlobType = BlobKind.Block;
        newSidecar.Length = length;
        newSidecar.ContentHashBase64 = Convert.ToBase64String(md5);
        newSidecar.ETag = etag.ToString()!;
        newSidecar.LastModifiedUtc = now;
        if (sidecar is null) newSidecar.CreatedOnUtc = now;
        if (options?.HttpHeaders is { } headers)
        {
            newSidecar.ContentType = headers.ContentType;
            newSidecar.ContentEncoding = headers.ContentEncoding;
            newSidecar.ContentLanguage = headers.ContentLanguage;
            newSidecar.ContentDisposition = headers.ContentDisposition;
            newSidecar.CacheControl = headers.CacheControl;
        }
        if (options?.Metadata is { } meta)
            newSidecar.Metadata = new Dictionary<string, string>(meta, StringComparer.Ordinal);
        newSidecar.CommittedBlocks.Clear();
        await _store.WriteSidecarAsync(_blobName, newSidecar, ct).ConfigureAwait(false);

        return Response.FromValue(BlobsModelFactory.BlobContentInfo(etag, now, md5, null!, null!, null!, 0), StubResponse.Created());
    }

    private async Task<Response<BlobDownloadResult>> DownloadContentCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(DownloadContent));

        var bytes = await File.ReadAllBytesAsync(_store.BlobPath(_blobName), ct).ConfigureAwait(false);
        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;
        var details = BuildDownloadDetails(sidecar, md5);
        return Response.FromValue(BlobsModelFactory.BlobDownloadResult(new BinaryData(bytes), details), StubResponse.Ok());
    }

    private async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingCoreAsync(BlobRequestConditions? conditions, CancellationToken ct)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, ct).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(DownloadStreaming));

        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;
        var stream = new FileStream(_store.BlobPath(_blobName), FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        var details = BuildDownloadDetails(sidecar, md5);
        return Response.FromValue(BlobsModelFactory.BlobDownloadStreamingResult(stream, details), StubResponse.Ok());
    }

    private static BlobDownloadDetails BuildDownloadDetails(BlobSidecar sidecar, byte[]? md5) =>
        BlobsModelFactory.BlobDownloadDetails(
            blobType: sidecar.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
            contentLength: sidecar.Length,
            contentType: sidecar.ContentType ?? "application/octet-stream",
            contentHash: md5, lastModified: sidecar.LastModifiedUtc, metadata: sidecar.Metadata,
            contentEncoding: sidecar.ContentEncoding, contentDisposition: sidecar.ContentDisposition,
            contentLanguage: sidecar.ContentLanguage, cacheControl: sidecar.CacheControl,
            createdOn: sidecar.CreatedOnUtc, eTag: new ETag(sidecar.ETag));
}
