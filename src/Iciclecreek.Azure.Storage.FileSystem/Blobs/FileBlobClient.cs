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

    // ---- Upload ----

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadCore(content, options);

    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken cancellationToken = default)
        => UploadCore(content, null);

    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadCore(content, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } });

    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadCore(content.ToStream(), options);

    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken cancellationToken = default)
        => UploadCore(content.ToStream(), null);

    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => UploadCore(content.ToStream(), overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } });

    public override Response<BlobContentInfo> Upload(string path, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        using var fs = File.OpenRead(path);
        return UploadCore(fs, options);
    }

    public override Response<BlobContentInfo> Upload(string path, CancellationToken cancellationToken = default)
    {
        using var fs = File.OpenRead(path);
        return UploadCore(fs, null);
    }

    public override Response<BlobContentInfo> Upload(string path, bool overwrite, CancellationToken cancellationToken = default)
    {
        using var fs = File.OpenRead(path);
        return UploadCore(fs, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } });
    }

    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
        => UploadCore(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions });

    public override Response<BlobContentInfo> Upload(string path, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    {
        using var fs = File.OpenRead(path);
        return UploadCore(fs, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions });
    }

    // ---- Async Upload ----

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, options, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, overwrite, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, options, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, overwrite, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobUploadOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(path, options, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(path, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, bool overwrite, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(path, overwrite, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(content, httpHeaders, metadata, conditions, progressHandler, accessTier, transferOptions, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Upload(path, httpHeaders, metadata, conditions, progressHandler, accessTier, transferOptions, cancellationToken); }

    // ---- Download ----

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default)
        => DownloadContentCore(null);

    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => DownloadContentCore(conditions);

    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions options, CancellationToken cancellationToken = default)
        => DownloadContentCore(options?.Conditions);

    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions options = default!, CancellationToken cancellationToken = default)
        => DownloadStreamingCore(options?.Conditions);

    // ---- Async Download ----

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DownloadContent(cancellationToken); }

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DownloadContent(conditions, cancellationToken); }

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DownloadContent(options, cancellationToken); }

    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions options = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DownloadStreaming(options, cancellationToken); }

    // ---- Exists / Delete / GetProperties / SetMetadata / SetHttpHeaders ----

    public override Response<bool> Exists(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.Exists(_blobName), StubResponse.Ok());

    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return Exists(cancellationToken); }

    public override Response Delete(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = _store.ReadSidecar(_blobName);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(Delete));
        _store.Delete(_blobName);
        return StubResponse.Accepted();
    }

    public override Response<bool> DeleteIfExists(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.Exists(_blobName))
            return Response.FromValue(false, StubResponse.Ok());
        Delete(snapshotsOption, conditions, cancellationToken);
        return Response.FromValue(true, StubResponse.Ok());
    }

    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(snapshotsOption, conditions, cancellationToken); }

    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return DeleteIfExists(snapshotsOption, conditions, cancellationToken); }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = _store.ReadSidecar(_blobName)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(GetProperties));

        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;
        var props = BlobsModelFactory.BlobProperties(
            lastModified: sidecar.LastModifiedUtc,
            createdOn: sidecar.CreatedOnUtc,
            metadata: sidecar.Metadata,
            blobType: sidecar.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
            contentLength: sidecar.Length,
            contentType: sidecar.ContentType ?? "application/octet-stream",
            eTag: new ETag(sidecar.ETag),
            contentHash: md5,
            contentEncoding: sidecar.ContentEncoding,
            contentDisposition: sidecar.ContentDisposition,
            contentLanguage: sidecar.ContentLanguage,
            cacheControl: sidecar.CacheControl);

        return Response.FromValue(props, StubResponse.Ok());
    }

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(conditions, cancellationToken); }

    public override Response<BlobInfo> SetMetadata(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = _store.ReadSidecar(_blobName)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(SetMetadata));

        sidecar.Metadata = new Dictionary<string, string>(metadata, StringComparer.Ordinal);
        sidecar.LastModifiedUtc = DateTimeOffset.UtcNow;
        var fi = new FileInfo(_store.BlobPath(_blobName));
        sidecar.ETag = ETagCalculator.Compute(fi.Length, sidecar.LastModifiedUtc, ReadOnlySpan<byte>.Empty).ToString()!;
        _store.WriteSidecar(_blobName, sidecar);

        var info = BlobsModelFactory.BlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override async Task<Response<BlobInfo>> SetMetadataAsync(IDictionary<string, string> metadata, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetMetadata(metadata, conditions, cancellationToken); }

    public override Response<BlobInfo> SetHttpHeaders(BlobHttpHeaders httpHeaders, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = _store.ReadSidecar(_blobName)
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
        _store.WriteSidecar(_blobName, sidecar);

        var info = BlobsModelFactory.BlobInfo(new ETag(sidecar.ETag), sidecar.LastModifiedUtc);
        return Response.FromValue(info, StubResponse.Ok());
    }

    public override async Task<Response<BlobInfo>> SetHttpHeadersAsync(BlobHttpHeaders httpHeaders, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return SetHttpHeaders(httpHeaders, conditions, cancellationToken); }

    // ---- Core helpers ----

    internal Response<BlobContentInfo> UploadCore(Stream content, BlobUploadOptions? options)
    {
        var conditions = options?.Conditions;
        var sidecar = _store.ReadSidecar(_blobName);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: false, nameof(Upload));

        var (length, md5) = _store.WriteContentFromStream(_blobName, content);
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
        _store.WriteSidecar(_blobName, newSidecar);

        var info = BlobsModelFactory.BlobContentInfo(etag, now, md5, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    private Response<BlobDownloadResult> DownloadContentCore(BlobRequestConditions? conditions)
    {
        var sidecar = _store.ReadSidecar(_blobName)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(DownloadContent));

        var bytes = File.ReadAllBytes(_store.BlobPath(_blobName));
        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;

        var details = BlobsModelFactory.BlobDownloadDetails(
            blobType: sidecar.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
            contentLength: sidecar.Length,
            contentType: sidecar.ContentType ?? "application/octet-stream",
            contentHash: md5,
            lastModified: sidecar.LastModifiedUtc,
            metadata: sidecar.Metadata,
            contentEncoding: sidecar.ContentEncoding,
            contentDisposition: sidecar.ContentDisposition,
            contentLanguage: sidecar.ContentLanguage,
            cacheControl: sidecar.CacheControl,
            createdOn: sidecar.CreatedOnUtc,
            eTag: new ETag(sidecar.ETag));

        var result = BlobsModelFactory.BlobDownloadResult(
            content: new BinaryData(bytes),
            details: details);

        return Response.FromValue(result, StubResponse.Ok());
    }

    private Response<BlobDownloadStreamingResult> DownloadStreamingCore(BlobRequestConditions? conditions)
    {
        var sidecar = _store.ReadSidecar(_blobName)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(DownloadStreaming));

        var md5 = sidecar.ContentHashBase64 is not null ? Convert.FromBase64String(sidecar.ContentHashBase64) : null;
        var stream = File.OpenRead(_store.BlobPath(_blobName));

        var details = BlobsModelFactory.BlobDownloadDetails(
            blobType: sidecar.BlobType == BlobKind.Append ? BlobType.Append : BlobType.Block,
            contentLength: sidecar.Length,
            contentType: sidecar.ContentType ?? "application/octet-stream",
            contentHash: md5,
            lastModified: sidecar.LastModifiedUtc,
            metadata: sidecar.Metadata,
            contentEncoding: sidecar.ContentEncoding,
            contentDisposition: sidecar.ContentDisposition,
            contentLanguage: sidecar.ContentLanguage,
            cacheControl: sidecar.CacheControl,
            createdOn: sidecar.CreatedOnUtc,
            eTag: new ETag(sidecar.ETag));

        var result = BlobsModelFactory.BlobDownloadStreamingResult(
            content: stream,
            details: details);

        return Response.FromValue(result, StubResponse.Ok());
    }
}
