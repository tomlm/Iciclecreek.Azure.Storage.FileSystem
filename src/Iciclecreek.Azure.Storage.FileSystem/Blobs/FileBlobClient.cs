using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

/// <summary>Filesystem-backed drop-in replacement for <see cref="Azure.Storage.Blobs.BlobClient"/>. Stores blob data as files on disk with JSON sidecar metadata.</summary>
public class FileBlobClient : BlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileStorageAccount _account;

    /// <summary>Initializes a new <see cref="FileBlobClient"/> from a connection string, container name, blob name, and provider.</summary>
    /// <param name="connectionString">The storage connection string.</param>
    /// <param name="containerName">The name of the blob container.</param>
    /// <param name="blobName">The name of the blob.</param>
    /// <param name="provider">The <see cref="FileStorageProvider"/> that resolves accounts.</param>
    public FileBlobClient(string connectionString, string containerName, string blobName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new BlobStore(_account, containerName);
        _blobName = blobName;
    }

    /// <summary>Initializes a new <see cref="FileBlobClient"/> by parsing a blob URI against the given provider.</summary>
    /// <param name="blobUri">The blob URI to parse.</param>
    /// <param name="provider">The <see cref="FileStorageProvider"/> that resolves accounts.</param>
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

    /// <summary>Creates a new <see cref="FileBlobClient"/> from an existing <see cref="FileStorageAccount"/>.</summary>
    /// <param name="account">The filesystem-backed storage account.</param>
    /// <param name="containerName">The name of the blob container.</param>
    /// <param name="blobName">The name of the blob.</param>
    public static FileBlobClient FromAccount(FileStorageAccount account, string containerName, string blobName)
        => new(account, containerName, blobName);

    /// <inheritdoc/>
    public override string Name => _blobName;
    /// <inheritdoc/>
    public override string BlobContainerName => _store.ContainerName;
    /// <inheritdoc/>
    public override string AccountName => _account.Name;
    /// <inheritdoc/>
    public override Uri Uri => new($"{_account.BlobServiceUri}{_store.ContainerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ==== Async Upload (primary) ====

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), options, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(BinaryData content, bool overwrite, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content.ToStream(), overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, options, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, null, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, bool overwrite, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, overwrite ? null : new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = ETag.All } }, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
        => await UploadCoreAsync(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobContentInfo>> UploadAsync(string path, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, IProgress<long>? progressHandler, AccessTier? accessTier, StorageTransferOptions transferOptions, CancellationToken cancellationToken = default)
    {
        await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
        return await UploadCoreAsync(fs, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);
    }

    // ==== Sync Upload (delegates) ====

    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(content, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, CancellationToken ct = default) => UploadAsync(content, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(BinaryData content, bool overwrite, CancellationToken ct = default) => UploadAsync(content, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, BlobUploadOptions options, CancellationToken ct = default) => UploadAsync(path, options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, CancellationToken ct = default) => UploadAsync(path, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, bool overwrite, CancellationToken ct = default) => UploadAsync(path, overwrite, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(content, h, m, c, p, a, t, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobContentInfo> Upload(string path, BlobHttpHeaders h, IDictionary<string, string> m, BlobRequestConditions c, IProgress<long>? p, AccessTier? a, StorageTransferOptions t, CancellationToken ct = default) => UploadAsync(path, h, m, c, p, a, t, ct).GetAwaiter().GetResult();

    // ==== Async Download (primary) ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(null, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobDownloadOptions options, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(BlobDownloadOptions options = default!, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(options?.Conditions, cancellationToken).ConfigureAwait(false);

    // ==== Sync Download (delegates) ====

    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(CancellationToken ct = default) => DownloadContentAsync(ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, CancellationToken ct = default) => DownloadContentAsync(conditions, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobDownloadOptions options, CancellationToken ct = default) => DownloadContentAsync(options, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(BlobDownloadOptions options = default!, CancellationToken ct = default) => DownloadStreamingAsync(options, ct).GetAwaiter().GetResult();

    // ==== Async Exists / Delete / GetProperties / SetMetadata / SetHttpHeaders (primary) ====

    /// <inheritdoc/>
    public override async Task<Response<bool>> ExistsAsync(CancellationToken cancellationToken = default)
        => Response.FromValue(_store.Exists(_blobName), StubResponse.Ok());

    /// <inheritdoc/>
    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: true, nameof(Delete));
        _store.Delete(_blobName);
        return StubResponse.Accepted();
    }

    /// <inheritdoc/>
    public override async Task<Response<bool>> DeleteIfExistsAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        if (!_store.Exists(_blobName))
            return Response.FromValue(false, StubResponse.Ok());
        await DeleteAsync(snapshotsOption, conditions, cancellationToken).ConfigureAwait(false);
        return Response.FromValue(true, StubResponse.Ok());
    }

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

    /// <inheritdoc/>
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

    /// <inheritdoc/>
    public override Response<bool> Exists(CancellationToken ct = default) => ExistsAsync(ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response Delete(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteAsync(s, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<bool> DeleteIfExists(DeleteSnapshotsOption s = default, BlobRequestConditions c = default!, CancellationToken ct = default) => DeleteIfExistsAsync(s, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobProperties> GetProperties(BlobRequestConditions c = default!, CancellationToken ct = default) => GetPropertiesAsync(c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response<BlobInfo> SetMetadata(IDictionary<string, string> m, BlobRequestConditions c = default!, CancellationToken ct = default) => SetMetadataAsync(m, c, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
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

    // ==== Item 2: GenerateSasUri — returns a synthetic URI ====

    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasBuilder builder) => Uri;
    /// <inheritdoc/>
    public override Uri GenerateSasUri(global::Azure.Storage.Sas.BlobSasPermissions permissions, DateTimeOffset expiresOn) => Uri;

    // ==== Item 3: OpenRead / OpenWrite ====

    /// <inheritdoc/>
    public override async Task<Stream> OpenReadAsync(long position = 0, int? bufferSize = null, BlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        var fs = new FileStream(_store.BlobPath(_blobName), FileMode.Open, FileAccess.Read, FileShare.Read, bufferSize ?? 4096, useAsync: true);
        if (position > 0) fs.Seek(position, SeekOrigin.Begin);
        return fs;
    }

    /// <inheritdoc/>
    public override Stream OpenRead(long position = 0, int? bufferSize = null, BlobRequestConditions conditions = null!, CancellationToken cancellationToken = default)
        => OpenReadAsync(position, bufferSize, conditions, cancellationToken).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public override async Task<Stream> OpenReadAsync(BlobOpenReadOptions options, CancellationToken cancellationToken = default)
        => await OpenReadAsync(options?.Position ?? 0, options?.BufferSize, options?.Conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Stream OpenRead(BlobOpenReadOptions options, CancellationToken cancellationToken = default)
        => OpenReadAsync(options, cancellationToken).GetAwaiter().GetResult();

    // ==== OpenWrite ====

    /// <inheritdoc/>
    public override async Task<Stream> OpenWriteAsync(bool overwrite, BlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
    {
        if (!overwrite)
        {
            var existing = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
            if (existing is not null)
                throw new RequestFailedException(409, "Blob already exists and overwrite is false.", "BlobAlreadyExists", null);
        }
        var blobPath = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(blobPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        var tmpPath = blobPath + ".openwrite.tmp";
        var inner = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
        return new CommitOnCloseStream(inner, tmpPath, _blobName, _store, options?.HttpHeaders, options?.Metadata);
    }

    /// <inheritdoc/>
    public override Stream OpenWrite(bool overwrite, BlobOpenWriteOptions? options = null, CancellationToken cancellationToken = default)
        => OpenWriteAsync(overwrite, options, cancellationToken).GetAwaiter().GetResult();

    // ==== DownloadStreaming additional overloads ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken cancellationToken = default)
        => DownloadStreamingAsync(range, conditions, rangeGetContentHash, cancellationToken).GetAwaiter().GetResult();

    // ==== Item 4: DownloadTo ====

    /// <inheritdoc/>
    public override async Task<Response> DownloadToAsync(Stream destination, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false)
            ?? throw new RequestFailedException(404, "Blob not found.", "BlobNotFound", null);
        await using var fs = new FileStream(_store.BlobPath(_blobName), FileMode.Open, FileAccess.Read, FileShare.Read, 81920, useAsync: true);
        await fs.CopyToAsync(destination, cancellationToken).ConfigureAwait(false);
        return StubResponse.Ok();
    }

    /// <inheritdoc/>
    public override async Task<Response> DownloadToAsync(string path, CancellationToken cancellationToken = default)
    {
        await using var dest = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true);
        return await DownloadToAsync(dest, cancellationToken).ConfigureAwait(false);
    }

    /// <inheritdoc/>
    public override Response DownloadTo(Stream destination, CancellationToken ct = default) => DownloadToAsync(destination, ct).GetAwaiter().GetResult();
    /// <inheritdoc/>
    public override Response DownloadTo(string path, CancellationToken ct = default) => DownloadToAsync(path, ct).GetAwaiter().GetResult();

    // ==== Item 4: Download(HttpRange, ...) overloads ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadStreamingResult>> DownloadStreamingAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
        => await DownloadStreamingCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadStreamingResult> DownloadStreaming(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, IProgress<long>? progressHandler = null, CancellationToken cancellationToken = default)
        => DownloadStreamingAsync(range, conditions, rangeGetContentHash, progressHandler, cancellationToken).GetAwaiter().GetResult();

    // ==== Item 5: DownloadContent(BlobRequestConditions, IProgress, HttpRange, ...) overloads ====

    /// <inheritdoc/>
    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(BlobRequestConditions conditions, IProgress<long>? progressHandler, HttpRange range, CancellationToken cancellationToken = default)
        => await DownloadContentCoreAsync(conditions, cancellationToken).ConfigureAwait(false);

    /// <inheritdoc/>
    public override Response<BlobDownloadResult> DownloadContent(BlobRequestConditions conditions, IProgress<long>? progressHandler, HttpRange range, CancellationToken cancellationToken = default)
        => DownloadContentAsync(conditions, progressHandler, range, cancellationToken).GetAwaiter().GetResult();

    // ==== NotSupported sweep — BlobClient ====

    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download() => NotSupported.Throw<Response<BlobDownloadInfo>>();
    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download(CancellationToken ct) => NotSupported.Throw<Response<BlobDownloadInfo>>();
    /// <inheritdoc/>
    public override Response<BlobDownloadInfo> Download(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken ct = default) => NotSupported.Throw<Response<BlobDownloadInfo>>();
    /// <inheritdoc/>
    public override Task<Response<BlobDownloadInfo>> DownloadAsync() => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();
    /// <inheritdoc/>
    public override Task<Response<BlobDownloadInfo>> DownloadAsync(CancellationToken ct) => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();
    /// <inheritdoc/>
    public override Task<Response<BlobDownloadInfo>> DownloadAsync(HttpRange range, BlobRequestConditions conditions = null!, bool rangeGetContentHash = false, CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobDownloadInfo>>>();
    /// <inheritdoc/>
    public new BlobBaseClient WithSnapshot(string snapshot) => NotSupported.Throw<BlobBaseClient>();
    /// <inheritdoc/>
    public new BlobBaseClient WithVersion(string versionId) => NotSupported.Throw<BlobBaseClient>();
    /// <inheritdoc/>
    public new BlobBaseClient WithCustomerProvidedKey(CustomerProvidedKey? key) => NotSupported.Throw<BlobBaseClient>();
    /// <inheritdoc/>
    public new BlobBaseClient WithEncryptionScope(string scope) => NotSupported.Throw<BlobBaseClient>();
    /// <inheritdoc/>
    public override Response SetAccessTier(AccessTier tier, BlobRequestConditions conditions = null!, RehydratePriority? priority = null, CancellationToken ct = default) => NotSupported.Throw<Response>();
    /// <inheritdoc/>
    public override Task<Response> SetAccessTierAsync(AccessTier tier, BlobRequestConditions conditions = null!, RehydratePriority? priority = null, CancellationToken ct = default) => NotSupported.Throw<Task<Response>>();
    /// <inheritdoc/>
    public override Response<BlobSnapshotInfo> CreateSnapshot(IDictionary<string, string>? metadata = null, BlobRequestConditions conditions = null!, CancellationToken ct = default) => NotSupported.Throw<Response<BlobSnapshotInfo>>();
    /// <inheritdoc/>
    public override Task<Response<BlobSnapshotInfo>> CreateSnapshotAsync(IDictionary<string, string>? metadata = null, BlobRequestConditions conditions = null!, CancellationToken ct = default) => NotSupported.Throw<Task<Response<BlobSnapshotInfo>>>();
    /// <inheritdoc/>
    public override CopyFromUriOperation StartCopyFromUri(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default) => NotSupported.Throw<CopyFromUriOperation>();
    /// <inheritdoc/>
    public override Task<CopyFromUriOperation> StartCopyFromUriAsync(Uri source, BlobCopyFromUriOptions options = null!, CancellationToken ct = default) => NotSupported.Throw<Task<CopyFromUriOperation>>();
    /// <inheritdoc/>
    public override Response AbortCopyFromUri(string copyId, BlobRequestConditions conditions = null!, CancellationToken ct = default) => NotSupported.Throw<Response>();
    /// <inheritdoc/>
    public override Task<Response> AbortCopyFromUriAsync(string copyId, BlobRequestConditions conditions = null!, CancellationToken ct = default) => NotSupported.Throw<Task<Response>>();
    /// <inheritdoc/>
    public override Response Undelete(CancellationToken ct = default) => NotSupported.Throw<Response>();
    /// <inheritdoc/>
    public override Task<Response> UndeleteAsync(CancellationToken ct = default) => NotSupported.Throw<Task<Response>>();
}
