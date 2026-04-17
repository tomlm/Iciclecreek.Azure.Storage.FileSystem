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

    // ---- Create ----

    public override Response<BlobContentInfo> Create(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        var conditions = options?.Conditions;
        var sidecar = _store.ReadSidecar(_blobName);
        if (conditions is not null)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: false, nameof(Create));

        var path = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);
        File.WriteAllBytes(path, []);

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
        _store.WriteSidecar(_blobName, newSidecar);

        var info = BlobsModelFactory.BlobContentInfo(etag, now, null, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobContentInfo> Create(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
        => Create(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken);

    public override Response<BlobContentInfo> CreateIfNotExists(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    {
        if (_store.Exists(_blobName))
            return Response.FromValue<BlobContentInfo>(null!, StubResponse.Ok());
        return Create(options, cancellationToken);
    }

    public override Response<BlobContentInfo> CreateIfNotExists(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
        => CreateIfNotExists(new AppendBlobCreateOptions { HttpHeaders = httpHeaders, Metadata = metadata }, cancellationToken);

    public override async Task<Response<BlobContentInfo>> CreateAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(options, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> CreateAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, AppendBlobRequestConditions conditions, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Create(httpHeaders, metadata, conditions, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(AppendBlobCreateOptions options, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(options, cancellationToken); }

    public override async Task<Response<BlobContentInfo>> CreateIfNotExistsAsync(BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, CancellationToken cancellationToken = default)
    { await Task.Yield(); return CreateIfNotExists(httpHeaders, metadata, cancellationToken); }

    // ---- AppendBlock ----

    public override Response<BlobAppendInfo> AppendBlock(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = _store.ReadSidecar(_blobName)
            ?? throw new RequestFailedException(404, "Append blob not found. Call Create first.", "BlobNotFound", null);

        if (options?.Conditions is { } conditions)
            _store.CheckConditions(sidecar, conditions.IfMatch, conditions.IfNoneMatch, mustExist: true, nameof(AppendBlock));

        var blobPath = _store.BlobPath(_blobName);
        using var fs = new FileStream(blobPath, FileMode.Append, FileAccess.Write, FileShare.None);

        // Check append position if specified.
        if (options?.Conditions?.IfAppendPositionEqual is { } expectedPos && fs.Position != expectedPos)
            throw new RequestFailedException(412, $"AppendPositionOffset mismatch. Expected {expectedPos}, actual {fs.Position}.", "AppendPositionConditionNotMet", null);

        content.CopyTo(fs);
        fs.Flush(flushToDisk: true);

        var newLength = fs.Length;
        fs.Close();

        // Check max size.
        if (options?.Conditions?.IfMaxSizeLessThanOrEqual is { } maxSize && newLength > maxSize)
            throw new RequestFailedException(412, $"Blob size {newLength} exceeds MaxSize {maxSize}.", "MaxBlobSizeConditionNotMet", null);

        var md5 = MD5.HashData(File.ReadAllBytes(blobPath));
        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(newLength, now, md5);

        sidecar.Length = newLength;
        sidecar.ContentHashBase64 = Convert.ToBase64String(md5);
        sidecar.ETag = etag.ToString()!;
        sidecar.LastModifiedUtc = now;
        _store.WriteSidecar(_blobName, sidecar);

        var info = BlobsModelFactory.BlobAppendInfo(etag, now, md5, null, newLength.ToString(), 0, false, null!, null!);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override Response<BlobAppendInfo> AppendBlock(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => AppendBlock(content, new AppendBlobAppendBlockOptions { Conditions = conditions }, cancellationToken);

    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, AppendBlobAppendBlockOptions options = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return AppendBlock(content, options, cancellationToken); }

    public override async Task<Response<BlobAppendInfo>> AppendBlockAsync(Stream content, byte[] transactionalContentHash, AppendBlobRequestConditions conditions, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
    { await Task.Yield(); return AppendBlock(content, transactionalContentHash, conditions, progressHandler, cancellationToken); }

    // ---- Shared blob operations ----

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

    public override async Task<Response> DeleteAsync(DeleteSnapshotsOption snapshotsOption = default, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return Delete(snapshotsOption, conditions, cancellationToken); }

    public override Response<BlobProperties> GetProperties(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_account, _store.ContainerName, _blobName);
        return blobClient.GetProperties(conditions, cancellationToken);
    }

    public override async Task<Response<BlobProperties>> GetPropertiesAsync(BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    { await Task.Yield(); return GetProperties(conditions, cancellationToken); }

    public override Response<BlobDownloadResult> DownloadContent(CancellationToken cancellationToken = default)
    {
        var blobClient = new FileBlobClient(_account, _store.ContainerName, _blobName);
        return blobClient.DownloadContent(cancellationToken);
    }

    public override async Task<Response<BlobDownloadResult>> DownloadContentAsync(CancellationToken cancellationToken = default)
    { await Task.Yield(); return DownloadContent(cancellationToken); }
}
