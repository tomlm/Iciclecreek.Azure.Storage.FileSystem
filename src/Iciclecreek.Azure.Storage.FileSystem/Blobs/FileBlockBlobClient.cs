using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs;

public class FileBlockBlobClient : BlockBlobClient
{
    internal readonly BlobStore _store;
    internal readonly string _blobName;
    internal readonly FileStorageAccount _account;

    public FileBlockBlobClient(string connectionString, string containerName, string blobName, FileStorageProvider provider) : base()
    {
        _account = ConnectionStringParser.ResolveAccount(connectionString, provider);
        _store = new BlobStore(_account, containerName);
        _blobName = blobName;
    }

    public FileBlockBlobClient(Uri blobUri, FileStorageProvider provider) : base()
    {
        var (acctName, container, blob) = Iciclecreek.Azure.Storage.FileSystem.Internal.StorageUriParser.ParseBlobUri(blobUri, provider.HostnameSuffix);
        _account = provider.GetAccount(acctName);
        _store = new BlobStore(_account, container);
        _blobName = blob ?? throw new ArgumentException("URI must include a blob name.", nameof(blobUri));
    }

    internal FileBlockBlobClient(FileStorageAccount account, string containerName, string blobName) : base()
    {
        _account = account;
        _store = new BlobStore(account, containerName);
        _blobName = blobName;
    }

    public static FileBlockBlobClient FromAccount(FileStorageAccount account, string containerName, string blobName)
        => new(account, containerName, blobName);

    public override string Name => _blobName;
    public override string BlobContainerName => _store.ContainerName;
    public override string AccountName => _account.Name;
    public override Uri Uri => new($"{_account.BlobServiceUri}{_store.ContainerName}/{System.Uri.EscapeDataString(_blobName)}");

    // ---- StageBlock (async = primary) ----

    public override async Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, byte[] transactionalContentHash = default!, BlobRequestConditions conditions = default!, IProgress<long>? progressHandler = default, CancellationToken cancellationToken = default)
    {
        var staging = new BlockStagingStore(_store, _blobName);
        await staging.StageAsync(base64BlockId, content, cancellationToken).ConfigureAwait(false);
        var info = BlobsModelFactory.BlockInfo(null, null, null!, null!);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override async Task<Response<BlockInfo>> StageBlockAsync(string base64BlockId, Stream content, BlockBlobStageBlockOptions options, CancellationToken cancellationToken = default)
        => await StageBlockAsync(base64BlockId, content, null!, options?.Conditions, null, cancellationToken).ConfigureAwait(false);

    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, byte[] transactionalContentHash = default!, BlobRequestConditions conditions = default!, IProgress<long>? progressHandler = default, CancellationToken cancellationToken = default)
        => StageBlockAsync(base64BlockId, content, transactionalContentHash, conditions, progressHandler, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlockInfo> StageBlock(string base64BlockId, Stream content, BlockBlobStageBlockOptions options, CancellationToken cancellationToken = default)
        => StageBlockAsync(base64BlockId, content, options, cancellationToken).GetAwaiter().GetResult();

    // ---- CommitBlockList (async = primary) ----

    public override async Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, CommitBlockListOptions options = default!, CancellationToken cancellationToken = default)
    {
        var staging = new BlockStagingStore(_store, _blobName);
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        var conditions = options?.Conditions;
        _store.CheckConditions(sidecar, conditions?.IfMatch, conditions?.IfNoneMatch, mustExist: false, nameof(CommitBlockList));

        var blobPath = _store.BlobPath(_blobName);
        var dir = Path.GetDirectoryName(blobPath);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        var tmpPath = blobPath + ".commit.tmp";
        var committedBlocks = new List<CommittedBlock>();

        byte[] hash;
        long length;
        using var md5 = System.Security.Cryptography.IncrementalHash.CreateHash(System.Security.Cryptography.HashAlgorithmName.MD5);

        await using (var outFs = new FileStream(tmpPath, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true))
        {
            foreach (var blockId in base64BlockIds)
            {
                await using var blockStream = await staging.OpenBlockAsync(blockId).ConfigureAwait(false);
                if (blockStream is null)
                    throw new RequestFailedException(400, $"Block '{blockId}' not found in staging.", "InvalidBlockList", null);

                var startPos = outFs.Position;
                var buf = new byte[81920];
                int read;
                while ((read = await blockStream.ReadAsync(buf, cancellationToken).ConfigureAwait(false)) > 0)
                {
                    await outFs.WriteAsync(buf.AsMemory(0, read), cancellationToken).ConfigureAwait(false);
                    md5.AppendData(buf, 0, read);
                }
                committedBlocks.Add(new CommittedBlock { Id = blockId, Size = outFs.Position - startPos });
            }
            await outFs.FlushAsync(cancellationToken).ConfigureAwait(false);

            hash = md5.GetHashAndReset();
            length = outFs.Length;
        }

        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(length, now, hash);

        if (File.Exists(blobPath))
            File.Replace(tmpPath, blobPath, destinationBackupFileName: null, ignoreMetadataErrors: true);
        else
            File.Move(tmpPath, blobPath);

        var newSidecar = sidecar ?? new BlobSidecar();
        newSidecar.BlobType = BlobKind.Block;
        newSidecar.Length = length;
        newSidecar.ContentHashBase64 = Convert.ToBase64String(hash);
        newSidecar.ETag = etag.ToString()!;
        newSidecar.LastModifiedUtc = now;
        if (sidecar is null) newSidecar.CreatedOnUtc = now;
        newSidecar.CommittedBlocks = committedBlocks;
        if (options?.HttpHeaders is { } headers)
        {
            newSidecar.ContentType = headers.ContentType;
            newSidecar.ContentEncoding = headers.ContentEncoding;
        }
        if (options?.Metadata is { } meta)
            newSidecar.Metadata = new Dictionary<string, string>(meta, StringComparer.Ordinal);
        await _store.WriteSidecarAsync(_blobName, newSidecar, cancellationToken).ConfigureAwait(false);

        var info = BlobsModelFactory.BlobContentInfo(etag, now, hash, null!, null!, null!, 0);
        return Response.FromValue(info, StubResponse.Created());
    }

    public override async Task<Response<BlobContentInfo>> CommitBlockListAsync(IEnumerable<string> base64BlockIds, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, CancellationToken cancellationToken = default)
        => await CommitBlockListAsync(base64BlockIds, new CommitBlockListOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    public override Response<BlobContentInfo> CommitBlockList(IEnumerable<string> base64BlockIds, CommitBlockListOptions options = default!, CancellationToken cancellationToken = default)
        => CommitBlockListAsync(base64BlockIds, options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> CommitBlockList(IEnumerable<string> base64BlockIds, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, CancellationToken cancellationToken = default)
        => CommitBlockListAsync(base64BlockIds, httpHeaders, metadata, conditions, accessTier, cancellationToken).GetAwaiter().GetResult();

    // ---- GetBlockList (async = primary) ----

    public override async Task<Response<BlockList>> GetBlockListAsync(BlockListTypes blockListTypes = BlockListTypes.All, string snapshot = default!, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
    {
        var sidecar = await _store.ReadSidecarAsync(_blobName, cancellationToken).ConfigureAwait(false);
        var staging = new BlockStagingStore(_store, _blobName);

        var committed = new List<BlobBlock>();
        var uncommitted = new List<BlobBlock>();

        if ((blockListTypes & BlockListTypes.Committed) != 0 && sidecar is not null)
        {
            foreach (var b in sidecar.CommittedBlocks)
                committed.Add(BlobsModelFactory.BlobBlock(b.Id, (int)b.Size));
        }

        if ((blockListTypes & BlockListTypes.Uncommitted) != 0)
        {
            var committedIds = sidecar?.CommittedBlocks.Select(b => b.Id).ToHashSet() ?? new HashSet<string>();
            var index = await staging.ReadIndexAsync(cancellationToken).ConfigureAwait(false);
            foreach (var kvp in index)
            {
                if (!committedIds.Contains(kvp.Key))
                    uncommitted.Add(BlobsModelFactory.BlobBlock(kvp.Key, (int)kvp.Value.Size));
            }
        }

        var blockList = BlobsModelFactory.BlockList(committed, uncommitted);
        return Response.FromValue(blockList, StubResponse.Ok());
    }

    public override Response<BlockList> GetBlockList(BlockListTypes blockListTypes = BlockListTypes.All, string snapshot = default!, BlobRequestConditions conditions = default!, CancellationToken cancellationToken = default)
        => GetBlockListAsync(blockListTypes, snapshot, conditions, cancellationToken).GetAwaiter().GetResult();

    // ---- Upload (async = primary) ----

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
    {
        var fileBlobClient = new FileBlobClient(_account, _store.ContainerName, _blobName);
        return await fileBlobClient.UploadCoreAsync(content, options, cancellationToken).ConfigureAwait(false);
    }

    public override async Task<Response<BlobContentInfo>> UploadAsync(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => await UploadAsync(content, new BlobUploadOptions { HttpHeaders = httpHeaders, Metadata = metadata, Conditions = conditions }, cancellationToken).ConfigureAwait(false);

    public override Response<BlobContentInfo> Upload(Stream content, BlobUploadOptions options, CancellationToken cancellationToken = default)
        => UploadAsync(content, options, cancellationToken).GetAwaiter().GetResult();

    public override Response<BlobContentInfo> Upload(Stream content, BlobHttpHeaders httpHeaders, IDictionary<string, string> metadata, BlobRequestConditions conditions, AccessTier? accessTier, IProgress<long>? progressHandler, CancellationToken cancellationToken = default)
        => UploadAsync(content, httpHeaders, metadata, conditions, accessTier, progressHandler, cancellationToken).GetAwaiter().GetResult();

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
