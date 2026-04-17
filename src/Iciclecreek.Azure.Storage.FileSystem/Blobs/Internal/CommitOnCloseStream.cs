using System.Security.Cryptography;
using Azure.Storage.Blobs.Models;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal sealed class CommitOnCloseStream : Stream
{
    private readonly FileStream _inner;
    private readonly string _tmpPath;
    private readonly string _blobName;
    private readonly BlobStore _store;
    private readonly BlobHttpHeaders? _headers;
    private readonly IDictionary<string, string>? _metadata;
    private bool _committed;

    public CommitOnCloseStream(FileStream inner, string tmpPath, string blobName, BlobStore store, BlobHttpHeaders? headers, IDictionary<string, string>? metadata)
    {
        _inner = inner;
        _tmpPath = tmpPath;
        _blobName = blobName;
        _store = store;
        _headers = headers;
        _metadata = metadata;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _inner.Length;
    public override long Position { get => _inner.Position; set => _inner.Position = value; }

    public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
    public override void Write(ReadOnlySpan<byte> buffer) => _inner.Write(buffer);
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _inner.WriteAsync(buffer, offset, count, ct);
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _inner.WriteAsync(buffer, ct);
    public override void WriteByte(byte value) => _inner.WriteByte(value);
    public override void Flush() => _inner.Flush();
    public override Task FlushAsync(CancellationToken ct) => _inner.FlushAsync(ct);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => _inner.SetLength(value);

    protected override void Dispose(bool disposing)
    {
        if (disposing && !_committed)
            CommitAsync().GetAwaiter().GetResult();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        if (!_committed)
            await CommitAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }

    private async Task CommitAsync()
    {
        _committed = true;
        await _inner.FlushAsync().ConfigureAwait(false);
        var length = _inner.Length;
        _inner.Close();
        await _inner.DisposeAsync().ConfigureAwait(false);

        var blobPath = _store.BlobPath(_blobName);
        if (File.Exists(blobPath))
            File.Replace(_tmpPath, blobPath, destinationBackupFileName: null, ignoreMetadataErrors: true);
        else
            File.Move(_tmpPath, blobPath);

        var md5 = MD5.HashData(await File.ReadAllBytesAsync(blobPath).ConfigureAwait(false));
        var now = DateTimeOffset.UtcNow;
        var etag = ETagCalculator.Compute(length, now, md5);

        var sidecar = await _store.ReadSidecarAsync(_blobName).ConfigureAwait(false) ?? new BlobSidecar();
        sidecar.BlobType = BlobKind.Block;
        sidecar.Length = length;
        sidecar.ContentHashBase64 = Convert.ToBase64String(md5);
        sidecar.ETag = etag.ToString()!;
        sidecar.LastModifiedUtc = now;
        if (sidecar.CreatedOnUtc == default) sidecar.CreatedOnUtc = now;
        if (_headers is not null)
        {
            sidecar.ContentType = _headers.ContentType;
            sidecar.ContentEncoding = _headers.ContentEncoding;
            sidecar.ContentLanguage = _headers.ContentLanguage;
            sidecar.ContentDisposition = _headers.ContentDisposition;
            sidecar.CacheControl = _headers.CacheControl;
        }
        if (_metadata is not null)
            sidecar.Metadata = new Dictionary<string, string>(_metadata, StringComparer.Ordinal);
        sidecar.CommittedBlocks.Clear();
        await _store.WriteSidecarAsync(_blobName, sidecar).ConfigureAwait(false);
    }
}
