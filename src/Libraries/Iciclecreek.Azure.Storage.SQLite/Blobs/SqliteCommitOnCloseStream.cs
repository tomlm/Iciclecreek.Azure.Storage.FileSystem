using Azure.Storage.Blobs.Models;

namespace Iciclecreek.Azure.Storage.SQLite.Blobs;

/// <summary>
/// A <see cref="Stream"/> wrapper that buffers writes in memory and commits the blob
/// to the SQLite database when disposed.
/// </summary>
internal sealed class SqliteCommitOnCloseStream : Stream
{
    private readonly MemoryStream _buffer = new();
    private readonly SqliteBlobClient _client;
    private readonly BlobHttpHeaders? _headers;
    private readonly IDictionary<string, string>? _metadata;
    private bool _committed;

    public SqliteCommitOnCloseStream(SqliteBlobClient client, BlobHttpHeaders? headers, IDictionary<string, string>? metadata)
    {
        _client = client;
        _headers = headers;
        _metadata = metadata;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => _buffer.Length;
    public override long Position { get => _buffer.Position; set => _buffer.Position = value; }

    public override void Write(byte[] buffer, int offset, int count) => _buffer.Write(buffer, offset, count);
    public override void Write(ReadOnlySpan<byte> buffer) => _buffer.Write(buffer);
    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken ct) => _buffer.WriteAsync(buffer, offset, count, ct);
    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken ct = default) => _buffer.WriteAsync(buffer, ct);
    public override void WriteByte(byte value) => _buffer.WriteByte(value);

    public override void Flush() => _buffer.Flush();
    public override Task FlushAsync(CancellationToken ct) => _buffer.FlushAsync(ct);

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    private void Commit()
    {
        if (_committed) return;
        _committed = true;

        _buffer.Position = 0;
        _client.UploadCoreAsync(_buffer, new BlobUploadOptions
        {
            HttpHeaders = _headers,
            Metadata = _metadata,
        }).GetAwaiter().GetResult();
    }

    private async Task CommitAsync()
    {
        if (_committed) return;
        _committed = true;

        _buffer.Position = 0;
        await _client.UploadCoreAsync(_buffer, new BlobUploadOptions
        {
            HttpHeaders = _headers,
            Metadata = _metadata,
        }).ConfigureAwait(false);
    }

    protected override void Dispose(bool disposing)
    {
        if (disposing) Commit();
        _buffer.Dispose();
        base.Dispose(disposing);
    }

    public override async ValueTask DisposeAsync()
    {
        await CommitAsync().ConfigureAwait(false);
        await _buffer.DisposeAsync().ConfigureAwait(false);
        await base.DisposeAsync().ConfigureAwait(false);
    }
}
