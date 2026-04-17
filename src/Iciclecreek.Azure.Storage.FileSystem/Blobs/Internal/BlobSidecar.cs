using System.Text.Json;
using System.Text.Json.Serialization;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal enum BlobKind { Block, Append }

internal sealed class BlobSidecar
{
    public BlobKind BlobType { get; set; } = BlobKind.Block;

    public string? ContentType { get; set; }

    public string? ContentEncoding { get; set; }

    public string? ContentLanguage { get; set; }

    public string? ContentDisposition { get; set; }

    public string? CacheControl { get; set; }

    public string? ContentHashBase64 { get; set; }

    public string ETag { get; set; } = "\"0x0\"";

    public DateTimeOffset CreatedOnUtc { get; set; } = DateTimeOffset.UtcNow;

    public DateTimeOffset LastModifiedUtc { get; set; } = DateTimeOffset.UtcNow;

    public long Length { get; set; }

    public Dictionary<string, string> Metadata { get; set; } = new(StringComparer.Ordinal);

    public List<CommittedBlock> CommittedBlocks { get; set; } = new();

    public static async Task<BlobSidecar?> ReadFromFileAsync(string path, JsonSerializerOptions options, CancellationToken ct = default)
    {
        if (!File.Exists(path)) return null;
        try
        {
            await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
            return await JsonSerializer.DeserializeAsync<BlobSidecar>(fs, options, ct).ConfigureAwait(false);
        }
        catch (IOException)
        {
            await Task.Delay(10, ct).ConfigureAwait(false);
            await using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
            return await JsonSerializer.DeserializeAsync<BlobSidecar>(fs, options, ct).ConfigureAwait(false);
        }
    }
}

internal sealed class CommittedBlock
{
    public string Id { get; set; } = "";
    public long Size { get; set; }
}

[JsonSerializable(typeof(BlobSidecar))]
[JsonSerializable(typeof(CommittedBlock))]
internal partial class BlobSidecarJsonContext : JsonSerializerContext { }
