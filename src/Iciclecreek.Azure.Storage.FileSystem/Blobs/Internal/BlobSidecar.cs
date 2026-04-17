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

    public static BlobSidecar? ReadFromFile(string path, JsonSerializerOptions options)
    {
        if (!File.Exists(path)) return null;
        try
        {
            using var fs = File.OpenRead(path);
            return JsonSerializer.Deserialize<BlobSidecar>(fs, options);
        }
        catch (IOException)
        {
            // Retry once — may have been mid-write.
            Thread.Sleep(10);
            using var fs = File.OpenRead(path);
            return JsonSerializer.Deserialize<BlobSidecar>(fs, options);
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
