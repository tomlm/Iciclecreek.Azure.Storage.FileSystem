using System.Text.Json;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal sealed class BlockStagingStore
{
    private readonly string _stagingDir;
    private readonly JsonSerializerOptions _jsonOpts;

    public BlockStagingStore(BlobStore store, string blobName)
    {
        _stagingDir = Path.Combine(store.ContainerPath, ".blocks", BlobPathEncoder.EncodeBlockId(blobName));
        _jsonOpts = store.Provider.JsonSerializerOptions;
    }

    public string StagingDir => _stagingDir;

    public void Stage(string base64BlockId, Stream content)
    {
        Directory.CreateDirectory(_stagingDir);
        var encoded = BlobPathEncoder.EncodeBlockId(base64BlockId);
        var blockPath = Path.Combine(_stagingDir, encoded + ".block");

        using var fs = new FileStream(blockPath, FileMode.Create, FileAccess.Write, FileShare.None);
        content.CopyTo(fs);
        fs.Flush(flushToDisk: true);

        var index = ReadIndex();
        index[base64BlockId] = new StagedBlockInfo { Size = new FileInfo(blockPath).Length, StagedAtUtc = DateTimeOffset.UtcNow };
        WriteIndex(index);
    }

    public Stream? OpenBlock(string base64BlockId)
    {
        var encoded = BlobPathEncoder.EncodeBlockId(base64BlockId);
        var blockPath = Path.Combine(_stagingDir, encoded + ".block");
        return File.Exists(blockPath) ? File.OpenRead(blockPath) : null;
    }

    public long? GetBlockSize(string base64BlockId)
    {
        var index = ReadIndex();
        return index.TryGetValue(base64BlockId, out var info) ? info.Size : null;
    }

    public Dictionary<string, StagedBlockInfo> ReadIndex()
    {
        var indexPath = Path.Combine(_stagingDir, "staging.json");
        if (!File.Exists(indexPath)) return new();
        var json = File.ReadAllText(indexPath);
        return JsonSerializer.Deserialize<Dictionary<string, StagedBlockInfo>>(json, _jsonOpts) ?? new();
    }

    public void WriteIndex(Dictionary<string, StagedBlockInfo> index)
    {
        Directory.CreateDirectory(_stagingDir);
        var indexPath = Path.Combine(_stagingDir, "staging.json");
        var json = JsonSerializer.Serialize(index, _jsonOpts);
        File.WriteAllText(indexPath, json);
    }

    public void RemoveBlock(string base64BlockId)
    {
        var encoded = BlobPathEncoder.EncodeBlockId(base64BlockId);
        var blockPath = Path.Combine(_stagingDir, encoded + ".block");
        if (File.Exists(blockPath)) File.Delete(blockPath);

        var index = ReadIndex();
        index.Remove(base64BlockId);
        WriteIndex(index);
    }

    public void Cleanup()
    {
        if (Directory.Exists(_stagingDir))
            Directory.Delete(_stagingDir, recursive: true);
    }
}

internal sealed class StagedBlockInfo
{
    public long Size { get; set; }
    public DateTimeOffset StagedAtUtc { get; set; }
}
