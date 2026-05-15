namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class AtomicFile
{
    public static async Task WriteAllBytesAsync(string path, byte[] bytes, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        await File.WriteAllBytesAsync(tmp, bytes, ct).ConfigureAwait(false);
        MoveOverwrite(tmp, path);
    }

    public static async Task WriteAllTextAsync(string path, string text, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        await File.WriteAllTextAsync(tmp, text, ct).ConfigureAwait(false);
        MoveOverwrite(tmp, path);
    }

    public static async Task WriteStreamAsync(string path, Stream content, CancellationToken ct = default)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        await using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None, 81920, useAsync: true))
        {
            await content.CopyToAsync(fs, ct).ConfigureAwait(false);
            await fs.FlushAsync(ct).ConfigureAwait(false);
        }
        MoveOverwrite(tmp, path);
    }

    private static void MoveOverwrite(string source, string destination)
    {
        if (File.Exists(destination))
            File.Replace(source, destination, destinationBackupFileName: null, ignoreMetadataErrors: true);
        else
            File.Move(source, destination);
    }
}
