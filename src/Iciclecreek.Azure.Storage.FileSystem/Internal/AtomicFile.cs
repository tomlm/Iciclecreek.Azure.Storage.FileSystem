namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class AtomicFile
{
    public static void WriteAllBytes(string path, byte[] bytes)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        File.WriteAllBytes(tmp, bytes);
        MoveOverwrite(tmp, path);
    }

    public static void WriteAllText(string path, string text)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        File.WriteAllText(tmp, text);
        MoveOverwrite(tmp, path);
    }

    public static void WriteStream(string path, Stream content)
    {
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            content.CopyTo(fs);
            fs.Flush(flushToDisk: true);
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
