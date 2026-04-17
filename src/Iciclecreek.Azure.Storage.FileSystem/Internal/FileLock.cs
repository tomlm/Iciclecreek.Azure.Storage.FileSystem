namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class FileLock
{
    public static FileStream Acquire(
        string path,
        FileMode mode,
        FileAccess access,
        int retryCount = 50,
        TimeSpan retryDelay = default)
    {
        if (retryDelay == default) retryDelay = TimeSpan.FromMilliseconds(20);

        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir))
            Directory.CreateDirectory(dir);

        Exception? last = null;
        for (var i = 0; i <= retryCount; i++)
        {
            try
            {
                return new FileStream(path, mode, access, FileShare.None);
            }
            catch (IOException ex)
            {
                last = ex;
                Thread.Sleep(retryDelay);
            }
            catch (UnauthorizedAccessException ex) when (mode == FileMode.OpenOrCreate || mode == FileMode.Create)
            {
                last = ex;
                Thread.Sleep(retryDelay);
            }
        }
        throw new IOException($"Unable to acquire exclusive lock on '{path}' after {retryCount} attempts.", last);
    }

    public static FileStream Acquire(string path, FileMode mode, FileAccess access, FileStorageProvider provider)
        => Acquire(path, mode, access, provider.LockRetryCount, provider.LockRetryDelay);
}
