namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class StorageUriParser
{
    public static string? ExtractAccountName(Uri uri, string hostnameSuffix)
    {
        var host = uri.Host;
        if (string.IsNullOrEmpty(host)) return null;

        // Expect "{account}.blob.{suffix}" or "{account}.table.{suffix}".
        var firstDot = host.IndexOf('.');
        if (firstDot <= 0) return null;
        return host[..firstDot];
    }

    public static (string Account, string Container, string? Blob) ParseBlobUri(Uri uri, string hostnameSuffix)
    {
        var account = ExtractAccountName(uri, hostnameSuffix)
            ?? throw new ArgumentException($"Cannot determine account name from URI: {uri}", nameof(uri));

        var path = uri.AbsolutePath.TrimStart('/');
        if (string.IsNullOrEmpty(path))
            throw new ArgumentException("URI must include a container name.", nameof(uri));

        var slash = path.IndexOf('/');
        if (slash < 0)
            return (account, path, null);

        return (account, path[..slash], Uri.UnescapeDataString(path[(slash + 1)..]));
    }

    public static (string Account, string Table) ParseTableUri(Uri uri, string hostnameSuffix)
    {
        var account = ExtractAccountName(uri, hostnameSuffix)
            ?? throw new ArgumentException($"Cannot determine account name from URI: {uri}", nameof(uri));

        var path = uri.AbsolutePath.TrimStart('/');
        if (string.IsNullOrEmpty(path))
            throw new ArgumentException("URI must include a table name.", nameof(uri));

        var slash = path.IndexOf('/');
        return (account, slash < 0 ? path : path[..slash]);
    }
}
