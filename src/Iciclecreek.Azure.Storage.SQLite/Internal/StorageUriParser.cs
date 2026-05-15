namespace Iciclecreek.Azure.Storage.SQLite.Internal;

internal static class StorageUriParser
{
    public static string? ExtractAccountName(Uri uri, string hostnameSuffix)
    {
        var host = uri.Host;
        // Expected format: {accountName}.blob.{hostnameSuffix}
        var blobSuffix = $".blob.{hostnameSuffix}";
        if (host.EndsWith(blobSuffix, StringComparison.OrdinalIgnoreCase))
            return host[..^blobSuffix.Length];

        var tableSuffix = $".table.{hostnameSuffix}";
        if (host.EndsWith(tableSuffix, StringComparison.OrdinalIgnoreCase))
            return host[..^tableSuffix.Length];

        var queueSuffix = $".queue.{hostnameSuffix}";
        if (host.EndsWith(queueSuffix, StringComparison.OrdinalIgnoreCase))
            return host[..^queueSuffix.Length];

        return null;
    }

    public static (string AccountName, string ContainerName, string? BlobName) ParseBlobUri(Uri uri, string hostnameSuffix)
    {
        var accountName = ExtractAccountName(uri, hostnameSuffix)
            ?? throw new ArgumentException("Cannot determine account name from URI.", nameof(uri));

        var path = uri.AbsolutePath.TrimStart('/');
        var slashIndex = path.IndexOf('/');
        if (slashIndex < 0)
            return (accountName, path, null);

        var container = path[..slashIndex];
        var blob = Uri.UnescapeDataString(path[(slashIndex + 1)..]);
        return (accountName, container, string.IsNullOrEmpty(blob) ? null : blob);
    }
}
