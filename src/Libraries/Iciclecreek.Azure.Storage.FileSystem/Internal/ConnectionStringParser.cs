namespace Iciclecreek.Azure.Storage.FileSystem.Internal;

internal static class ConnectionStringParser
{
    public static FileStorageAccount ResolveAccount(string connectionString, FileStorageProvider provider)
    {
        if (string.IsNullOrWhiteSpace(connectionString))
            throw new ArgumentException("Connection string is required.", nameof(connectionString));

        var parts = Parse(connectionString);

        if (parts.TryGetValue("AccountName", out var accountName) && !string.IsNullOrEmpty(accountName))
            return provider.GetAccount(accountName);

        // Fall back to subdomain of BlobEndpoint / TableEndpoint.
        foreach (var key in new[] { "BlobEndpoint", "TableEndpoint" })
        {
            if (parts.TryGetValue(key, out var endpoint) && Uri.TryCreate(endpoint, UriKind.Absolute, out var uri))
            {
                var name = StorageUriParser.ExtractAccountName(uri, provider.HostnameSuffix);
                if (!string.IsNullOrEmpty(name) && provider.TryGetAccount(name, out var acct) && acct is not null)
                    return acct;
            }
        }

        throw new InvalidOperationException("Connection string did not resolve to any registered account.");
    }

    public static IReadOnlyDictionary<string, string> Parse(string connectionString)
    {
        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = segment.IndexOf('=');
            if (eq <= 0) continue;
            var key = segment[..eq].Trim();
            var value = segment[(eq + 1)..].Trim();
            result[key] = value;
        }
        return result;
    }
}
