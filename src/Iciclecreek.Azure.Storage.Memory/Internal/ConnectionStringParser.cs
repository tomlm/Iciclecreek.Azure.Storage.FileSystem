namespace Iciclecreek.Azure.Storage.Memory.Internal;

internal static class ConnectionStringParser
{
    public static MemoryStorageAccount ResolveAccount(string connectionString, MemoryStorageProvider provider)
    {
        var parts = Parse(connectionString);

        if (parts.TryGetValue("AccountName", out var name))
            return provider.GetAccount(name);

        // Fall back to subdomain of BlobEndpoint / TableEndpoint.
        foreach (var key in new[] { "BlobEndpoint", "TableEndpoint", "QueueEndpoint" })
        {
            if (parts.TryGetValue(key, out var ep) && Uri.TryCreate(ep, UriKind.Absolute, out var uri))
            {
                var acct = StorageUriParser.ExtractAccountName(uri, provider.HostnameSuffix);
                if (acct is not null)
                    return provider.GetAccount(acct);
            }
        }

        throw new InvalidOperationException("Cannot determine account name from connection string.");
    }

    public static IReadOnlyDictionary<string, string> Parse(string connectionString)
    {
        var dict = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        foreach (var segment in connectionString.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            var eq = segment.IndexOf('=');
            if (eq > 0)
                dict[segment[..eq].Trim()] = segment[(eq + 1)..].Trim();
        }
        return dict;
    }
}
