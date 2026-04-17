namespace Iciclecreek.Azure.Storage.FileSystem;

public sealed class FileStorageAccount
{
    internal FileStorageAccount(FileStorageProvider provider, string name)
    {
        Provider = provider;
        Name = name;
        RootPath = Path.Combine(provider.RootPath, name);
        BlobsRootPath = Path.Combine(RootPath, "blobs");
        TablesRootPath = Path.Combine(RootPath, "tables");
        BlobServiceUri = new Uri($"https://{name}.blob.{provider.HostnameSuffix}/");
        TableServiceUri = new Uri($"https://{name}.table.{provider.HostnameSuffix}/");
    }

    public FileStorageProvider Provider { get; }

    public string Name { get; }

    public string RootPath { get; }

    public string BlobsRootPath { get; }

    public string TablesRootPath { get; }

    public Uri BlobServiceUri { get; }

    public Uri TableServiceUri { get; }

    public string GetConnectionString()
    {
        // Fabricated key — consumers should never validate signatures against a file fake.
        const string fakeKey = "RmlsZUZha2VBY2NvdW50S2V5Tm90UmVhbA==";
        return $"DefaultEndpointsProtocol=https;"
             + $"AccountName={Name};"
             + $"AccountKey={fakeKey};"
             + $"BlobEndpoint={BlobServiceUri};"
             + $"TableEndpoint={TableServiceUri};"
             + $"EndpointSuffix={Provider.HostnameSuffix}";
    }
}
