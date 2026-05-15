using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.SQLite;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tables;
using Iciclecreek.Azure.Storage.SQLite.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

public sealed class SqliteStorageTestFixture : StorageTestFixture
{
    private readonly TempDb _db = new();

    public SqliteStorageProvider Provider => _db.Provider;
    public SqliteStorageAccount Account => _db.Account;

    public override string TempPath => _db.Path;
    public override Uri BlobServiceUri => _db.Account.BlobServiceUri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => SqliteBlobContainerClient.FromAccount(_db.Account, name);

    public override BlobServiceClient CreateBlobServiceClient()
        => SqliteBlobServiceClient.FromAccount(_db.Account);

    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => SqliteBlockBlobClient.FromAccount(_db.Account, container.Name, name);

    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => SqliteAppendBlobClient.FromAccount(_db.Account, container.Name, name);

    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => SqlitePageBlobClient.FromAccount(_db.Account, container.Name, name);

    public override TableClient CreateTableClient(string name)
        => SqliteTableClient.FromAccount(_db.Account, name);

    public override TableServiceClient CreateTableServiceClient()
        => SqliteTableServiceClient.FromAccount(_db.Account);

    public override QueueClient CreateQueueClient(string name)
        => SqliteQueueClient.FromAccount(_db.Account, name);

    public override QueueServiceClient CreateQueueServiceClient()
        => SqliteQueueServiceClient.FromAccount(_db.Account);

    public override void Dispose() => _db.Dispose();
}
