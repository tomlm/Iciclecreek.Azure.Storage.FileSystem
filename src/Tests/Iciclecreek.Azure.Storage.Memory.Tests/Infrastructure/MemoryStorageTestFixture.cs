using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.Memory;
using Iciclecreek.Azure.Storage.Memory.Blobs;
using Iciclecreek.Azure.Storage.Memory.Tables;
using Iciclecreek.Azure.Storage.Memory.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.Memory.Tests.Infrastructure;

public sealed class MemoryStorageTestFixture : StorageTestFixture
{
    private readonly string _tempPath;

    public MemoryStorageTestFixture()
    {
        _tempPath = Path.Combine(Path.GetTempPath(), "mem-tests", Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(_tempPath);
        Provider = new MemoryStorageProvider();
        Account = Provider.AddAccount("testacct");
    }

    public MemoryStorageProvider Provider { get; }
    public MemoryStorageAccount Account { get; }

    public override string TempPath => _tempPath;
    public override Uri BlobServiceUri => Account.BlobServiceUri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => MemoryBlobContainerClient.FromAccount(Account, name);

    public override BlobServiceClient CreateBlobServiceClient()
        => MemoryBlobServiceClient.FromAccount(Account);

    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => MemoryBlockBlobClient.FromAccount(Account, container.Name, name);

    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => MemoryAppendBlobClient.FromAccount(Account, container.Name, name);

    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => MemoryPageBlobClient.FromAccount(Account, container.Name, name);

    public override TableClient CreateTableClient(string name)
        => MemoryTableClient.FromAccount(Account, name);

    public override TableServiceClient CreateTableServiceClient()
        => MemoryTableServiceClient.FromAccount(Account);

    public override QueueClient CreateQueueClient(string name)
        => MemoryQueueClient.FromAccount(Account, name);

    public override QueueServiceClient CreateQueueServiceClient()
        => MemoryQueueServiceClient.FromAccount(Account);

    public override void Dispose()
    {
        try { Directory.Delete(_tempPath, true); } catch { }
    }
}
