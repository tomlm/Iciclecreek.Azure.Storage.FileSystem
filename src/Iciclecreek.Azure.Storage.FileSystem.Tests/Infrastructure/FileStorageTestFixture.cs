using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Specialized;
using Azure.Storage.Queues;
using Azure.Data.Tables;
using Iciclecreek.Azure.Storage.FileSystem;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

public sealed class FileStorageTestFixture : StorageTestFixture
{
    private readonly TempRoot _root = new();

    public FileStorageProvider Provider => _root.Provider;
    public FileStorageAccount Account => _root.Account;

    public override string TempPath => _root.Path;
    public override Uri BlobServiceUri => _root.Account.BlobServiceUri;

    public override BlobContainerClient CreateBlobContainerClient(string name)
        => FileBlobContainerClient.FromAccount(_root.Account, name);

    public override BlobServiceClient CreateBlobServiceClient()
        => FileBlobServiceClient.FromAccount(_root.Account);

    public override BlockBlobClient CreateBlockBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)container).GetFileBlockBlobClient(name);

    public override AppendBlobClient CreateAppendBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)container).GetFileAppendBlobClient(name);

    public override PageBlobClient CreatePageBlobClient(BlobContainerClient container, string name)
        => ((FileBlobContainerClient)container).GetFilePageBlobClient(name);

    public override TableClient CreateTableClient(string name)
        => FileTableClient.FromAccount(_root.Account, name);

    public override TableServiceClient CreateTableServiceClient()
        => FileTableServiceClient.FromAccount(_root.Account);

    public override QueueClient CreateQueueClient(string name)
        => FileQueueClient.FromAccount(_root.Account, name);

    public override QueueServiceClient CreateQueueServiceClient()
        => FileQueueServiceClient.FromAccount(_root.Account);

    public override void Dispose() => _root.Dispose();
}
