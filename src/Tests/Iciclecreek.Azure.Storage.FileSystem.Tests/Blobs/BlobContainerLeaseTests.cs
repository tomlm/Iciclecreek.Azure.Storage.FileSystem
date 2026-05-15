using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobContainerLeaseTests : BlobContainerLeaseTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    [Test]
    public void GetBlobLeaseClient_Returns_FileContainerLeaseClient()
    {
        var cc = _fixture.CreateBlobContainerClient("cl-type");
        cc.CreateIfNotExists();
        var lease = cc.GetBlobLeaseClient();

        Assert.That(lease, Is.InstanceOf<FileContainerLeaseClient>());
    }
}
