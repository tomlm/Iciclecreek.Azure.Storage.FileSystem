using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;
using Iciclecreek.Azure.Storage.Tests.Shared.Blobs;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobLeaseTests : BlobLeaseTestsBase
{
    protected override StorageTestFixture CreateFixture() => new FileStorageTestFixture();

    [Test]
    public void GetBlobLeaseClient_Returns_FileBlobLeaseClient()
    {
        var bc = CreateBlob("lease-type", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        Assert.That(lease, Is.InstanceOf<FileBlobLeaseClient>());
    }

    [Test]
    public void GetBlobLeaseClient_With_Specified_LeaseId()
    {
        var bc = CreateBlob("lease-id", "myblob.txt");
        var myId = "my-custom-lease-id";
        var lease = bc.GetBlobLeaseClient(myId);
        Assert.That(lease.LeaseId, Is.EqualTo(myId));
    }
}
