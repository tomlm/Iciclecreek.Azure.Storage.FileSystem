using Azure;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobContainerLeaseTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    private FileBlobContainerClient CreateContainer(string name)
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, name);
        cc.CreateIfNotExists();
        return cc;
    }

    [Test]
    public void Acquire_Returns_LeaseId()
    {
        var cc = CreateContainer("cl-acquire");
        var lease = cc.GetBlobLeaseClient();

        var result = lease.Acquire(TimeSpan.FromSeconds(30)).Value;

        Assert.That(result.LeaseId, Is.Not.Null.And.Not.Empty);
        Assert.That(lease.LeaseId, Is.EqualTo(result.LeaseId));
    }

    [Test]
    public void Acquire_Infinite_Duration()
    {
        var cc = CreateContainer("cl-infinite");
        var lease = cc.GetBlobLeaseClient();

        var result = lease.Acquire(TimeSpan.FromSeconds(-1)).Value;

        Assert.That(result.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Acquire_Throws_409_When_Already_Leased()
    {
        var cc = CreateContainer("cl-conflict");
        var lease1 = cc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(60));

        var lease2 = cc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void Release_Frees_Lease()
    {
        var cc = CreateContainer("cl-release");
        var lease = cc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(30));
        lease.Release();

        // Should be able to acquire again
        var lease2 = cc.GetBlobLeaseClient();
        Assert.DoesNotThrow(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Renew_Extends_Lease()
    {
        var cc = CreateContainer("cl-renew");
        var lease = cc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(15));

        var result = lease.Renew().Value;

        Assert.That(result.LeaseId, Is.EqualTo(lease.LeaseId));
    }

    [Test]
    public void Change_UpdatesId()
    {
        var cc = CreateContainer("cl-change");
        var lease = cc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(30));

        var newId = Guid.NewGuid().ToString();
        var result = lease.Change(newId).Value;

        Assert.That(result.LeaseId, Is.EqualTo(newId));
        Assert.That(lease.LeaseId, Is.EqualTo(newId));
    }

    [Test]
    public void Break_Releases_Lease()
    {
        var cc = CreateContainer("cl-break");
        var lease = cc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(60));

        var result = lease.Break().Value;
        Assert.That(result.LeaseId, Is.Not.Null);

        // Should be able to acquire again
        var lease2 = cc.GetBlobLeaseClient();
        Assert.DoesNotThrow(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void GetBlobLeaseClient_Returns_FileContainerLeaseClient()
    {
        var cc = CreateContainer("cl-type");
        var lease = cc.GetBlobLeaseClient();

        Assert.That(lease, Is.InstanceOf<FileContainerLeaseClient>());
    }
}
