using Azure;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.SQLite.Blobs;
using Iciclecreek.Azure.Storage.SQLite.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.SQLite.Tests.Blobs;

public class BlobLeaseTests
{
    private TempDb _db = null!;

    [SetUp]
    public void Setup() => _db = new TempDb();

    [TearDown]
    public void TearDown() => _db.Dispose();

    private SqliteBlobClient CreateBlob(string container, string blob, string content = "test")
    {
        var cc = SqliteBlobContainerClient.FromAccount(_db.Account, container);
        cc.CreateIfNotExists();
        var bc = (SqliteBlobClient)cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString(content));
        return bc;
    }

    [Test]
    public void Acquire_Returns_Lease_With_Id()
    {
        var bc = CreateBlob("lease-acq", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        var result = lease.Acquire(TimeSpan.FromSeconds(30)).Value;

        Assert.That(result.LeaseId, Is.Not.Null.And.Not.Empty);
        Assert.That(lease.LeaseId, Is.EqualTo(result.LeaseId));
    }

    [Test]
    public async Task AcquireAsync_Works()
    {
        var bc = CreateBlob("lease-acq-async", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        var result = (await lease.AcquireAsync(TimeSpan.FromSeconds(15))).Value;

        Assert.That(result.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Acquire_Infinite_Duration()
    {
        var bc = CreateBlob("lease-inf", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        var result = lease.Acquire(TimeSpan.FromSeconds(-1)).Value;

        Assert.That(result.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    [Test]
    public void Acquire_Throws_409_When_Already_Leased()
    {
        var bc = CreateBlob("lease-conflict", "myblob.txt");
        var lease1 = bc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(60));

        var lease2 = bc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void Release_Frees_Lease()
    {
        var bc = CreateBlob("lease-release", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(30));
        lease.Release();

        // Should be able to acquire again
        var lease2 = bc.GetBlobLeaseClient();
        Assert.DoesNotThrow(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Release_Throws_409_With_Wrong_LeaseId()
    {
        var bc = CreateBlob("lease-bad-release", "myblob.txt");
        var lease1 = bc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(60));

        var lease2 = bc.GetBlobLeaseClient("wrong-id");
        var ex = Assert.Throws<RequestFailedException>(() => lease2.Release());
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void Renew_Extends_Lease()
    {
        var bc = CreateBlob("lease-renew", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(15));

        var result = lease.Renew().Value;
        Assert.That(result.LeaseId, Is.EqualTo(lease.LeaseId));
    }

    [Test]
    public void Change_Updates_LeaseId()
    {
        var bc = CreateBlob("lease-change", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(30));

        var newId = Guid.NewGuid().ToString();
        var result = lease.Change(newId).Value;

        Assert.That(result.LeaseId, Is.EqualTo(newId));
        Assert.That(lease.LeaseId, Is.EqualTo(newId));
    }

    [Test]
    public void Break_Releases_Lease()
    {
        var bc = CreateBlob("lease-break", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        lease.Acquire(TimeSpan.FromSeconds(60));

        var result = lease.Break().Value;
        Assert.That(result.LeaseId, Is.Not.Null);

        // Should be able to acquire again
        var lease2 = bc.GetBlobLeaseClient();
        Assert.DoesNotThrow(() => lease2.Acquire(TimeSpan.FromSeconds(30)));
    }

    [Test]
    public void Break_Throws_409_When_No_Lease()
    {
        var bc = CreateBlob("lease-no-break", "myblob.txt");
        var lease = bc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() => lease.Break());
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    [Test]
    public void Acquire_Throws_404_For_Missing_Blob()
    {
        var cc = SqliteBlobContainerClient.FromAccount(_db.Account, "lease-missing");
        cc.CreateIfNotExists();
        var bc = (SqliteBlobClient)cc.GetBlobClient("nope.txt");
        var lease = bc.GetBlobLeaseClient();

        var ex = Assert.Throws<RequestFailedException>(() => lease.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(404));
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
