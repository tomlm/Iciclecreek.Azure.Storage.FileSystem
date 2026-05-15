using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobLeaseTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    protected BlobClient CreateBlob(string container, string blob, string content = "test")
    {
        var cc = _fixture.CreateBlobContainerClient(container);
        cc.CreateIfNotExists();
        var bc = cc.GetBlobClient(blob);
        bc.Upload(BinaryData.FromString(content));
        return bc;
    }

    // ── Acquire_Returns_Lease_With_Id ──────────────────────────────────

    [Test]
    public void Acquire_Returns_Lease_With_Id()
    {
        var bc = CreateBlob("lease-acq", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        var lease = leaseClient.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── AcquireAsync_Works ─────────────────────────────────────────────

    [Test]
    public async Task AcquireAsync_Works()
    {
        var bc = CreateBlob("lease-acq-async", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        var lease = await leaseClient.AcquireAsync(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Acquire_Infinite_Duration ──────────────────────────────────────

    [Test]
    public void Acquire_Infinite_Duration()
    {
        var bc = CreateBlob("lease-inf", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        var lease = leaseClient.Acquire(TimeSpan.FromSeconds(-1));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Acquire_Throws_409_When_Already_Leased ─────────────────────────

    [Test]
    public void Acquire_Throws_409_When_Already_Leased()
    {
        var bc = CreateBlob("lease-conflict", "file.txt");
        var lease1 = bc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(30));

        var lease2 = bc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() =>
            lease2.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    // ── Release_Frees_Lease ────────────────────────────────────────────

    [Test]
    public void Release_Frees_Lease()
    {
        var bc = CreateBlob("lease-release", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));
        leaseClient.Release();

        var leaseClient2 = bc.GetBlobLeaseClient();
        var lease = leaseClient2.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Release_Throws_409_With_Wrong_LeaseId ──────────────────────────

    [Test]
    public void Release_Throws_409_With_Wrong_LeaseId()
    {
        var bc = CreateBlob("lease-wrong", "file.txt");
        var lease1 = bc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(30));

        var lease2 = bc.GetBlobLeaseClient("wrong-id");
        var ex = Assert.Throws<RequestFailedException>(() => lease2.Release());
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    // ── Renew_Extends_Lease ────────────────────────────────────────────

    [Test]
    public void Renew_Extends_Lease()
    {
        var bc = CreateBlob("lease-renew", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        var original = leaseClient.Acquire(TimeSpan.FromSeconds(15));
        var renewed = leaseClient.Renew();
        Assert.That(renewed.Value.LeaseId, Is.EqualTo(original.Value.LeaseId));
    }

    // ── Change_Updates_LeaseId ─────────────────────────────────────────

    [Test]
    public void Change_Updates_LeaseId()
    {
        var bc = CreateBlob("lease-change", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));

        var newId = Guid.NewGuid().ToString();
        var changed = leaseClient.Change(newId);
        Assert.That(changed.Value.LeaseId, Is.EqualTo(newId));
    }

    // ── Break_Releases_Lease ───────────────────────────────────────────

    [Test]
    public void Break_Releases_Lease()
    {
        var bc = CreateBlob("lease-break", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));
        leaseClient.Break();

        var leaseClient2 = bc.GetBlobLeaseClient();
        var lease = leaseClient2.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Break_Throws_409_When_No_Lease ─────────────────────────────────

    [Test]
    public void Break_Throws_409_When_No_Lease()
    {
        var bc = CreateBlob("lease-break-none", "file.txt");
        var leaseClient = bc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() => leaseClient.Break());
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    // ── Acquire_Throws_404_For_Missing_Blob ────────────────────────────

    [Test]
    public void Acquire_Throws_404_For_Missing_Blob()
    {
        var cc = _fixture.CreateBlobContainerClient("lease-missing");
        cc.CreateIfNotExists();
        var bc = cc.GetBlobClient("does-not-exist.txt");

        var leaseClient = bc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() =>
            leaseClient.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(404));
    }
}
