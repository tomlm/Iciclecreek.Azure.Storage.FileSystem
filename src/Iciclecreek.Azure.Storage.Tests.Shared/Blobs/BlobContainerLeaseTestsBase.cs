using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobContainerLeaseTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    protected BlobContainerClient CreateContainer(string name)
    {
        var cc = _fixture.CreateBlobContainerClient(name);
        cc.CreateIfNotExists();
        return cc;
    }

    // ── Acquire_Returns_LeaseId ────────────────────────────────────────

    [Test]
    public void Acquire_Returns_LeaseId()
    {
        var cc = CreateContainer("clease-acq");
        var leaseClient = cc.GetBlobLeaseClient();
        var lease = leaseClient.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Acquire_Infinite_Duration ──────────────────────────────────────

    [Test]
    public void Acquire_Infinite_Duration()
    {
        var cc = CreateContainer("clease-inf");
        var leaseClient = cc.GetBlobLeaseClient();
        var lease = leaseClient.Acquire(TimeSpan.FromSeconds(-1));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Acquire_Throws_409_When_Already_Leased ─────────────────────────

    [Test]
    public void Acquire_Throws_409_When_Already_Leased()
    {
        var cc = CreateContainer("clease-conflict");
        var lease1 = cc.GetBlobLeaseClient();
        lease1.Acquire(TimeSpan.FromSeconds(30));

        var lease2 = cc.GetBlobLeaseClient();
        var ex = Assert.Throws<RequestFailedException>(() =>
            lease2.Acquire(TimeSpan.FromSeconds(30)));
        Assert.That(ex!.Status, Is.EqualTo(409));
    }

    // ── Release_Frees_Lease ────────────────────────────────────────────

    [Test]
    public void Release_Frees_Lease()
    {
        var cc = CreateContainer("clease-release");
        var leaseClient = cc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));
        leaseClient.Release();

        var leaseClient2 = cc.GetBlobLeaseClient();
        var lease = leaseClient2.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }

    // ── Renew_Extends_Lease ────────────────────────────────────────────

    [Test]
    public void Renew_Extends_Lease()
    {
        var cc = CreateContainer("clease-renew");
        var leaseClient = cc.GetBlobLeaseClient();
        var original = leaseClient.Acquire(TimeSpan.FromSeconds(15));
        var renewed = leaseClient.Renew();
        Assert.That(renewed.Value.LeaseId, Is.EqualTo(original.Value.LeaseId));
    }

    // ── Change_UpdatesId ───────────────────────────────────────────────

    [Test]
    public void Change_UpdatesId()
    {
        var cc = CreateContainer("clease-change");
        var leaseClient = cc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));

        var newId = Guid.NewGuid().ToString();
        var changed = leaseClient.Change(newId);
        Assert.That(changed.Value.LeaseId, Is.EqualTo(newId));
    }

    // ── Break_Releases_Lease ───────────────────────────────────────────

    [Test]
    public void Break_Releases_Lease()
    {
        var cc = CreateContainer("clease-break");
        var leaseClient = cc.GetBlobLeaseClient();
        leaseClient.Acquire(TimeSpan.FromSeconds(30));
        leaseClient.Break();

        var leaseClient2 = cc.GetBlobLeaseClient();
        var lease = leaseClient2.Acquire(TimeSpan.FromSeconds(30));
        Assert.That(lease.Value.LeaseId, Is.Not.Null.And.Not.Empty);
    }
}
