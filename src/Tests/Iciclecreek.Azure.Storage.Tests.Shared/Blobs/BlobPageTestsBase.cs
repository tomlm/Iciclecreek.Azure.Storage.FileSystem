using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlobPageTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── Helper ─────────────────────────────────────────────────────────

    private (BlobContainerClient cc, PageBlobClient pb) CreatePageBlob(string container, string blob, long size = 4096)
    {
        var cc = _fixture.CreateBlobContainerClient(container);
        cc.CreateIfNotExists();
        var pb = _fixture.CreatePageBlobClient(cc, blob);
        pb.Create(size, (PageBlobCreateOptions?)null);
        return (cc, pb);
    }

    // ── CreateAsync_Works ──────────────────────────────────────────────

    [Test]
    public async Task CreateAsync_Works()
    {
        var container = _fixture.CreateBlobContainerClient("page-create");
        container.CreateIfNotExists();
        var pb = _fixture.CreatePageBlobClient(container, "blob.bin");

        var result = await pb.CreateAsync(4096, (PageBlobCreateOptions?)null);

        Assert.That(result.Value, Is.Not.Null);
        Assert.That(result.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    // ── CreateIfNotExists_IsIdempotent ─────────────────────────────────

    [Test]
    public void CreateIfNotExists_IsIdempotent()
    {
        var container = _fixture.CreateBlobContainerClient("page-idempotent");
        container.CreateIfNotExists();
        var pb = _fixture.CreatePageBlobClient(container, "blob.bin");

        var first = pb.CreateIfNotExists(4096, (PageBlobCreateOptions?)null);
        var second = pb.CreateIfNotExists(4096, (PageBlobCreateOptions?)null);

        Assert.That(first, Is.Not.Null);
        Assert.That(second, Is.Not.Null);
    }

    // ── UploadPages_WritesData ─────────────────────────────────────────

    [Test]
    public void UploadPages_WritesData()
    {
        var (container, pb) = CreatePageBlob("page-upload", "blob.bin", 4096);

        var data = new byte[512];
        Array.Fill(data, (byte)0xAB);
        pb.UploadPages(new MemoryStream(data), 0);

        var downloaded = container.GetBlobClient("blob.bin").DownloadContent();
        var content = downloaded.Value.Content.ToArray();

        Assert.That(content.Length, Is.GreaterThanOrEqualTo(512));
        for (int i = 0; i < 512; i++)
            Assert.That(content[i], Is.EqualTo((byte)0xAB), $"Byte at offset {i} should be 0xAB");
    }

    // ── UploadPages_AtOffset ───────────────────────────────────────────

    [Test]
    public void UploadPages_AtOffset()
    {
        var (container, pb) = CreatePageBlob("page-offset", "blob.bin", 4096);

        var data = new byte[512];
        Array.Fill(data, (byte)0xCD);
        pb.UploadPages(new MemoryStream(data), 512);

        var downloaded = container.GetBlobClient("blob.bin").DownloadContent();
        var content = downloaded.Value.Content.ToArray();

        // First 512 bytes should be zeros
        for (int i = 0; i < 512; i++)
            Assert.That(content[i], Is.EqualTo((byte)0x00), $"Byte at offset {i} should be 0x00");

        // Bytes 512-1023 should be the uploaded data
        for (int i = 512; i < 1024; i++)
            Assert.That(content[i], Is.EqualTo((byte)0xCD), $"Byte at offset {i} should be 0xCD");
    }

    // ── ClearPages_ZeroesData ──────────────────────────────────────────

    [Test]
    public void ClearPages_ZeroesData()
    {
        var (container, pb) = CreatePageBlob("page-clear", "blob.bin", 4096);

        var data = new byte[1024];
        Array.Fill(data, (byte)0xFF);
        pb.UploadPages(new MemoryStream(data), 0);

        pb.ClearPages(new HttpRange(0, 512));

        var downloaded = container.GetBlobClient("blob.bin").DownloadContent();
        var content = downloaded.Value.Content.ToArray();

        // First 512 bytes should be zeroed
        for (int i = 0; i < 512; i++)
            Assert.That(content[i], Is.EqualTo((byte)0x00), $"Byte at offset {i} should be 0x00 after clear");

        // Bytes 512-1023 should still be 0xFF
        for (int i = 512; i < 1024; i++)
            Assert.That(content[i], Is.EqualTo((byte)0xFF), $"Byte at offset {i} should still be 0xFF");
    }

    // ── GetPageRanges_ReturnsWrittenRanges ─────────────────────────────

    [Test]
    public void GetPageRanges_ReturnsWrittenRanges()
    {
        var (container, pb) = CreatePageBlob("page-ranges", "blob.bin", 4096);

        var data1 = new byte[512];
        Array.Fill(data1, (byte)0xAA);
        pb.UploadPages(new MemoryStream(data1), 0);

        var data2 = new byte[512];
        Array.Fill(data2, (byte)0xBB);
        pb.UploadPages(new MemoryStream(data2), 1024);

        var ranges = pb.GetPageRanges();
        Assert.That(ranges.Value.PageRanges.Count(), Is.GreaterThanOrEqualTo(2));
    }

    // ── GetProperties_ReturnsBlobTypePage ──────────────────────────────

    [Test]
    public void GetProperties_ReturnsBlobTypePage()
    {
        var (container, pb) = CreatePageBlob("page-props", "blob.bin", 4096);

        var props = pb.GetProperties().Value;
        Assert.That(props.BlobType, Is.EqualTo(BlobType.Page));
    }
}
