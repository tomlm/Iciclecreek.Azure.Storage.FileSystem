using Azure;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlobPageTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    private (FileBlobContainerClient cc, FilePageBlobClient pb) CreatePageBlob(string container, string blob, long size = 4096)
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, container);
        cc.CreateIfNotExists();
        var pb = (FilePageBlobClient)cc.GetPageBlobClient(blob);
        pb.Create(size, (PageBlobCreateOptions?)null);
        return (cc, pb);
    }

    [Test]
    public void Create_CreatesPreAllocatedFile()
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, "page-create");
        cc.CreateIfNotExists();
        var pb = (FilePageBlobClient)cc.GetPageBlobClient("blob.bin");

        pb.Create(4096, (PageBlobCreateOptions?)null);

        var blobPath = Path.Combine(cc.ContainerPath, "blob.bin");
        Assert.That(File.Exists(blobPath), Is.True);
        Assert.That(new FileInfo(blobPath).Length, Is.EqualTo(4096));
    }

    [Test]
    public async Task CreateAsync_Works()
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, "page-create-async");
        cc.CreateIfNotExists();
        var pb = (FilePageBlobClient)cc.GetPageBlobClient("blob.bin");

        var result = await pb.CreateAsync(4096, (PageBlobCreateOptions?)null);

        Assert.That(result.Value, Is.Not.Null);
        Assert.That(result.Value.ETag, Is.Not.EqualTo(default(ETag)));
    }

    [Test]
    public void CreateIfNotExists_IsIdempotent()
    {
        var cc = FileBlobContainerClient.FromAccount(_root.Account, "page-idempotent");
        cc.CreateIfNotExists();
        var pb = (FilePageBlobClient)cc.GetPageBlobClient("blob.bin");

        var r1 = pb.CreateIfNotExists(4096, (PageBlobCreateOptions?)null);
        var r2 = pb.CreateIfNotExists(4096, (PageBlobCreateOptions?)null);

        Assert.That(r1.Value, Is.Not.Null);
        Assert.That(r2.Value, Is.Not.Null);
    }

    [Test]
    public void UploadPages_WritesData()
    {
        var (cc, pb) = CreatePageBlob("page-upload", "blob.bin", 4096);

        var data = new byte[512];
        Array.Fill(data, (byte)0xAB);
        pb.UploadPages(new MemoryStream(data), 0, (PageBlobUploadPagesOptions?)null);

        var bc = (FileBlobClient)cc.GetBlobClient("blob.bin");
        var downloaded = bc.DownloadContent().Value.Content.ToArray();
        Assert.That(downloaded[..512], Is.EqualTo(data));
    }

    [Test]
    public void UploadPages_AtOffset()
    {
        var (cc, pb) = CreatePageBlob("page-offset", "blob.bin", 4096);

        var data = new byte[512];
        Array.Fill(data, (byte)0xCD);
        pb.UploadPages(new MemoryStream(data), 512, (PageBlobUploadPagesOptions?)null);

        var bc = (FileBlobClient)cc.GetBlobClient("blob.bin");
        var downloaded = bc.DownloadContent().Value.Content.ToArray();

        // First 512 bytes should be zeros
        Assert.That(downloaded[..512], Is.EqualTo(new byte[512]));
        // Bytes 512-1023 should be our data
        Assert.That(downloaded[512..1024], Is.EqualTo(data));
    }

    [Test]
    public void ClearPages_ZeroesData()
    {
        var (cc, pb) = CreatePageBlob("page-clear", "blob.bin", 4096);

        var data = new byte[1024];
        Array.Fill(data, (byte)0xFF);
        pb.UploadPages(new MemoryStream(data), 0, (PageBlobUploadPagesOptions?)null);

        pb.ClearPages(new HttpRange(0, 512));

        var bc = (FileBlobClient)cc.GetBlobClient("blob.bin");
        var downloaded = bc.DownloadContent().Value.Content.ToArray();

        // First 512 bytes should be zeroed
        Assert.That(downloaded[..512], Is.EqualTo(new byte[512]));
        // Bytes 512-1023 should still have data
        var expected = new byte[512];
        Array.Fill(expected, (byte)0xFF);
        Assert.That(downloaded[512..1024], Is.EqualTo(expected));
    }

    [Test]
    public void GetPageRanges_ReturnsWrittenRanges()
    {
        var (_, pb) = CreatePageBlob("page-ranges", "blob.bin", 4096);

        pb.UploadPages(new MemoryStream(new byte[512]), 0, (PageBlobUploadPagesOptions?)null);
        pb.UploadPages(new MemoryStream(new byte[512]), 1024, (PageBlobUploadPagesOptions?)null);

        var ranges = pb.GetPageRanges().Value;

        Assert.That(ranges.PageRanges.Count(), Is.GreaterThanOrEqualTo(2));
    }

    [Test]
    public void Resize_ChangesFileSize()
    {
        var (cc, pb) = CreatePageBlob("page-resize", "blob.bin", 1024);

        pb.Resize(4096);

        var blobPath = Path.Combine(cc.ContainerPath, "blob.bin");
        Assert.That(new FileInfo(blobPath).Length, Is.EqualTo(4096));
    }

    [Test]
    public void GetProperties_ReturnsBlobTypePage()
    {
        var (_, pb) = CreatePageBlob("page-props", "blob.bin", 4096);

        var props = pb.GetProperties().Value;

        Assert.That(props.BlobType, Is.EqualTo(BlobType.Page));
    }
}
