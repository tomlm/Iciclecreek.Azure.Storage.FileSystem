using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Tests.Infrastructure;

namespace Iciclecreek.Azure.Storage.FileSystem.Tests.Blobs;

public class BlockBlobTests
{
    private TempRoot _root = null!;

    [SetUp]
    public void Setup() => _root = new TempRoot();

    [TearDown]
    public void TearDown() => _root.Dispose();

    [Test]
    public void StageBlock_And_CommitBlockList_Roundtrip()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "blocks");
        container.CreateIfNotExists();

        var client = container.GetFileBlockBlobClient("assembled.bin");

        var block1Id = Convert.ToBase64String("0001"u8.ToArray());
        var block2Id = Convert.ToBase64String("0002"u8.ToArray());

        client.StageBlock(block1Id, new MemoryStream("Hello, "u8.ToArray()));
        client.StageBlock(block2Id, new MemoryStream("World!"u8.ToArray()));

        client.CommitBlockList(new[] { block1Id, block2Id });

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public void GetBlockList_Shows_Committed_And_Uncommitted()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "blocklist");
        container.CreateIfNotExists();

        var client = container.GetFileBlockBlobClient("partial.bin");

        var b1 = Convert.ToBase64String("b001"u8.ToArray());
        var b2 = Convert.ToBase64String("b002"u8.ToArray());
        var b3 = Convert.ToBase64String("b003"u8.ToArray());

        client.StageBlock(b1, new MemoryStream(new byte[10]));
        client.StageBlock(b2, new MemoryStream(new byte[20]));
        client.CommitBlockList(new[] { b1, b2 });

        // Stage a third block but don't commit.
        client.StageBlock(b3, new MemoryStream(new byte[30]));

        var blockList = client.GetBlockList(BlockListTypes.All);
        Assert.That(blockList.Value.CommittedBlocks.Count(), Is.EqualTo(2));
        Assert.That(blockList.Value.UncommittedBlocks.Count(), Is.EqualTo(1));
    }

    [Test]
    public void SingleShot_Upload_Works_On_BlockBlobClient()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "single");
        container.CreateIfNotExists();

        var client = container.GetFileBlockBlobClient("quick.txt");
        client.Upload(new MemoryStream("quick upload"u8.ToArray()), new BlobUploadOptions());

        var content = client.DownloadContent().Value.Content.ToString();
        Assert.That(content, Is.EqualTo("quick upload"));
    }

    [Test]
    public async Task StageBlock_And_CommitBlockList_Roundtrip_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "blocks");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileBlockBlobClient("assembled.bin");

        var block1Id = Convert.ToBase64String("0001"u8.ToArray());
        var block2Id = Convert.ToBase64String("0002"u8.ToArray());

        await client.StageBlockAsync(block1Id, new MemoryStream("Hello, "u8.ToArray()));
        await client.StageBlockAsync(block2Id, new MemoryStream("World!"u8.ToArray()));

        await client.CommitBlockListAsync(new[] { block1Id, block2Id });

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public async Task GetBlockList_Shows_Committed_And_Uncommitted_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "blocklist");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileBlockBlobClient("partial.bin");

        var b1 = Convert.ToBase64String("b001"u8.ToArray());
        var b2 = Convert.ToBase64String("b002"u8.ToArray());
        var b3 = Convert.ToBase64String("b003"u8.ToArray());

        await client.StageBlockAsync(b1, new MemoryStream(new byte[10]));
        await client.StageBlockAsync(b2, new MemoryStream(new byte[20]));
        await client.CommitBlockListAsync(new[] { b1, b2 });

        // Stage a third block but don't commit.
        await client.StageBlockAsync(b3, new MemoryStream(new byte[30]));

        var blockList = await client.GetBlockListAsync(BlockListTypes.All);
        Assert.That(blockList.Value.CommittedBlocks.Count(), Is.EqualTo(2));
        Assert.That(blockList.Value.UncommittedBlocks.Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task SingleShot_Upload_Works_On_BlockBlobClient_Async()
    {
        var container = FileBlobContainerClient.FromAccount(_root.Account, "single");
        await container.CreateIfNotExistsAsync();

        var client = container.GetFileBlockBlobClient("quick.txt");
        await client.UploadAsync(new MemoryStream("quick upload"u8.ToArray()), new BlobUploadOptions());

        var content = (await client.DownloadContentAsync()).Value.Content.ToString();
        Assert.That(content, Is.EqualTo("quick upload"));
    }
}
