using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Iciclecreek.Azure.Storage.Tests.Shared.Infrastructure;
using NUnit.Framework;

namespace Iciclecreek.Azure.Storage.Tests.Shared.Blobs;

[TestFixture]
public abstract class BlockBlobTestsBase
{
    protected StorageTestFixture _fixture = null!;

    protected abstract StorageTestFixture CreateFixture();

    [SetUp]
    public void SetUp() => _fixture = CreateFixture();

    [TearDown]
    public void TearDown() => _fixture.Dispose();

    // ── StageBlock_And_CommitBlockList_Roundtrip ───────────────────────

    [Test]
    public void StageBlock_And_CommitBlockList_Roundtrip()
    {
        var container = _fixture.CreateBlobContainerClient("blocks");
        container.CreateIfNotExists();
        var client = _fixture.CreateBlockBlobClient(container, "assembled.bin");

        var blockId1 = Convert.ToBase64String("0001"u8.ToArray());
        var blockId2 = Convert.ToBase64String("0002"u8.ToArray());

        client.StageBlock(blockId1, new MemoryStream("Hello, "u8.ToArray()));
        client.StageBlock(blockId2, new MemoryStream("World!"u8.ToArray()));

        client.CommitBlockList(new[] { blockId1, blockId2 });

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    [Test]
    public async Task StageBlock_And_CommitBlockList_Roundtrip_Async()
    {
        var container = _fixture.CreateBlobContainerClient("blocks-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateBlockBlobClient(container, "assembled.bin");

        var blockId1 = Convert.ToBase64String("0001"u8.ToArray());
        var blockId2 = Convert.ToBase64String("0002"u8.ToArray());

        await client.StageBlockAsync(blockId1, new MemoryStream("Hello, "u8.ToArray()));
        await client.StageBlockAsync(blockId2, new MemoryStream("World!"u8.ToArray()));

        await client.CommitBlockListAsync(new[] { blockId1, blockId2 });

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("Hello, World!"));
    }

    // ── GetBlockList_Shows_Committed_And_Uncommitted ───────────────────

    [Test]
    public void GetBlockList_Shows_Committed_And_Uncommitted()
    {
        var container = _fixture.CreateBlobContainerClient("blocklist");
        container.CreateIfNotExists();
        var client = _fixture.CreateBlockBlobClient(container, "blocklist.bin");

        var committedId = Convert.ToBase64String("0001"u8.ToArray());
        var uncommittedId = Convert.ToBase64String("0002"u8.ToArray());

        client.StageBlock(committedId, new MemoryStream("committed"u8.ToArray()));
        client.CommitBlockList(new[] { committedId });

        client.StageBlock(uncommittedId, new MemoryStream("uncommitted"u8.ToArray()));

        var blockList = client.GetBlockList(BlockListTypes.All);
        Assert.That(blockList.Value.CommittedBlocks.Count(), Is.EqualTo(1));
        Assert.That(blockList.Value.UncommittedBlocks.Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task GetBlockList_Shows_Committed_And_Uncommitted_Async()
    {
        var container = _fixture.CreateBlobContainerClient("blocklist-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateBlockBlobClient(container, "blocklist.bin");

        var committedId = Convert.ToBase64String("0001"u8.ToArray());
        var uncommittedId = Convert.ToBase64String("0002"u8.ToArray());

        await client.StageBlockAsync(committedId, new MemoryStream("committed"u8.ToArray()));
        await client.CommitBlockListAsync(new[] { committedId });

        await client.StageBlockAsync(uncommittedId, new MemoryStream("uncommitted"u8.ToArray()));

        var blockList = await client.GetBlockListAsync(BlockListTypes.All);
        Assert.That(blockList.Value.CommittedBlocks.Count(), Is.EqualTo(1));
        Assert.That(blockList.Value.UncommittedBlocks.Count(), Is.EqualTo(1));
    }

    // ── SingleShot_Upload_Works_On_BlockBlobClient ─────────────────────

    [Test]
    public void SingleShot_Upload_Works_On_BlockBlobClient()
    {
        var container = _fixture.CreateBlobContainerClient("singleshot");
        container.CreateIfNotExists();
        var client = _fixture.CreateBlockBlobClient(container, "quick.bin");

        client.Upload(new MemoryStream("quick upload"u8.ToArray()), new BlobUploadOptions());

        var downloaded = client.DownloadContent();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("quick upload"));
    }

    [Test]
    public async Task SingleShot_Upload_Works_On_BlockBlobClient_Async()
    {
        var container = _fixture.CreateBlobContainerClient("singleshot-async");
        await container.CreateIfNotExistsAsync();
        var client = _fixture.CreateBlockBlobClient(container, "quick.bin");

        await client.UploadAsync(new MemoryStream("quick upload"u8.ToArray()), new BlobUploadOptions());

        var downloaded = await client.DownloadContentAsync();
        Assert.That(downloaded.Value.Content.ToString(), Is.EqualTo("quick upload"));
    }
}
