using System.Net;
using System.Text;
using System.Xml.Linq;

namespace Iciclecreek.Azure.Storage.Server.Tests;

[TestFixture]
public class BlobControllerTests
{
    private HttpClient Http => StorageServerFixture.BlobHttp;
    private const string Account = StorageServerFixture.AccountName;

    private async Task AssertSuccess(HttpResponseMessage resp, HttpStatusCode expected)
    {
        if (resp.StatusCode != expected)
        {
            var body = await resp.Content.ReadAsStringAsync();
            Assert.Fail($"Expected {expected} but got {resp.StatusCode}: {body}");
        }
    }

    // ── Container CRUD ──────────────────────────────────────────────────

    [Test]
    public async Task CreateContainer_Returns201()
    {
        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/testcontainer-create?restype=container"));
        await AssertSuccess(resp, HttpStatusCode.Created);
    }

    [Test]
    public async Task DeleteContainer_Returns202()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/testcontainer-delete?restype=container"));

        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Delete,
            $"/{Account}/testcontainer-delete?restype=container"));
        await AssertSuccess(resp, HttpStatusCode.Accepted);
    }

    [Test]
    public async Task GetContainerProperties_Returns200_WithETag()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/testcontainer-props?restype=container"));

        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Head,
            $"/{Account}/testcontainer-props?restype=container"));
        await AssertSuccess(resp, HttpStatusCode.OK);
        Assert.That(resp.Headers.Contains("ETag"), Is.True);
    }

    [Test]
    public async Task ListContainers_ReturnsXml_WithContainerNames()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/listtest-container1?restype=container"));
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/listtest-container2?restype=container"));

        var resp = await Http.GetAsync($"/{Account}?comp=list");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var names = doc.Descendants("Name").Select(e => e.Value).ToList();

        Assert.That(names, Does.Contain("listtest-container1"));
        Assert.That(names, Does.Contain("listtest-container2"));
    }

    [Test]
    public async Task ListContainers_WithPrefix_FiltersResults()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/prefixtest-aaa?restype=container"));
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/prefixtest-bbb?restype=container"));
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/other-ccc?restype=container"));

        var resp = await Http.GetAsync($"/{Account}?comp=list&prefix=prefixtest-");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var names = doc.Descendants("Name").Select(e => e.Value).ToList();

        Assert.That(names, Does.Contain("prefixtest-aaa"));
        Assert.That(names, Does.Contain("prefixtest-bbb"));
        Assert.That(names, Does.Not.Contain("other-ccc"));
    }

    // ── Blob Upload / Download ──────────────────────────────────────────

    [Test]
    public async Task PutBlob_And_GetBlob_Roundtrip()
    {
        var containerName = "blob-roundtrip";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var content = "Hello, Azure Storage REST!"u8.ToArray();
        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/greeting.txt")
        {
            Content = new ByteArrayContent(content)
        };
        putReq.Content.Headers.ContentType = new("text/plain");
        putReq.Headers.Add("x-ms-blob-content-type", "text/plain");

        var putResp = await Http.SendAsync(putReq);
        await AssertSuccess(putResp, HttpStatusCode.Created);
        Assert.That(putResp.Headers.Contains("ETag"), Is.True);

        var getResp = await Http.GetAsync($"/{Account}/{containerName}/greeting.txt");
        await AssertSuccess(getResp, HttpStatusCode.OK);

        var downloaded = await getResp.Content.ReadAsByteArrayAsync();
        Assert.That(downloaded, Is.EqualTo(content));
        Assert.That(getResp.Content.Headers.ContentType?.MediaType, Is.EqualTo("text/plain"));
    }

    [Test]
    public async Task PutBlob_LargeContent_Roundtrips()
    {
        var containerName = "blob-large";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var content = new byte[64 * 1024];
        Random.Shared.NextBytes(content);

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/large.bin")
        {
            Content = new ByteArrayContent(content)
        };
        await Http.SendAsync(putReq);

        var downloaded = await Http.GetByteArrayAsync($"/{Account}/{containerName}/large.bin");
        Assert.That(downloaded, Is.EqualTo(content));
    }

    [Test]
    public async Task GetBlobProperties_ReturnsHeaders()
    {
        var containerName = "blob-props";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/myfile.txt")
        {
            Content = new StringContent("test content", Encoding.UTF8, "text/plain")
        };
        putReq.Headers.Add("x-ms-blob-content-type", "text/plain");
        await Http.SendAsync(putReq);

        var headResp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Head,
            $"/{Account}/{containerName}/myfile.txt"));
        await AssertSuccess(headResp, HttpStatusCode.OK);
        Assert.That(headResp.Headers.Contains("ETag"), Is.True);
        Assert.That(headResp.Content.Headers.ContentLength, Is.GreaterThan(0));
    }

    [Test]
    public async Task DeleteBlob_Returns202()
    {
        var containerName = "blob-delete";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/todelete.txt")
        {
            Content = new StringContent("delete me")
        };
        await Http.SendAsync(putReq);

        var delResp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Delete,
            $"/{Account}/{containerName}/todelete.txt"));
        await AssertSuccess(delResp, HttpStatusCode.Accepted);

        var getResp = await Http.GetAsync($"/{Account}/{containerName}/todelete.txt");
        Assert.That(getResp.StatusCode, Is.EqualTo(HttpStatusCode.NotFound));
    }

    // ── Blob Listing ────────────────────────────────────────────────────

    [Test]
    public async Task ListBlobs_ReturnsXml_WithBlobNames()
    {
        var containerName = "blob-listing";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        for (int i = 1; i <= 3; i++)
        {
            var req = new HttpRequestMessage(HttpMethod.Put,
                $"/{Account}/{containerName}/file{i}.txt")
            {
                Content = new StringContent($"content {i}")
            };
            await Http.SendAsync(req);
        }

        var resp = await Http.GetAsync(
            $"/{Account}/{containerName}?restype=container&comp=list");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var names = doc.Descendants("Name").Select(e => e.Value).ToList();

        Assert.That(names, Does.Contain("file1.txt"));
        Assert.That(names, Does.Contain("file2.txt"));
        Assert.That(names, Does.Contain("file3.txt"));
    }

    [Test]
    public async Task ListBlobs_WithPrefix_FiltersResults()
    {
        var containerName = "blob-prefix";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/logs/a.txt") { Content = new StringContent("a") });
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/logs/b.txt") { Content = new StringContent("b") });
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/data/c.txt") { Content = new StringContent("c") });

        var resp = await Http.GetAsync(
            $"/{Account}/{containerName}?restype=container&comp=list&prefix=logs/");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var names = doc.Descendants("Name").Select(e => e.Value).ToList();

        Assert.That(names.Count, Is.EqualTo(2));
        Assert.That(names, Has.All.StartWith("logs/"));
    }

    // ── Blob Metadata ───────────────────────────────────────────────────

    [Test]
    public async Task PutBlob_WithMetadata_RoundtripsViaDownload()
    {
        var containerName = "blob-meta";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/metafile.txt")
        {
            Content = new StringContent("meta test")
        };
        putReq.Headers.Add("x-ms-meta-author", "TestUser");
        putReq.Headers.Add("x-ms-meta-version", "42");
        await Http.SendAsync(putReq);

        var getResp = await Http.GetAsync($"/{Account}/{containerName}/metafile.txt");
        await AssertSuccess(getResp, HttpStatusCode.OK);
        Assert.That(getResp.Headers.Contains("x-ms-meta-author"), Is.True);
        Assert.That(getResp.Headers.GetValues("x-ms-meta-author").First(), Is.EqualTo("TestUser"));
    }

    // ── Block Blob Operations ───────────────────────────────────────────

    [Test]
    public async Task BlockBlob_StageAndCommit_Roundtrip()
    {
        var containerName = "blob-blocks";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var block1Id = Convert.ToBase64String(Encoding.UTF8.GetBytes("0001"));
        var block2Id = Convert.ToBase64String(Encoding.UTF8.GetBytes("0002"));

        // Stage blocks
        var stage1 = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/assembled.bin?comp=block&blockid={Uri.EscapeDataString(block1Id)}")
        {
            Content = new ByteArrayContent("AAAA"u8.ToArray())
        });
        await AssertSuccess(stage1, HttpStatusCode.Created);

        var stage2 = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/assembled.bin?comp=block&blockid={Uri.EscapeDataString(block2Id)}")
        {
            Content = new ByteArrayContent("BBBB"u8.ToArray())
        });
        await AssertSuccess(stage2, HttpStatusCode.Created);

        // Commit block list
        var blockListXml = $@"<?xml version=""1.0"" encoding=""utf-8""?>
<BlockList>
  <Latest>{block1Id}</Latest>
  <Latest>{block2Id}</Latest>
</BlockList>";

        var commitReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/assembled.bin?comp=blocklist")
        {
            Content = new StringContent(blockListXml, Encoding.UTF8, "application/xml")
        };
        var commitResp = await Http.SendAsync(commitReq);
        await AssertSuccess(commitResp, HttpStatusCode.Created);

        // Download and verify
        var downloaded = await Http.GetStringAsync($"/{Account}/{containerName}/assembled.bin");
        Assert.That(downloaded, Is.EqualTo("AAAABBBB"));
    }

    [Test]
    public async Task GetBlockList_ReturnsCommittedBlocks()
    {
        var containerName = "blob-blocklist";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var blockId = Convert.ToBase64String(Encoding.UTF8.GetBytes("b001"));
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/blocked.bin?comp=block&blockid={Uri.EscapeDataString(blockId)}")
        {
            Content = new ByteArrayContent("DATA"u8.ToArray())
        });

        var blockListXml = $"<BlockList><Latest>{blockId}</Latest></BlockList>";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/blocked.bin?comp=blocklist")
        {
            Content = new StringContent(blockListXml, Encoding.UTF8, "application/xml")
        });

        var resp = await Http.GetAsync(
            $"/{Account}/{containerName}/blocked.bin?comp=blocklist");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var committedBlocks = doc.Descendants("CommittedBlocks").Descendants("Block").ToList();
        Assert.That(committedBlocks.Count, Is.EqualTo(1));
    }

    // ── Response Headers ────────────────────────────────────────────────

    [Test]
    public async Task AllResponses_Include_StorageHeaders()
    {
        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/header-test?restype=container"));

        Assert.That(resp.Headers.Contains("x-ms-version"), Is.True);
        Assert.That(resp.Headers.Contains("x-ms-request-id"), Is.True);
        Assert.That(resp.Headers.Contains("Date"), Is.True);
    }

    // ── Hierarchical Blob Names ─────────────────────────────────────────

    [Test]
    public async Task HierarchicalBlobNames_UploadAndDownload()
    {
        var containerName = "blob-hierarchy";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/path/to/deep/file.json")
        {
            Content = new StringContent("{\"key\":\"value\"}")
        };
        putReq.Headers.Add("x-ms-blob-content-type", "application/json");
        await Http.SendAsync(putReq);

        var getResp = await Http.GetAsync(
            $"/{Account}/{containerName}/path/to/deep/file.json");
        await AssertSuccess(getResp, HttpStatusCode.OK);
        var body = await getResp.Content.ReadAsStringAsync();
        Assert.That(body, Is.EqualTo("{\"key\":\"value\"}"));
    }

    // ── Overwrite Blob ──────────────────────────────────────────────────

    [Test]
    public async Task PutBlob_Overwrite_UpdatesContent()
    {
        var containerName = "blob-overwrite";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/mutable.txt")
        { Content = new StringContent("version 1") });

        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/mutable.txt")
        { Content = new StringContent("version 2") });

        var body = await Http.GetStringAsync($"/{Account}/{containerName}/mutable.txt");
        Assert.That(body, Is.EqualTo("version 2"));
    }

    // ── Untested Existing Endpoints ────────────────────────────────────

    [Test]
    public async Task SetBlobProperties_Updates_ContentType()
    {
        var containerName = "blob-setprops";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/propfile.txt")
        {
            Content = new StringContent("property test")
        };
        putReq.Headers.Add("x-ms-blob-content-type", "text/plain");
        await Http.SendAsync(putReq);

        var setPropsReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/propfile.txt?comp=properties");
        setPropsReq.Headers.Add("x-ms-blob-content-type", "application/octet-stream");
        var setPropsResp = await Http.SendAsync(setPropsReq);
        await AssertSuccess(setPropsResp, HttpStatusCode.OK);

        var headResp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Head,
            $"/{Account}/{containerName}/propfile.txt"));
        await AssertSuccess(headResp, HttpStatusCode.OK);
        Assert.That(headResp.Content.Headers.ContentType?.MediaType,
            Is.EqualTo("application/octet-stream"));
    }

    [Test]
    public async Task SetBlobMetadata_Updates_Metadata()
    {
        var containerName = "blob-setmeta";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/metaupdate.txt")
        {
            Content = new StringContent("metadata update test")
        };
        await Http.SendAsync(putReq);

        var setMetaReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/metaupdate.txt?comp=metadata");
        setMetaReq.Headers.Add("x-ms-meta-mykey", "myvalue");
        var setMetaResp = await Http.SendAsync(setMetaReq);
        await AssertSuccess(setMetaResp, HttpStatusCode.OK);

        var getResp = await Http.GetAsync($"/{Account}/{containerName}/metaupdate.txt");
        await AssertSuccess(getResp, HttpStatusCode.OK);
        Assert.That(getResp.Headers.Contains("x-ms-meta-mykey"), Is.True);
        Assert.That(getResp.Headers.GetValues("x-ms-meta-mykey").First(),
            Is.EqualTo("myvalue"));
    }

    [Test]
    public async Task SetContainerMetadata_Updates_Metadata()
    {
        var containerName = "container-setmeta";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var setMetaReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container&comp=metadata");
        setMetaReq.Headers.Add("x-ms-meta-env", "testing");
        var resp = await Http.SendAsync(setMetaReq);
        await AssertSuccess(resp, HttpStatusCode.OK);
    }

    // ── Tags ────────────────────────────────────────────────────────────

    [Test]
    public async Task SetAndGetTags_Roundtrip()
    {
        var containerName = "blob-tags";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/tagged.txt")
        {
            Content = new StringContent("tag test")
        };
        await Http.SendAsync(putReq);

        var tagsXml = "<Tags><TagSet><Tag><Key>env</Key><Value>prod</Value></Tag></TagSet></Tags>";
        var setTagsReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/tagged.txt?comp=tags")
        {
            Content = new StringContent(tagsXml, Encoding.UTF8, "application/xml")
        };
        var setTagsResp = await Http.SendAsync(setTagsReq);
        await AssertSuccess(setTagsResp, HttpStatusCode.NoContent);

        var getTagsResp = await Http.GetAsync(
            $"/{Account}/{containerName}/tagged.txt?comp=tags");
        await AssertSuccess(getTagsResp, HttpStatusCode.OK);

        var xml = await getTagsResp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var tag = doc.Descendants("Tag").First();
        Assert.That(tag.Element("Key")?.Value, Is.EqualTo("env"));
        Assert.That(tag.Element("Value")?.Value, Is.EqualTo("prod"));
    }

    // ── Snapshot ────────────────────────────────────────────────────────

    [Test]
    public async Task CreateSnapshot_Returns201_WithSnapshotHeader()
    {
        var containerName = "blob-snapshot";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/snapme.txt")
        {
            Content = new StringContent("snapshot test")
        };
        await Http.SendAsync(putReq);

        var snapReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/snapme.txt?comp=snapshot");
        var snapResp = await Http.SendAsync(snapReq);
        await AssertSuccess(snapResp, HttpStatusCode.Created);
        Assert.That(snapResp.Headers.Contains("x-ms-snapshot"), Is.True);
    }

    // ── Lease ───────────────────────────────────────────────────────────

    [Test]
    public async Task BlobLease_AcquireAndRelease()
    {
        var containerName = "blob-lease";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        var putReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/leased.txt")
        {
            Content = new StringContent("lease test")
        };
        await Http.SendAsync(putReq);

        // Acquire lease
        var acquireReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/leased.txt?comp=lease");
        acquireReq.Headers.Add("x-ms-lease-action", "acquire");
        acquireReq.Headers.Add("x-ms-lease-duration", "30");
        var acquireResp = await Http.SendAsync(acquireReq);
        await AssertSuccess(acquireResp, HttpStatusCode.Created);
        Assert.That(acquireResp.Headers.Contains("x-ms-lease-id"), Is.True);

        var leaseId = acquireResp.Headers.GetValues("x-ms-lease-id").First();

        // Release lease
        var releaseReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/leased.txt?comp=lease");
        releaseReq.Headers.Add("x-ms-lease-action", "release");
        releaseReq.Headers.Add("x-ms-lease-id", leaseId);
        var releaseResp = await Http.SendAsync(releaseReq);
        await AssertSuccess(releaseResp, HttpStatusCode.OK);
    }

    [Test]
    public async Task ContainerLease_AcquireAndRelease()
    {
        var containerName = "container-lease";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        // Acquire lease
        var acquireReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container&comp=lease");
        acquireReq.Headers.Add("x-ms-lease-action", "acquire");
        acquireReq.Headers.Add("x-ms-lease-duration", "-1");
        var acquireResp = await Http.SendAsync(acquireReq);
        await AssertSuccess(acquireResp, HttpStatusCode.Created);
        Assert.That(acquireResp.Headers.Contains("x-ms-lease-id"), Is.True);

        var leaseId = acquireResp.Headers.GetValues("x-ms-lease-id").First();

        // Release lease
        var releaseReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container&comp=lease");
        releaseReq.Headers.Add("x-ms-lease-action", "release");
        releaseReq.Headers.Add("x-ms-lease-id", leaseId);
        var releaseResp = await Http.SendAsync(releaseReq);
        await AssertSuccess(releaseResp, HttpStatusCode.OK);
    }

    // ── AppendBlob ──────────────────────────────────────────────────────

    [Test]
    public async Task AppendBlob_CreateAndAppend()
    {
        var containerName = "blob-append";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        // Create empty append blob
        var createReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/appended.txt")
        {
            Content = new ByteArrayContent(Array.Empty<byte>())
        };
        createReq.Headers.Add("x-ms-blob-type", "AppendBlob");
        var createResp = await Http.SendAsync(createReq);
        await AssertSuccess(createResp, HttpStatusCode.Created);

        // Append chunk1
        var append1 = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/appended.txt?comp=appendblock")
        {
            Content = new ByteArrayContent("chunk1"u8.ToArray())
        };
        var append1Resp = await Http.SendAsync(append1);
        await AssertSuccess(append1Resp, HttpStatusCode.Created);

        // Append chunk2
        var append2 = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/appended.txt?comp=appendblock")
        {
            Content = new ByteArrayContent("chunk2"u8.ToArray())
        };
        var append2Resp = await Http.SendAsync(append2);
        await AssertSuccess(append2Resp, HttpStatusCode.Created);

        // Verify content
        var body = await Http.GetStringAsync($"/{Account}/{containerName}/appended.txt");
        Assert.That(body, Is.EqualTo("chunk1chunk2"));
    }

    // ── Page Blob ───────────────────────────────────────────────────────

    [Test]
    public async Task PageBlob_CreateAndUploadPages()
    {
        var containerName = "blob-page";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        // Create empty page blob
        var createReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/paged.bin")
        {
            Content = new ByteArrayContent(Array.Empty<byte>())
        };
        createReq.Headers.Add("x-ms-blob-type", "PageBlob");
        createReq.Headers.Add("x-ms-blob-content-length", "1024");
        var createResp = await Http.SendAsync(createReq);
        await AssertSuccess(createResp, HttpStatusCode.Created);

        // Upload a 512-byte page
        var pageData = new byte[512];
        for (int i = 0; i < pageData.Length; i++)
            pageData[i] = (byte)(i % 256);

        var uploadReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/paged.bin?comp=page")
        {
            Content = new ByteArrayContent(pageData)
        };
        uploadReq.Headers.Add("x-ms-page-write", "update");
        uploadReq.Headers.Add("x-ms-range", "bytes=0-511");
        var uploadResp = await Http.SendAsync(uploadReq);
        await AssertSuccess(uploadResp, HttpStatusCode.Created);

        // Download and verify first 512 bytes
        var getResp = await Http.GetAsync($"/{Account}/{containerName}/paged.bin");
        await AssertSuccess(getResp, HttpStatusCode.OK);
        var downloaded = await getResp.Content.ReadAsByteArrayAsync();
        Assert.That(downloaded.Take(512).ToArray(), Is.EqualTo(pageData));
    }

    [Test]
    public async Task PageBlob_GetPageRanges()
    {
        var containerName = "blob-pageranges";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}?restype=container"));

        // Create page blob
        var createReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/ranged.bin")
        {
            Content = new ByteArrayContent(Array.Empty<byte>())
        };
        createReq.Headers.Add("x-ms-blob-type", "PageBlob");
        createReq.Headers.Add("x-ms-blob-content-length", "1024");
        await Http.SendAsync(createReq);

        // Upload a page
        var pageData = new byte[512];
        Array.Fill<byte>(pageData, 0xAB);
        var uploadReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{containerName}/ranged.bin?comp=page")
        {
            Content = new ByteArrayContent(pageData)
        };
        uploadReq.Headers.Add("x-ms-page-write", "update");
        uploadReq.Headers.Add("x-ms-range", "bytes=0-511");
        await Http.SendAsync(uploadReq);

        // Get page ranges
        var resp = await Http.GetAsync(
            $"/{Account}/{containerName}/ranged.bin?comp=pagelist");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var pageRanges = doc.Descendants("PageRange").ToList();
        Assert.That(pageRanges.Count, Is.GreaterThan(0));
    }
}
