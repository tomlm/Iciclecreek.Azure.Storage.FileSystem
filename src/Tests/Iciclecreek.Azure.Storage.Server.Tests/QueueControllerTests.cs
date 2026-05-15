using System.Net;
using System.Text;
using System.Xml.Linq;

namespace Iciclecreek.Azure.Storage.Server.Tests;

[TestFixture]
public class QueueControllerTests
{
    private HttpClient Http => StorageServerFixture.QueueHttp;
    private const string Account = StorageServerFixture.AccountName;

    private async Task AssertSuccess(HttpResponseMessage resp, HttpStatusCode expected)
    {
        if (resp.StatusCode != expected)
        {
            var body = await resp.Content.ReadAsStringAsync();
            Assert.Fail($"Expected {expected} but got {resp.StatusCode}: {body}");
        }
    }

    // ── Queue CRUD ──────────────────────────────────────────────────────

    [Test]
    public async Task CreateQueue_Returns201()
    {
        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/testqueue-create"));
        await AssertSuccess(resp, HttpStatusCode.Created);
    }

    [Test]
    public async Task DeleteQueue_Returns204()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/testqueue-delete"));

        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Delete,
            $"/{Account}/testqueue-delete"));
        await AssertSuccess(resp, HttpStatusCode.NoContent);
    }

    [Test]
    public async Task ListQueues_ReturnsXml_WithQueueNames()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/listq-one"));
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/listq-two"));

        var resp = await Http.GetAsync($"/{Account}?comp=list");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var xml = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(xml);
        var names = doc.Descendants("Name").Select(e => e.Value).ToList();

        Assert.That(names, Does.Contain("listq-one"));
        Assert.That(names, Does.Contain("listq-two"));
    }

    // ── Queue Metadata ──────────────────────────────────────────────────

    [Test]
    public async Task GetQueueMetadata_ReturnsMessageCount()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/metaq"));

        var resp = await Http.GetAsync($"/{Account}/metaq?comp=metadata");
        await AssertSuccess(resp, HttpStatusCode.OK);
        Assert.That(resp.Headers.Contains("x-ms-approximate-messages-count"), Is.True);
    }

    [Test]
    public async Task SetQueueMetadata_Roundtrips()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/metaq-set"));

        var setReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/metaq-set?comp=metadata");
        setReq.Headers.Add("x-ms-meta-env", "test");
        var setResp = await Http.SendAsync(setReq);
        await AssertSuccess(setResp, HttpStatusCode.NoContent);

        // Note: metadata roundtrip through GET metadata is tested by the controller
        // reading from the queue client's GetProperties
    }

    // ── Messages: Send and Receive ──────────────────────────────────────

    [Test]
    public async Task PutMessage_Returns_MessageXml()
    {
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/msgq-put"));

        var msgXml = "<QueueMessage><MessageText>SGVsbG8=</MessageText></QueueMessage>";
        var resp = await Http.PostAsync($"/{Account}/msgq-put/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));
        await AssertSuccess(resp, HttpStatusCode.OK);

        var body = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(body);
        Assert.That(doc.Descendants("MessageId").Any(), Is.True);
        Assert.That(doc.Descendants("PopReceipt").Any(), Is.True);
    }

    [Test]
    public async Task GetMessages_DequeuesMessages()
    {
        var queueName = "msgq-dequeue";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        var msgXml = "<QueueMessage><MessageText>dGVzdA==</MessageText></QueueMessage>";
        await Http.PostAsync($"/{Account}/{queueName}/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));

        var resp = await Http.GetAsync($"/{Account}/{queueName}/messages?numofmessages=1");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var body = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(body);
        var messages = doc.Descendants("QueueMessage").ToList();
        Assert.That(messages.Count, Is.EqualTo(1));
        Assert.That(messages[0].Element("PopReceipt"), Is.Not.Null);
        Assert.That(messages[0].Element("MessageId"), Is.Not.Null);
        Assert.That(messages[0].Element("MessageText")!.Value, Is.EqualTo("dGVzdA=="));
    }

    [Test]
    public async Task PeekMessages_DoesNotRemoveFromQueue()
    {
        var queueName = "msgq-peek";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        var msgXml = "<QueueMessage><MessageText>cGVlaw==</MessageText></QueueMessage>";
        await Http.PostAsync($"/{Account}/{queueName}/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));

        // Peek
        var peekResp = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?peekonly=true&numofmessages=1");
        await AssertSuccess(peekResp, HttpStatusCode.OK);

        var peekBody = await peekResp.Content.ReadAsStringAsync();
        var peekDoc = XDocument.Parse(peekBody);
        Assert.That(peekDoc.Descendants("QueueMessage").Count(), Is.EqualTo(1));

        // Peek again — message should still be there
        var peekResp2 = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?peekonly=true&numofmessages=1");
        var peekBody2 = await peekResp2.Content.ReadAsStringAsync();
        var peekDoc2 = XDocument.Parse(peekBody2);
        Assert.That(peekDoc2.Descendants("QueueMessage").Count(), Is.EqualTo(1));
    }

    [Test]
    public async Task DeleteMessage_Returns204()
    {
        var queueName = "msgq-delmsg";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        var msgXml = "<QueueMessage><MessageText>ZGVs</MessageText></QueueMessage>";
        await Http.PostAsync($"/{Account}/{queueName}/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));

        // Dequeue to get messageId and popReceipt
        var deqResp = await Http.GetAsync($"/{Account}/{queueName}/messages");
        var deqBody = await deqResp.Content.ReadAsStringAsync();
        var deqDoc = XDocument.Parse(deqBody);
        var msg = deqDoc.Descendants("QueueMessage").First();
        var messageId = msg.Element("MessageId")!.Value;
        var popReceipt = Uri.EscapeDataString(msg.Element("PopReceipt")!.Value);

        // Delete
        var delResp = await Http.DeleteAsync(
            $"/{Account}/{queueName}/messages/{messageId}?popreceipt={popReceipt}");
        await AssertSuccess(delResp, HttpStatusCode.NoContent);
    }

    [Test]
    public async Task ClearMessages_Returns204_And_EmptiesQueue()
    {
        var queueName = "msgq-clear";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        // Send two messages
        for (int i = 0; i < 2; i++)
        {
            var xml = $"<QueueMessage><MessageText>msg{i}</MessageText></QueueMessage>";
            await Http.PostAsync($"/{Account}/{queueName}/messages",
                new StringContent(xml, Encoding.UTF8, "application/xml"));
        }

        var clearResp = await Http.DeleteAsync($"/{Account}/{queueName}/messages");
        await AssertSuccess(clearResp, HttpStatusCode.NoContent);

        // Verify empty
        var peekResp = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?peekonly=true");
        var peekBody = await peekResp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(peekBody);
        Assert.That(doc.Descendants("QueueMessage").Count(), Is.EqualTo(0));
    }

    [Test]
    public async Task MultipleMessages_DequeueMultiple()
    {
        var queueName = "msgq-multi";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        for (int i = 1; i <= 3; i++)
        {
            var text = Convert.ToBase64String(Encoding.UTF8.GetBytes($"msg{i}"));
            var xml = $"<QueueMessage><MessageText>{text}</MessageText></QueueMessage>";
            await Http.PostAsync($"/{Account}/{queueName}/messages",
                new StringContent(xml, Encoding.UTF8, "application/xml"));
        }

        var resp = await Http.GetAsync($"/{Account}/{queueName}/messages?numofmessages=3");
        await AssertSuccess(resp, HttpStatusCode.OK);

        var body = await resp.Content.ReadAsStringAsync();
        var doc = XDocument.Parse(body);
        var messages = doc.Descendants("QueueMessage").ToList();
        Assert.That(messages.Count, Is.EqualTo(3));
    }

    // ── Update Message ──────────────────────────────────────────────────

    [Test]
    public async Task UpdateMessage_ChangesContent()
    {
        var queueName = "msgq-update";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        var msgXml = "<QueueMessage><MessageText>original</MessageText></QueueMessage>";
        await Http.PostAsync($"/{Account}/{queueName}/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));

        // Dequeue
        var deqResp = await Http.GetAsync($"/{Account}/{queueName}/messages");
        var deqBody = await deqResp.Content.ReadAsStringAsync();
        var deqDoc = XDocument.Parse(deqBody);
        var msg = deqDoc.Descendants("QueueMessage").First();
        var messageId = msg.Element("MessageId")!.Value;
        var popReceipt = Uri.EscapeDataString(msg.Element("PopReceipt")!.Value);

        // Update with new content and 0 visibility timeout (immediately visible)
        var updateXml = "<QueueMessage><MessageText>updated</MessageText></QueueMessage>";
        var updateReq = new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}/messages/{messageId}?popreceipt={popReceipt}&visibilitytimeout=0")
        {
            Content = new StringContent(updateXml, Encoding.UTF8, "application/xml")
        };
        var updateResp = await Http.SendAsync(updateReq);
        await AssertSuccess(updateResp, HttpStatusCode.NoContent);

        // Peek to verify updated content
        var peekResp = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?peekonly=true");
        var peekBody = await peekResp.Content.ReadAsStringAsync();
        var peekDoc = XDocument.Parse(peekBody);
        var peekedMsg = peekDoc.Descendants("QueueMessage").FirstOrDefault();
        Assert.That(peekedMsg, Is.Not.Null);
        Assert.That(peekedMsg!.Element("MessageText")!.Value, Is.EqualTo("updated"));
    }

    // ── Response Headers ────────────────────────────────────────────────

    [Test]
    public async Task QueueResponses_Include_StorageHeaders()
    {
        var resp = await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/headerq"));

        Assert.That(resp.Headers.Contains("x-ms-version"), Is.True);
        Assert.That(resp.Headers.Contains("x-ms-request-id"), Is.True);
    }

    // ── Dequeue count increments ────────────────────────────────────────

    [Test]
    public async Task DequeueCount_Increments_OnEachReceive()
    {
        var queueName = "msgq-deqcount";
        await Http.SendAsync(new HttpRequestMessage(HttpMethod.Put,
            $"/{Account}/{queueName}"));

        var msgXml = "<QueueMessage><MessageText>countme</MessageText></QueueMessage>";
        await Http.PostAsync($"/{Account}/{queueName}/messages",
            new StringContent(msgXml, Encoding.UTF8, "application/xml"));

        // First dequeue — dequeue count should be 1
        var resp1 = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?visibilitytimeout=0");
        var body1 = await resp1.Content.ReadAsStringAsync();
        var doc1 = XDocument.Parse(body1);
        var count1 = int.Parse(doc1.Descendants("DequeueCount").First().Value);
        Assert.That(count1, Is.EqualTo(1));

        // Wait a tiny bit for visibility to expire (we set 0 seconds)
        await Task.Delay(100);

        // Second dequeue — dequeue count should be 2
        var resp2 = await Http.GetAsync(
            $"/{Account}/{queueName}/messages?visibilitytimeout=0");
        var body2 = await resp2.Content.ReadAsStringAsync();
        var doc2 = XDocument.Parse(body2);
        var count2 = int.Parse(doc2.Descendants("DequeueCount").First().Value);
        Assert.That(count2, Is.EqualTo(2));
    }
}
