using Azure.Data.Tables;
using Azure.Storage.Blobs;
using Azure.Storage.Queues;
using Iciclecreek.Azure.Storage.FileSystem;
using Iciclecreek.Azure.Storage.FileSystem.Blobs;
using Iciclecreek.Azure.Storage.FileSystem.Queues;
using Iciclecreek.Azure.Storage.FileSystem.Tables;
using Iciclecreek.Azure.Storage.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Iciclecreek.Azure.Storage.Server.Tests;

/// <summary>
/// Starts a real Kestrel-based storage server backed by the FileSystem provider.
/// Shared across all tests in the assembly via [SetUpFixture].
/// </summary>
[SetUpFixture]
public class StorageServerFixture
{
    public const int BlobPort = 17000;
    public const int QueuePort = 17001;
    public const int TablePort = 17002;
    public const string AccountName = "testaccount1";

    private static WebApplication? _app;
    private static string? _storagePath;
    public static HttpClient BlobHttp { get; private set; } = null!;
    public static HttpClient QueueHttp { get; private set; } = null!;
    public static HttpClient TableHttp { get; private set; } = null!;

    [OneTimeSetUp]
    public async Task GlobalSetup()
    {
        _storagePath = Path.Combine(Path.GetTempPath(), $"StorageServerTests_{Guid.NewGuid():N}");
        var provider = new FileStorageProvider(_storagePath);
        var account = provider.AddAccount(AccountName);

        var builder = WebApplication.CreateBuilder();

        builder.Configuration["BlobPort"] = BlobPort.ToString();
        builder.Configuration["QueuePort"] = QueuePort.ToString();
        builder.Configuration["TablePort"] = TablePort.ToString();

        builder.Services.AddSingleton<BlobServiceClient>(
            FileBlobServiceClient.FromAccount(account));
        builder.Services.AddSingleton<QueueServiceClient>(
            FileQueueServiceClient.FromAccount(account));
        builder.Services.AddSingleton<TableServiceClient>(
            FileTableServiceClient.FromAccount(account));

        builder.Services.AddStorageServer();

        builder.WebHost.ConfigureKestrel(k =>
        {
            k.ListenLocalhost(BlobPort);
            k.ListenLocalhost(QueuePort);
            k.ListenLocalhost(TablePort);
        });

        builder.Logging.ClearProviders();
        builder.Logging.AddConsole();
        builder.Logging.SetMinimumLevel(Microsoft.Extensions.Logging.LogLevel.Error);

        _app = builder.Build();
        _app.MapStorageServer();

        await _app.StartAsync();

        BlobHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{BlobPort}") };
        QueueHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{QueuePort}") };
        TableHttp = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{TablePort}") };
    }

    [OneTimeTearDown]
    public async Task GlobalTeardown()
    {
        BlobHttp?.Dispose();
        QueueHttp?.Dispose();
        TableHttp?.Dispose();
        if (_app != null)
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
        if (_storagePath != null && Directory.Exists(_storagePath))
        {
            try { Directory.Delete(_storagePath, true); } catch { }
        }
    }
}
