using Iciclecreek.Azure.Storage.Server.Infrastructure;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;

namespace Iciclecreek.Azure.Storage.Server;

/// <summary>
/// Extension methods to register the Azure Storage REST API controllers and middleware.
/// </summary>
public static class StorageServerExtensions
{
    /// <summary>
    /// Adds the Azure Storage REST API controllers and required filters to the service collection.
    /// The caller must also register <c>BlobServiceClient</c>, <c>TableServiceClient</c>,
    /// and/or <c>QueueServiceClient</c> in DI before building the host.
    /// </summary>
    public static IMvcBuilder AddStorageServer(this IServiceCollection services)
    {
        return services
            .AddControllers(options =>
            {
                options.Filters.Add<StorageHeadersFilter>();
                options.Filters.Add<StorageExceptionFilter>();
            })
            .AddApplicationPart(typeof(StorageServerExtensions).Assembly);
    }

    /// <summary>
    /// Maps the Azure Storage REST API controller endpoints.
    /// Call this instead of (or in addition to) <c>app.MapControllers()</c>.
    /// </summary>
    public static WebApplication MapStorageServer(this WebApplication app)
    {
        app.MapControllers();
        return app;
    }
}
