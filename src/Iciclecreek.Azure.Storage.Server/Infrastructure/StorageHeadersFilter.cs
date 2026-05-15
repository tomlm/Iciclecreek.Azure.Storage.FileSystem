using Microsoft.AspNetCore.Mvc.Filters;

namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Adds the standard Azure Storage response headers to every response.
/// </summary>
public sealed class StorageHeadersFilter : IActionFilter
{
    private const string Version = "2024-11-04";

    public void OnActionExecuting(ActionExecutingContext context) { }

    public void OnActionExecuted(ActionExecutedContext context)
    {
        var headers = context.HttpContext.Response.Headers;
        headers["x-ms-version"] = Version;
        headers["x-ms-request-id"] = Guid.NewGuid().ToString();
        headers["Date"] = DateTimeOffset.UtcNow.ToString("R");
    }
}
