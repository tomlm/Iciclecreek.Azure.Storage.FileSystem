using Microsoft.AspNetCore.Mvc.ActionConstraints;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Routes requests to controllers based on the local port the request arrived on.
/// The port is read from IConfiguration using the key "{ServiceName}Port" (e.g. "BlobPort"),
/// falling back to the default if not configured.
/// Defaults: Blob = 10000, Queue = 10001, Table = 10002 (Azurite-compatible).
/// </summary>
[AttributeUsage(AttributeTargets.Class, AllowMultiple = false)]
public sealed class ServicePortConstraintAttribute : Attribute, IActionConstraint
{
    private readonly string _serviceName;
    private readonly int _defaultPort;

    /// <param name="serviceName">Config key prefix, e.g. "Blob" reads "BlobPort".</param>
    /// <param name="defaultPort">Fallback port when configuration is absent.</param>
    public ServicePortConstraintAttribute(string serviceName, int defaultPort)
    {
        _serviceName = serviceName;
        _defaultPort = defaultPort;
    }

    public int Order => 0;

    public bool Accept(ActionConstraintContext context)
    {
        var config = context.RouteContext.HttpContext.RequestServices.GetService<IConfiguration>();
        var port = config?.GetValue<int?>($"{_serviceName}Port") ?? _defaultPort;
        return context.RouteContext.HttpContext.Connection.LocalPort == port;
    }
}
