using System.Net;
using Azure;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.Extensions.Configuration;

namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Catches <see cref="RequestFailedException"/> thrown by the Azure SDK clients
/// and converts them into proper Azure Storage error responses (XML or JSON).
/// </summary>
public sealed class StorageExceptionFilter : IExceptionFilter
{
    public void OnException(ExceptionContext context)
    {
        int statusCode;
        string errorCode;
        string message;

        if (context.Exception is RequestFailedException rfe)
        {
            statusCode = rfe.Status;
            errorCode = rfe.ErrorCode ?? "RequestError";
            message = rfe.Message;
        }
        else
        {
            statusCode = 500;
            errorCode = "InternalError";
            message = context.Exception.Message;
        }

        var config = (IConfiguration?)context.HttpContext.RequestServices.GetService(typeof(IConfiguration));
        var tablePort = config?.GetValue<int?>("TablePort") ?? 10002;
        var isTable = context.HttpContext.Connection.LocalPort == tablePort;

        if (isTable)
        {
            context.Result = new ContentResult
            {
                StatusCode = statusCode,
                ContentType = "application/json;odata=minimalmetadata",
                Content = $"{{\"odata.error\":{{\"code\":\"{errorCode}\",\"message\":{{\"value\":\"{Escape(message)}\"}}}}}}"
            };
        }
        else
        {
            context.Result = new ContentResult
            {
                StatusCode = statusCode,
                ContentType = "application/xml",
                Content = $"<?xml version=\"1.0\" encoding=\"utf-8\"?><Error><Code>{errorCode}</Code><Message>{Escape(message)}</Message></Error>"
            };
        }

        context.ExceptionHandled = true;
    }

    private static string Escape(string s)
        => s.Replace("&", "&amp;").Replace("<", "&lt;").Replace(">", "&gt;").Replace("\"", "&quot;");
}
