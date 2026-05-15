using Microsoft.AspNetCore.Mvc.ActionConstraints;

namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Constrains an action to match only when a specific query-string parameter
/// is present with an expected value (or is absent).
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public sealed class QueryConstraintAttribute : Attribute, IActionConstraint
{
    private readonly string _param;
    private readonly string? _value;

    /// <summary>Require <paramref name="param"/> to equal <paramref name="value"/>.</summary>
    public QueryConstraintAttribute(string param, string value)
    {
        _param = param;
        _value = value;
    }

    public int Order { get; set; } = 100;

    public bool Accept(ActionConstraintContext context)
    {
        var query = context.RouteContext.HttpContext.Request.Query;
        if (!query.ContainsKey(_param))
            return false;
        return string.Equals(query[_param], _value, StringComparison.OrdinalIgnoreCase);
    }
}

/// <summary>
/// Constrains an action to match only when a query-string parameter is NOT present.
/// </summary>
[AttributeUsage(AttributeTargets.Method, AllowMultiple = true)]
public sealed class QueryAbsentConstraintAttribute : Attribute, IActionConstraint
{
    private readonly string _param;

    public QueryAbsentConstraintAttribute(string param) => _param = param;

    public int Order { get; set; } = 100;

    public bool Accept(ActionConstraintContext context)
        => !context.RouteContext.HttpContext.Request.Query.ContainsKey(_param);
}
