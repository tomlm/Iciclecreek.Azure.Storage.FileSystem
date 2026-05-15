namespace Iciclecreek.Azure.Storage.Server.Infrastructure;

/// <summary>
/// Marker-based pagination for list operations.
/// Items are sorted by key, the marker indicates where to start, maxresults limits page size.
/// </summary>
public static class PaginationHelper
{
    public const int DefaultPageSize = 5000;

    /// <summary>
    /// Applies marker-based pagination to a list of items.
    /// Returns the current page and the next marker (null if no more pages).
    /// </summary>
    public static (List<T> Page, string? NextMarker) Paginate<T>(
        List<T> allItems, string? marker, int? maxResults, Func<T, string> getKey)
    {
        var pageSize = maxResults ?? DefaultPageSize;

        // Skip past the marker
        IEnumerable<T> items = allItems;
        if (!string.IsNullOrEmpty(marker))
            items = items.SkipWhile(i => string.Compare(getKey(i), marker, StringComparison.Ordinal) <= 0);

        var page = items.Take(pageSize).ToList();

        // Determine next marker
        string? nextMarker = null;
        if (page.Count == pageSize)
        {
            var lastKey = getKey(page[^1]);
            // Check if there are more items after this page
            var remaining = allItems.SkipWhile(i => string.Compare(getKey(i), lastKey, StringComparison.Ordinal) <= 0);
            if (remaining.Any())
                nextMarker = lastKey;
        }

        return (page, nextMarker);
    }
}
