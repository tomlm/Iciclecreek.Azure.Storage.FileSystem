namespace Iciclecreek.Azure.Storage.FileSystem.Tables.Internal;

internal static class TableKeyEncoder
{
    public static string Encode(string key)
    {
        if (string.IsNullOrEmpty(key)) return "__empty__";
        return Uri.EscapeDataString(key);
    }

    public static string Decode(string encoded)
    {
        if (encoded == "__empty__") return string.Empty;
        return Uri.UnescapeDataString(encoded);
    }
}
