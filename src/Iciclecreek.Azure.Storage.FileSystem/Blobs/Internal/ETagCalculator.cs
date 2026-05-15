using System.Security.Cryptography;
using Azure;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal static class ETagCalculator
{
    public static ETag Compute(long size, DateTimeOffset lastModified, ReadOnlySpan<byte> md5)
    {
        Span<byte> buffer = stackalloc byte[8 + 8 + 16];
        BitConverter.TryWriteBytes(buffer, size);
        BitConverter.TryWriteBytes(buffer[8..], lastModified.UtcTicks);
        if (!md5.IsEmpty && md5.Length <= 16)
            md5.CopyTo(buffer[16..]);

        byte[] hashBytes;
        using (var sha256 = SHA256.Create())
            hashBytes = sha256.ComputeHash(buffer.ToArray());

        var hex = BitConverter.ToString(hashBytes, 0, 8).Replace("-", ""); // 16 hex chars
        return new ETag($"\"0x{hex}\"");
    }
}
