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

        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(buffer, hash);

        var hex = Convert.ToHexString(hash[..8]); // 16 hex chars
        return new ETag($"\"0x{hex}\"");
    }
}
