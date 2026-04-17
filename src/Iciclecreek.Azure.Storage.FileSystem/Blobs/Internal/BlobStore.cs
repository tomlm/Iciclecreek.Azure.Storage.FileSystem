using System.Security.Cryptography;
using System.Text.Json;
using Azure;
using Iciclecreek.Azure.Storage.FileSystem.Internal;

namespace Iciclecreek.Azure.Storage.FileSystem.Blobs.Internal;

internal sealed class BlobStore
{
    public BlobStore(FileStorageAccount account, string containerName)
    {
        Account = account;
        ContainerName = containerName;
        ContainerPath = Path.Combine(account.BlobsRootPath, containerName);
    }

    public FileStorageAccount Account { get; }

    public string ContainerName { get; }

    public string ContainerPath { get; }

    public FileStorageProvider Provider => Account.Provider;

    public bool ContainerExists() => Directory.Exists(ContainerPath);

    public bool CreateContainer()
    {
        if (Directory.Exists(ContainerPath)) return false;
        Directory.CreateDirectory(ContainerPath);
        return true;
    }

    public bool DeleteContainer()
    {
        if (!Directory.Exists(ContainerPath)) return false;
        Directory.Delete(ContainerPath, recursive: true);
        return true;
    }

    public string BlobPath(string blobName) => Path.Combine(ContainerPath, BlobPathEncoder.ToRelativePath(blobName));

    public string SidecarPath(string blobName) => BlobPathEncoder.SidecarPath(BlobPath(blobName));

    public bool Exists(string blobName) => File.Exists(BlobPath(blobName));

    public bool Delete(string blobName)
    {
        var path = BlobPath(blobName);
        var sidecar = BlobPathEncoder.SidecarPath(path);
        var existed = File.Exists(path);
        if (File.Exists(path)) File.Delete(path);
        if (File.Exists(sidecar)) File.Delete(sidecar);
        DeleteStagingFolder(blobName);
        return existed;
    }

    public void DeleteStagingFolder(string blobName)
    {
        var stagingDir = Path.Combine(ContainerPath, ".blocks", BlobPathEncoder.EncodeBlockId(blobName));
        if (Directory.Exists(stagingDir)) Directory.Delete(stagingDir, recursive: true);
    }

    public BlobSidecar? ReadSidecar(string blobName)
        => BlobSidecar.ReadFromFile(SidecarPath(blobName), Provider.JsonSerializerOptions);

    public void WriteSidecar(string blobName, BlobSidecar sidecar)
    {
        var json = JsonSerializer.Serialize(sidecar, Provider.JsonSerializerOptions);
        AtomicFile.WriteAllText(SidecarPath(blobName), json);
    }

    public (long Length, byte[] Md5) WriteContentFromStream(string blobName, Stream content)
    {
        var path = BlobPath(blobName);
        var dir = Path.GetDirectoryName(path);
        if (!string.IsNullOrEmpty(dir)) Directory.CreateDirectory(dir);

        var tmp = path + ".tmp";
        byte[] hash;
        long length;
        using (var fs = new FileStream(tmp, FileMode.Create, FileAccess.Write, FileShare.None))
        using (var md5 = IncrementalHash.CreateHash(HashAlgorithmName.MD5))
        {
            var buf = new byte[81920];
            int read;
            while ((read = content.Read(buf, 0, buf.Length)) > 0)
            {
                fs.Write(buf, 0, read);
                md5.AppendData(buf, 0, read);
            }
            fs.Flush(flushToDisk: true);
            length = fs.Length;
            hash = md5.GetHashAndReset();
        }
        // Stream closed — safe to move.
        if (File.Exists(path))
            File.Replace(tmp, path, destinationBackupFileName: null, ignoreMetadataErrors: true);
        else
            File.Move(tmp, path);
        return (length, hash);
    }

    public void CheckConditions(BlobSidecar? sidecar, ETag? ifMatch, ETag? ifNoneMatch, bool mustExist, string op)
    {
        if (mustExist && sidecar is null)
            throw new RequestFailedException(404, $"Blob not found for {op}.", "BlobNotFound", null);

        if (ifMatch.HasValue)
        {
            var actual = sidecar is null ? "" : sidecar.ETag;
            var want = ifMatch.Value;
            if (want == ETag.All)
            {
                if (sidecar is null)
                    throw new RequestFailedException(412, $"Condition not met on {op}: IfMatch=*, but blob does not exist.", "ConditionNotMet", null);
            }
            else if (actual != want.ToString())
            {
                throw new RequestFailedException(412, $"Condition not met on {op}: IfMatch.", "ConditionNotMet", null);
            }
        }

        if (ifNoneMatch.HasValue)
        {
            var want = ifNoneMatch.Value;
            if (want == ETag.All)
            {
                if (sidecar is not null)
                    throw new RequestFailedException(409, $"Condition not met on {op}: IfNoneMatch=*, but blob already exists.", "BlobAlreadyExists", null);
            }
            else if (sidecar is not null && sidecar.ETag == want.ToString())
            {
                throw new RequestFailedException(412, $"Condition not met on {op}: IfNoneMatch.", "ConditionNotMet", null);
            }
        }
    }

    public IEnumerable<(string BlobName, string FullPath, FileInfo Info)> EnumerateBlobs(string? prefix)
    {
        if (!Directory.Exists(ContainerPath)) yield break;

        var stack = new Stack<string>();
        stack.Push(ContainerPath);
        var prefixLen = ContainerPath.Length + 1;

        while (stack.Count > 0)
        {
            var dir = stack.Pop();
            var dirName = Path.GetFileName(dir);

            // Skip hidden system folders (.blocks, .tx*, or names starting with _)
            if (dir != ContainerPath)
            {
                if (dirName.StartsWith('.') || dirName.StartsWith('_')) continue;
            }

            foreach (var sub in Directory.EnumerateDirectories(dir))
                stack.Push(sub);

            foreach (var file in Directory.EnumerateFiles(dir))
            {
                var name = Path.GetFileName(file);
                if (BlobPathEncoder.IsSidecar(name)) continue;
                if (name.EndsWith(".tmp", StringComparison.Ordinal)) continue;
                if (name.StartsWith('_')) continue;

                var relative = file.Length > prefixLen ? file[prefixLen..] : "";
                if (string.IsNullOrEmpty(relative)) continue;
                var blobName = BlobPathEncoder.FromRelativePath(relative);

                if (prefix is null || blobName.StartsWith(prefix, StringComparison.Ordinal))
                    yield return (blobName, file, new FileInfo(file));
            }
        }
    }
}
