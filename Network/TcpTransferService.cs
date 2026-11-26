using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text.Json;
using P2PFileSharing.Models;

namespace P2PFileSharing.Network
{
    public class TcpTransferService
    {
        private const int MaxRequestBytes = 1024;
        private const int MaxConcurrentRequests = 4;
        private const int MaxDownloadWorkers = 4;
        private const int MaxChunkRetries = 3;
        private const byte Rejected = 0;
        private const byte Accepted = 1;
        private static readonly TimeSpan RequestTimeout = TimeSpan.FromSeconds(10);

        public static int CalculateTotalChunks(long fileSize)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(fileSize);

            var (wholeChunks, remainingBytes) = Math.DivRem(fileSize, AppConfig.ChunkSize);
            return checked((int)(wholeChunks + (remainingBytes == 0 ? 0 : 1)));
        }

        public static long CalculateOffset(int chunkIndex)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(chunkIndex);

            return (long)chunkIndex * AppConfig.ChunkSize;
        }

        public static int CalculateChunkLength(long fileSize, int chunkIndex)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(fileSize);

            long offset = CalculateOffset(chunkIndex);
            ArgumentOutOfRangeException.ThrowIfGreaterThanOrEqual(offset, fileSize, nameof(chunkIndex));

            return (int)Math.Min(AppConfig.ChunkSize, fileSize - offset);
        }

        public static void PreAllocateFile(string tempPath, long fileSize)
        {
            ArgumentOutOfRangeException.ThrowIfNegative(fileSize);

            using var file = CreatePreallocatedFile(tempPath, fileSize);
        }

        public static async Task RunServerAsync(
            TcpListener listener,
            Func<string, SharedFileInfo?> findSharedFile,
            CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(listener);
            ArgumentNullException.ThrowIfNull(findSharedFile);
            cancellationToken.ThrowIfCancellationRequested();

            using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            listener.Start();
            try
            {
                var workers = new Task[MaxConcurrentRequests];
                for (int i = 0; i < workers.Length; i++)
                    workers[i] = ServeRequestsAsync(listener, findSharedFile, shutdown.Token);

                // Dừng các worker còn lại nếu một worker kết thúc hoặc gặp lỗi ngoài dự kiến.
                await Task.WhenAny(workers).ConfigureAwait(false);
                await shutdown.CancelAsync().ConfigureAwait(false);
                await Task.WhenAll(workers).ConfigureAwait(false);
            }
            finally
            {
                listener.Stop();
            }
        }

        public static async Task<byte[]> DownloadSingleChunkAsync(
            IPEndPoint source,
            string fileHash,
            int chunkIndex,
            long fileSize,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(source);
            ArgumentException.ThrowIfNullOrWhiteSpace(fileHash);
            int expectedLength = CalculateChunkLength(fileSize, chunkIndex);

            var request = new FileChunkRequest { FileHash = fileHash, ChunkIndex = chunkIndex };
            byte[] json = JsonSerializer.SerializeToUtf8Bytes(request);
            if (json.Length > MaxRequestBytes)
                throw new ArgumentException("Yêu cầu tải mảnh vượt giới hạn kích thước.", nameof(fileHash));

            byte[] frame = new byte[sizeof(int) + json.Length];
            BinaryPrimitives.WriteInt32LittleEndian(frame, json.Length);
            json.CopyTo(frame, sizeof(int));

            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(RequestTimeout);
            using var client = new TcpClient(source.AddressFamily);
            try
            {
                await client.ConnectAsync(source, timeout.Token).ConfigureAwait(false);
                NetworkStream stream = client.GetStream();
                await stream.WriteAsync(frame, timeout.Token).ConfigureAwait(false);

                byte[] header = new byte[1 + sizeof(int)];
                await stream.ReadExactlyAsync(header.AsMemory(0, 1), timeout.Token).ConfigureAwait(false);
                if (header[0] == Rejected)
                    throw new IOException("Nguồn tải từ chối yêu cầu mảnh.");
                if (header[0] != Accepted)
                    throw new InvalidDataException("Mã trạng thái TCP không hợp lệ.");

                await stream.ReadExactlyAsync(header.AsMemory(1), timeout.Token).ConfigureAwait(false);
                int length = BinaryPrimitives.ReadInt32LittleEndian(header.AsSpan(1));
                if (length != expectedLength)
                    throw new InvalidDataException("Độ dài mảnh nhận được khác độ dài dự kiến.");

                byte[] chunk = new byte[length];
                await stream.ReadExactlyAsync(chunk, timeout.Token).ConfigureAwait(false);
                return chunk;
            }
            catch (OperationCanceledException ex) when (!cancellationToken.IsCancellationRequested)
            {
                throw new TimeoutException("Quá thời gian 10 giây khi tải mảnh qua TCP.", ex);
            }
        }

        public static async Task DownloadMultiSourceAsync(
            IEnumerable<IPEndPoint> sources,
            string fileHash,
            long fileSize,
            string destinationPath,
            IProgress<int>? progress = null,
            CancellationToken cancellationToken = default)
        {
            ArgumentNullException.ThrowIfNull(sources);
            ArgumentException.ThrowIfNullOrWhiteSpace(fileHash);
            ArgumentException.ThrowIfNullOrWhiteSpace(destinationPath);
            if (fileHash.Length != 64 || !fileHash.All(Uri.IsHexDigit))
                throw new ArgumentException("Mã SHA-256 phải gồm 64 ký tự hex.", nameof(fileHash));

            int totalChunks = CalculateTotalChunks(fileSize);
            var readySources = new Queue<IPEndPoint>();
            if (totalChunks > 0)
            {
                foreach (IPEndPoint source in sources.Distinct())
                {
                    if (source is null || source.Port == 0)
                        throw new ArgumentException("Nguồn tải cần địa chỉ IP và cổng TCP hợp lệ.", nameof(sources));

                    readySources.Enqueue(source);
                }

                if (readySources.Count == 0)
                    throw new IOException("Không có nguồn để tải tệp.");
            }

            cancellationToken.ThrowIfCancellationRequested();
            string finalPath = Path.GetFullPath(destinationPath);
            if (File.Exists(finalPath))
                throw new IOException("Tệp đích đã tồn tại.");

            // Mỗi lượt tải có tệp tạm riêng trong cùng thư mục với tệp đích.
            string tempPath = Path.Combine(Path.GetDirectoryName(finalPath)!, $"{Guid.NewGuid():N}.part");
            bool ownsTempFile = false;
            try
            {
                using (FileStream file = CreatePreallocatedFile(tempPath, fileSize))
                {
                    ownsTempFile = true;
                    progress?.Report(0);

                    if (totalChunks > 0)
                    {
                        await DownloadChunksAsync(file, readySources, fileHash, fileSize, totalChunks,
                            progress, cancellationToken).ConfigureAwait(false);
                    }

                    // Đọc lại tệp ghép từ byte đầu tiên để kiểm tra SHA-256.
                    file.Seek(0, SeekOrigin.Begin);
                    byte[] hash = await SHA256.HashDataAsync(file, cancellationToken).ConfigureAwait(false);
                    if (!string.Equals(Convert.ToHexString(hash), fileHash, StringComparison.OrdinalIgnoreCase))
                        throw new InvalidDataException("SHA-256 của tệp tải về không khớp.");
                }

                cancellationToken.ThrowIfCancellationRequested();
                File.Move(tempPath, finalPath);
            }
            catch
            {
                if (ownsTempFile)
                    TryDeleteTempFile(tempPath);

                throw;
            }

            progress?.Report(100);
        }

        private static async Task DownloadChunksAsync(
            FileStream file,
            Queue<IPEndPoint> readySources,
            string fileHash,
            long fileSize,
            int totalChunks,
            IProgress<int>? progress,
            CancellationToken cancellationToken)
        {
            var pendingChunks = new ConcurrentQueue<int>();
            for (int index = 0; index < totalChunks; index++)
                pendingChunks.Enqueue(index);

            var retries = new Dictionary<int, int>();
            var running = new Dictionary<Task<byte[]>, (int Index, IPEndPoint Source)>();
            int writtenChunks = 0;
            Exception? lastSourceError = null;

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            try
            {
                while (writtenChunks < totalChunks)
                {
                    stop.Token.ThrowIfCancellationRequested();

                    while (running.Count < MaxDownloadWorkers && readySources.Count > 0 &&
                           pendingChunks.TryDequeue(out int nextIndex))
                    {
                        IPEndPoint nextSource = readySources.Dequeue();
                        Task<byte[]> download = DownloadSingleChunkAsync(nextSource, fileHash, nextIndex,
                            fileSize, stop.Token);
                        running.Add(download, (nextIndex, nextSource));
                    }

                    if (running.Count == 0)
                        throw new IOException("Không còn nguồn hoạt động để tải các mảnh còn thiếu.", lastSourceError);

                    // Hàng đợi có thể trống trong lúc các mảnh cuối vẫn đang được tải.
                    Task<byte[]> finished = await Task.WhenAny(running.Keys).ConfigureAwait(false);
                    var (chunkIndex, source) = running[finished];
                    running.Remove(finished);

                    byte[] chunk;
                    try
                    {
                        chunk = await finished.ConfigureAwait(false);
                    }
                    catch (Exception ex) when (ex is IOException or SocketException or TimeoutException or InvalidDataException)
                    {
                        lastSourceError = ex;
                        retries.TryGetValue(chunkIndex, out int retryCount);
                        if (retryCount >= MaxChunkRetries)
                            throw new IOException($"Mảnh {chunkIndex} đã hết số lần thử lại.", ex);

                        // Trả mảnh về hàng đợi và bỏ nguồn vừa lỗi khỏi lượt tải.
                        retries[chunkIndex] = retryCount + 1;
                        pendingChunks.Enqueue(chunkIndex);
                        continue;
                    }

                    // Chỉ vòng lặp này ghi tệp nên các lượt Seek và Write không tranh nhau.
                    file.Seek(CalculateOffset(chunkIndex), SeekOrigin.Begin);
                    await file.WriteAsync(chunk, stop.Token).ConfigureAwait(false);
                    readySources.Enqueue(source);
                    writtenChunks++;

                    int percent = (int)(writtenChunks * 100L / totalChunks);
                    progress?.Report(Math.Min(percent, 99));
                }
            }
            finally
            {
                if (running.Count > 0)
                {
                    await stop.CancelAsync().ConfigureAwait(false);
                    try
                    {
                        await Task.WhenAll(running.Keys).ConfigureAwait(false);
                    }
                    catch (Exception)
                    {
                        // Đợi các kết nối đóng. Lỗi chính được báo từ vòng lặp tải.
                    }
                }
            }
        }

        private static FileStream CreatePreallocatedFile(string tempPath, long fileSize)
        {
            var file = new FileStream(tempPath, FileMode.CreateNew, FileAccess.ReadWrite,
                FileShare.None, 4096, useAsync: true);
            try
            {
                file.SetLength(fileSize);
                return file;
            }
            catch
            {
                file.Dispose();
                TryDeleteTempFile(tempPath);
                throw;
            }
        }

        private static void TryDeleteTempFile(string tempPath)
        {
            try
            {
                File.Delete(tempPath);
            }
            catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
            {
                // Giữ nguyên lỗi tải ban đầu nếu hệ điều hành không cho dọn tệp tạm.
            }
        }

        private static async Task ServeRequestsAsync(
            TcpListener listener,
            Func<string, SharedFileInfo?> findSharedFile,
            CancellationToken cancellationToken)
        {
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    TcpClient client;
                    try
                    {
                        client = await listener.AcceptTcpClientAsync(cancellationToken).ConfigureAwait(false);
                    }
                    catch (SocketException ex) when (ex.SocketErrorCode is SocketError.ConnectionAborted
                        or SocketError.ConnectionReset or SocketError.NetworkReset or SocketError.TimedOut)
                    {
                        Trace.TraceWarning("Lỗi tạm thời khi nhận kết nối TCP: {0}", ex.SocketErrorCode);
                        await Task.Delay(100, cancellationToken).ConfigureAwait(false);
                        continue;
                    }

                    using (client)
                    {
                        await SendRequestedChunkAsync(client, findSharedFile, cancellationToken).ConfigureAwait(false);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
            }
        }

        private static async Task SendRequestedChunkAsync(
            TcpClient client,
            Func<string, SharedFileInfo?> findSharedFile,
            CancellationToken cancellationToken)
        {
            using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            timeout.CancelAfter(RequestTimeout);

            try
            {
                NetworkStream stream = client.GetStream();
                FileChunkRequest? request = await ReadRequestAsync(stream, timeout.Token).ConfigureAwait(false);
                byte[]? chunk = request is null
                    ? null
                    : await ReadSharedChunkAsync(request, findSharedFile, timeout.Token).ConfigureAwait(false);

                if (chunk is null)
                {
                    await stream.WriteAsync(new byte[] { Rejected }, timeout.Token).ConfigureAwait(false);
                    return;
                }

                byte[] header = new byte[1 + sizeof(int)];
                header[0] = Accepted;
                BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(1), chunk.Length);
                await stream.WriteAsync(header, timeout.Token).ConfigureAwait(false);
                await stream.WriteAsync(chunk, timeout.Token).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (timeout.IsCancellationRequested)
            {
                // Hết thời gian hoặc server dừng, đóng kết nối hiện tại.
            }
            catch (IOException)
            {
                // Peer đã ngắt kết nối hoặc không gửi đủ dữ liệu.
            }
            catch (SocketException)
            {
                // Lỗi kết nối chỉ ảnh hưởng đến yêu cầu hiện tại.
            }
            catch (Exception ex)
            {
                // Đóng kết nối hiện tại và tiếp tục phục vụ yêu cầu khác.
                Trace.TraceError("Không xử lý được yêu cầu tải mảnh TCP: {0}", ex);
            }
        }

        private static async Task<FileChunkRequest?> ReadRequestAsync(Stream stream, CancellationToken cancellationToken)
        {
            byte[] header = new byte[sizeof(int)];
            await stream.ReadExactlyAsync(header, cancellationToken).ConfigureAwait(false);
            int length = BinaryPrimitives.ReadInt32LittleEndian(header);
            if (length <= 0 || length > MaxRequestBytes)
                return null;

            byte[] json = new byte[length];
            await stream.ReadExactlyAsync(json, cancellationToken).ConfigureAwait(false);

            try
            {
                using JsonDocument document = JsonDocument.Parse(json);
                JsonElement root = document.RootElement;
                if (root.ValueKind != JsonValueKind.Object ||
                    !root.TryGetProperty(nameof(FileChunkRequest.FileHash), out JsonElement hash) ||
                    hash.ValueKind != JsonValueKind.String ||
                    !root.TryGetProperty(nameof(FileChunkRequest.ChunkIndex), out JsonElement index) ||
                    index.ValueKind != JsonValueKind.Number || !index.TryGetInt32(out int chunkIndex) || chunkIndex < 0)
                {
                    return null;
                }

                string? fileHash = hash.GetString();
                return string.IsNullOrWhiteSpace(fileHash) ? null : new FileChunkRequest
                {
                    FileHash = fileHash,
                    ChunkIndex = chunkIndex
                };
            }
            catch (JsonException)
            {
                return null;
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        private static async Task<byte[]?> ReadSharedChunkAsync(
            FileChunkRequest request,
            Func<string, SharedFileInfo?> findSharedFile,
            CancellationToken cancellationToken)
        {
            SharedFileInfo? sharedFile;
            try
            {
                sharedFile = findSharedFile(request.FileHash);
            }
            catch (Exception ex)
            {
                Trace.TraceError("Không tra cứu được danh mục tệp chia sẻ: {0}", ex);
                return null;
            }

            long offset = CalculateOffset(request.ChunkIndex);
            if (sharedFile is null || offset >= sharedFile.FileSize || string.IsNullOrWhiteSpace(sharedFile.LocalPath))
                return null;

            try
            {
                using var file = new FileStream(
                    sharedFile.LocalPath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, useAsync: true);
                if (file.Length != sharedFile.FileSize)
                    return null;

                byte[] chunk = new byte[CalculateChunkLength(sharedFile.FileSize, request.ChunkIndex)];
                file.Seek(offset, SeekOrigin.Begin);
                await file.ReadExactlyAsync(chunk, cancellationToken).ConfigureAwait(false);
                return chunk;
            }
            catch (IOException)
            {
                return null;
            }
            catch (UnauthorizedAccessException)
            {
                return null;
            }
        }
    }
}
