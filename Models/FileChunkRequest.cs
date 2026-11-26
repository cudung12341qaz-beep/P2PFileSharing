namespace P2PFileSharing.Models
{
    public class FileChunkRequest
    {
        public string FileHash { get; set; } = string.Empty;
        public int ChunkIndex { get; set; }
    }
}
