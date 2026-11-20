namespace P2PFileSharing.Models
{
    public class SharedFileInfo
    {
        public string FileHash { get; set; } = string.Empty;
        public string FileName { get; set; } = string.Empty;
        public long FileSize { get; set; }
        public int TotalChunks { get; set; }
        public string LocalPath { get; set; } = string.Empty;
    }
}