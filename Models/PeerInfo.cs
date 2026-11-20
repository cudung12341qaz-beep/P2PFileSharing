namespace P2PFileSharing.Models
{
    public class PeerInfo
    {
        public string PeerId { get; set; } = string.Empty;
        public string PeerName { get; set; } = string.Empty;
        public string IpAddress { get; set; } = string.Empty;
        public int TcpPort { get; set; }
        public DateTime LastSeen { get; set; }
    }
}