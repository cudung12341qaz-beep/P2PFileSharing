namespace P2PFileSharing.Models
{
    public class NetworkPacket
    {
        public string Command { get; set; } = string.Empty; // HELLO, BYE, SEARCH_REQ, SEARCH_RES
        public string SearchId { get; set; } = string.Empty;
        public string SenderPeerId { get; set; } = string.Empty;
        public string SenderName { get; set; } = string.Empty;
        public int TcpPort { get; set; }
        public string Payload { get; set; } = string.Empty;
    }
}