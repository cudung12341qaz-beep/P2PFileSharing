namespace P2PFileSharing.Models
{
    public static class AppConfig
    {
        public const int UdpDiscoveryPort = 8888;
        public const int DefaultTcpPort = 9999;
        public const int HeartbeatIntervalMs = 5000;   // Gửi HELLO mỗi 5 giây
        public const int PeerTimeoutSeconds = 15;      // Quá 15 giây không thấy tín hiệu thì xóa
        public const int ChunkSize = 1024 * 1024;      // 1 MiB mỗi mảnh
        public const int MaxUdpPacketBytes = 1200;     // Tránh phân mảnh gói UDP trên mạng LAN/VPN
    }
}