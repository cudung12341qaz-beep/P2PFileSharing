using System.Net;

namespace P2PFileSharing.Models
{
    public class NetworkAdapterOption
    {
        public string Name { get; set; } = string.Empty;
        public IPAddress IpAddress { get; set; } = IPAddress.None;
        public IPAddress SubnetMask { get; set; } = IPAddress.None;
        public IPAddress BroadcastAddress { get; set; } = IPAddress.Broadcast;

        public override string ToString() => $"{Name} ({IpAddress})";
    }
}