using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using P2PFileSharing.Models;

namespace P2PFileSharing.Network
{
    public static class NetworkHelper
    {
        public static List<NetworkAdapterOption> GetAvailableAdapters()
        {
            var adapters = new List<NetworkAdapterOption>();

            foreach (var ni in NetworkInterface.GetAllNetworkInterfaces())
            {
                if (ni.OperationalStatus != OperationalStatus.Up)
                    continue;
                if (ni.NetworkInterfaceType == NetworkInterfaceType.Loopback)
                    continue;

                var ipProps = ni.GetIPProperties();
                foreach (var ua in ipProps.UnicastAddresses)
                {
                    if (ua.Address.AddressFamily != AddressFamily.InterNetwork)
                        continue;

                    var broadcastIp = CalculateBroadcastAddress(ua.Address, ua.IPv4Mask);
                    adapters.Add(new NetworkAdapterOption
                    {
                        Name = ni.Name,
                        IpAddress = ua.Address,
                        SubnetMask = ua.IPv4Mask,
                        BroadcastAddress = broadcastIp
                    });
                }
            }

            return adapters;
        }

        private static IPAddress CalculateBroadcastAddress(IPAddress ip, IPAddress mask)
        {
            byte[] ipBytes = ip.GetAddressBytes();
            byte[] maskBytes = mask.GetAddressBytes();
            byte[] broadcastBytes = new byte[4];

            for (int i = 0; i < 4; i++)
            {
                broadcastBytes[i] = (byte)(ipBytes[i] | (maskBytes[i] ^ 255));
            }

            return new IPAddress(broadcastBytes);
        }
    }
}