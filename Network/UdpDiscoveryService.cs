using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using P2PFileSharing.Models;

namespace P2PFileSharing.Network
{
    public class UdpDiscoveryService : IDisposable
    {
        private readonly string _myPeerId = Guid.NewGuid().ToString("N")[..8];
        private readonly string _myPeerName;
        private readonly int _myTcpPort;
        private readonly NetworkAdapterOption _adapter;

        private UdpClient? _receiveClient;
        private UdpClient? _sendClient;
        private CancellationTokenSource? _cts;

        private readonly ConcurrentDictionary<string, PeerInfo> _activePeers = new();

        // Sự kiện cung cấp cho Thành viên 4 (GUI)
        public event Action<List<PeerInfo>>? OnPeerListChanged;

        // Sự kiện điều phối gói SEARCH_REQ / SEARCH_RES sang Thành viên 3
        public event Action<NetworkPacket, string>? OnSearchPacketReceived;

        // Sự kiện ghi log lên giao diện
        public event Action<string>? OnLog;

        public UdpDiscoveryService(string peerName, int tcpPort, NetworkAdapterOption adapter)
        {
            _myPeerName = peerName;
            _myTcpPort = tcpPort;
            _adapter = adapter;
        }

        public void Start()
        {
            _cts = new CancellationTokenSource();

            // Socket lắng nghe chung ở cổng 8888
            _receiveClient = new UdpClient();
            _receiveClient.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
            _receiveClient.Client.Bind(new IPEndPoint(IPAddress.Any, AppConfig.UdpDiscoveryPort));

            // Socket gửi gắn cố định vào IP của card mạng đã chọn
            _sendClient = new UdpClient(new IPEndPoint(_adapter.IpAddress, 0));
            _sendClient.EnableBroadcast = true;

            _ = Task.Run(() => ReceiveLoopAsync(_cts.Token));
            _ = Task.Run(() => HeartbeatLoopAsync(_cts.Token));
            _ = Task.Run(() => CleanupLoopAsync(_cts.Token));

            OnLog?.Invoke($"[Discovery] Đã khởi động trên {_adapter.IpAddress} (Broadcast: {_adapter.BroadcastAddress})");
        }

        public async Task StopAsync()
        {
            try
            {
                await SendControlPacketAsync("BYE");
            }
            catch { }

            _cts?.Cancel();
            _receiveClient?.Close();
            _sendClient?.Close();
            _activePeers.Clear();
            NotifyPeerListChanged();
        }

        public List<PeerInfo> GetActivePeers() => _activePeers.Values.ToList();

        // Hàm cung cấp cho Thành viên 3 để gửi bản tin UDP đến 1 IP cụ thể
        public async Task SendToPeerAsync(string targetIp, NetworkPacket packet)
        {
            if (_sendClient == null) return;

            packet.SenderPeerId = _myPeerId;
            packet.SenderName = _myPeerName;
            packet.TcpPort = _myTcpPort;

            byte[] data = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(packet));
            if (data.Length > AppConfig.MaxUdpPacketBytes)
            {
                OnLog?.Invoke($"[Cảnh báo] Gói UDP vượt quá {AppConfig.MaxUdpPacketBytes} bytes, hủy gửi.");
                return;
            }

            var endpoint = new IPEndPoint(IPAddress.Parse(targetIp), AppConfig.UdpDiscoveryPort);
            await _sendClient.SendAsync(data, data.Length, endpoint);
        }

        // Hàm cung cấp cho Thành viên 3 để phát câu hỏi tìm kiếm tới toàn bộ Peer đang online
        public async Task BroadcastPacketAsync(NetworkPacket packet)
        {
            if (_sendClient == null) return;

            packet.SenderPeerId = _myPeerId;
            packet.SenderName = _myPeerName;
            packet.TcpPort = _myTcpPort;

            byte[] data = Encoding.UTF8.GetBytes(JsonSerializer.Serialize(packet));
            if (data.Length > AppConfig.MaxUdpPacketBytes)
            {
                OnLog?.Invoke($"[Cảnh báo] Gói UDP vượt quá {AppConfig.MaxUdpPacketBytes} bytes, hủy gửi.");
                return;
            }

            var broadcastEndpoint = new IPEndPoint(_adapter.BroadcastAddress, AppConfig.UdpDiscoveryPort);
            await _sendClient.SendAsync(data, data.Length, broadcastEndpoint);
        }

        private async Task SendControlPacketAsync(string command)
        {
            var packet = new NetworkPacket { Command = command };
            await BroadcastPacketAsync(packet);
        }

        private async Task HeartbeatLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await SendControlPacketAsync("HELLO");
                    await Task.Delay(AppConfig.HeartbeatIntervalMs, token);
                }
                catch (OperationCanceledException) { break; }
                catch (Exception ex)
                {
                    OnLog?.Invoke($"[Lỗi gửi HELLO] {ex.Message}");
                }
            }
        }

        private async Task ReceiveLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested && _receiveClient != null)
            {
                try
                {
                    var result = await _receiveClient.ReceiveAsync(token);
                    string json = Encoding.UTF8.GetString(result.Buffer);
                    var packet = JsonSerializer.Deserialize<NetworkPacket>(json);

                    if (packet == null || packet.SenderPeerId == _myPeerId)
                        continue; // Bỏ qua gói tin của chính mình

                    string senderIp = result.RemoteEndPoint.Address.ToString();
                    HandlePacket(packet, senderIp);
                }
                catch (OperationCanceledException) { break; }
                catch (ObjectDisposedException) { break; }
                catch (Exception ex)
                {
                    OnLog?.Invoke($"[Lỗi nhận UDP] {ex.Message}");
                }
            }
        }

        private void HandlePacket(NetworkPacket packet, string senderIp)
        {
            switch (packet.Command)
            {
                case "HELLO":
                    bool isNew = !_activePeers.ContainsKey(packet.SenderPeerId);
                    _activePeers[packet.SenderPeerId] = new PeerInfo
                    {
                        PeerId = packet.SenderPeerId,
                        PeerName = packet.SenderName,
                        IpAddress = senderIp,
                        TcpPort = packet.TcpPort,
                        LastSeen = DateTime.Now
                    };
                    if (isNew)
                    {
                        OnLog?.Invoke($"[Discovery] Phát hiện Peer mới: {packet.SenderName} ({senderIp}:{packet.TcpPort})");
                        NotifyPeerListChanged();
                    }
                    break;

                case "BYE":
                    if (_activePeers.TryRemove(packet.SenderPeerId, out var removed))
                    {
                        OnLog?.Invoke($"[Discovery] Peer đã thoát: {removed.PeerName} ({removed.IpAddress})");
                        NotifyPeerListChanged();
                    }
                    break;

                case "SEARCH_REQ":
                case "SEARCH_RES":
                    OnSearchPacketReceived?.Invoke(packet, senderIp);
                    break;
            }
        }

        private async Task CleanupLoopAsync(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                try
                {
                    await Task.Delay(3000, token);
                    var now = DateTime.Now;
                    bool changed = false;

                    foreach (var kvp in _activePeers)
                    {
                        if ((now - kvp.Value.LastSeen).TotalSeconds > AppConfig.PeerTimeoutSeconds)
                        {
                            if (_activePeers.TryRemove(kvp.Key, out var expired))
                            {
                                OnLog?.Invoke($"[Timeout] Mất kết nối với {expired.PeerName} ({expired.IpAddress})");
                                changed = true;
                            }
                        }
                    }

                    if (changed) NotifyPeerListChanged();
                }
                catch (OperationCanceledException) { break; }
            }
        }

        private void NotifyPeerListChanged()
        {
            OnPeerListChanged?.Invoke(GetActivePeers());
        }

        public void Dispose()
        {
            _cts?.Cancel();
            _receiveClient?.Dispose();
            _sendClient?.Dispose();
        }
    }
}