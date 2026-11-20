using P2PFileSharing.Models;
using P2PFileSharing.Network;

namespace P2PFileSharing
{
    public partial class Form1 : Form
    {
        private ComboBox cboAdapters = null!;
        private TextBox txtPeerName = null!;
        private Button btnToggle = null!;
        private ListBox lstPeers = null!;
        private ListBox lstLogs = null!;

        private UdpDiscoveryService? _discoveryService;
        private bool _isRunning = false;
        private bool _isClosing = false;

        public Form1()
        {
            InitializeComponent();
            BuildTestUi();
            LoadNetworkAdapters();
            this.FormClosing += Form1_FormClosing;
        }

        private void BuildTestUi()
        {
            this.Text = "P2P File Sharing - Test Discovery (Thành viên 1)";
            this.Size = new Size(760, 500);
            this.StartPosition = FormStartPosition.CenterScreen;

            var lblAdapter = new Label { Text = "Card mạng:", Left = 15, Top = 18, Width = 80 };
            cboAdapters = new ComboBox { Left = 100, Top = 14, Width = 330, DropDownStyle = ComboBoxStyle.DropDownList };

            var lblName = new Label { Text = "Tên máy:", Left = 445, Top = 18, Width = 65 };
            txtPeerName = new TextBox { Left = 515, Top = 14, Width = 120, Text = Environment.MachineName };

            btnToggle = new Button { Text = "Bắt đầu", Left = 645, Top = 12, Width = 85, Height = 27 };
            btnToggle.Click += BtnToggle_Click;

            var lblPeers = new Label { Text = "Danh sách Peer đang Online:", Left = 15, Top = 55, Width = 250 };
            lstPeers = new ListBox { Left = 15, Top = 80, Width = 340, Height = 360 };

            var lblLogs = new Label { Text = "Nhật ký hệ thống (Log):", Left = 370, Top = 55, Width = 250 };
            lstLogs = new ListBox { Left = 370, Top = 80, Width = 360, Height = 360 };

            this.Controls.AddRange(new Control[]
            {
                lblAdapter, cboAdapters, lblName, txtPeerName, btnToggle,
                lblPeers, lstPeers, lblLogs, lstLogs
            });
        }

        private void LoadNetworkAdapters()
        {
            cboAdapters.Items.Clear();
            var adapters = NetworkHelper.GetAvailableAdapters();
            foreach (var adapter in adapters)
            {
                cboAdapters.Items.Add(adapter);
            }

            if (cboAdapters.Items.Count > 0)
            {
                cboAdapters.SelectedIndex = 0;
            }
            else
            {
                AddLog("[Cảnh báo] Không tìm thấy card mạng IPv4 nào đang kết nối.");
            }
        }

        private async void BtnToggle_Click(object? sender, EventArgs e)
        {
            if (!_isRunning)
            {
                if (cboAdapters.SelectedItem is not NetworkAdapterOption selectedAdapter)
                {
                    MessageBox.Show("Vui lòng chọn một card mạng trước khi bắt đầu.");
                    return;
                }

                string peerName = string.IsNullOrWhiteSpace(txtPeerName.Text)
                    ? Environment.MachineName
                    : txtPeerName.Text.Trim();

                _discoveryService = new UdpDiscoveryService(peerName, AppConfig.DefaultTcpPort, selectedAdapter);
                _discoveryService.OnPeerListChanged += UpdatePeerList;
                _discoveryService.OnLog += AddLog;

                try
                {
                    _discoveryService.Start();
                    _isRunning = true;
                    btnToggle.Text = "Dừng";
                    cboAdapters.Enabled = false;
                    txtPeerName.Enabled = false;
                }
                catch (Exception ex)
                {
                    MessageBox.Show($"Không thể khởi động Discovery: {ex.Message}");
                }
            }
            else
            {
                if (_discoveryService != null)
                {
                    await _discoveryService.StopAsync();
                    _discoveryService.Dispose();
                    _discoveryService = null;
                }

                _isRunning = false;
                btnToggle.Text = "Bắt đầu";
                cboAdapters.Enabled = true;
                txtPeerName.Enabled = true;
                AddLog("[Discovery] Đã dừng dịch vụ.");
            }
        }

        private void UpdatePeerList(List<PeerInfo> peers)
        {
            if (InvokeRequired)
            {
                BeginInvoke(() => UpdatePeerList(peers));
                return;
            }

            lstPeers.Items.Clear();
            foreach (var peer in peers)
            {
                lstPeers.Items.Add($"{peer.PeerName} | {peer.IpAddress}:{peer.TcpPort}");
            }
        }

        private void AddLog(string message)
        {
            if (InvokeRequired)
            {
                BeginInvoke(() => AddLog(message));
                return;
            }

            lstLogs.Items.Add($"[{DateTime.Now:HH:mm:ss}] {message}");
            lstLogs.TopIndex = lstLogs.Items.Count - 1;
        }

        private async void Form1_FormClosing(object? sender, FormClosingEventArgs e)
        {
            if (_discoveryService != null && !_isClosing)
            {
                e.Cancel = true;
                _isClosing = true;
                await _discoveryService.StopAsync();
                _discoveryService.Dispose();
                _discoveryService = null;
                this.Close();
            }
        }
    }
}