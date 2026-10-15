"""Shared Network Guardrail, Userspace Wireproxy, and Route Resolution Infrastructure.

This module provides engine-agnostic network protection, low-latency route inspection,
userspace Wireproxy lifecycle management, and rate-limit circuit breakers.
It is shared across dynamic recording, static recording, and multi-stream coordination.
"""

from __future__ import annotations

import ctypes
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import threading
import time
from typing import Callable, Dict, List, Optional, Tuple
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener, urlopen


# Standard ports for local userspace wireproxy
WIREPROXY_SOCKS5_PORT = 10808
WIREPROXY_HTTP_PORT = 10809

# Rate limiting and hold timeouts
RATE_LIMIT_COOLDOWN_SEC = 90.0
RATE_LIMIT_ERROR_THRESHOLD = 8
RATE_LIMIT_WINDOW_SEC = 15.0
VPN_HOLD_TIMEOUT_SEC = 30 * 60.0
INTERNET_HOLD_TIMEOUT_SEC = 30 * 60.0


def default_log(msg: str, level: str = "INFO", **kwargs) -> None:
    """Fallback console logger when caller does not provide an orchestrator logger."""
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}", flush=True)


def default_render_progress(text: str, pad: int = 0) -> None:
    """Fallback single-line terminal progress renderer."""
    print(f"\r{text}{' ' * max(0, pad)}", end="", flush=True)


def default_clear_progress() -> None:
    """Fallback progress line clearer."""
    print("\r\x1b[2K", end="", flush=True)


def fmt_hms(seconds: float) -> str:
    """Format duration in seconds as HH:MM:SS or MM:SS."""
    s = int(max(0.0, seconds))
    h = s // 3600
    m = (s % 3600) // 60
    sec = s % 60
    if h > 0:
        return f"{h:02d}:{m:02d}:{sec:02d}"
    return f"{m:02d}:{sec:02d}"


# ==============================================================================
# WireproxyManager: Local Userspace WireGuard Proxy Daemon
# ==============================================================================

class WireproxyManager:
    """
    Manages local userspace wireproxy child process lifecycle,
    runtime config generation, health checks, and 3-2-1 tiered failover.
    """

    def __init__(
        self,
        socks5_port: int = WIREPROXY_SOCKS5_PORT,
        http_port: int = WIREPROXY_HTTP_PORT,
        log_fn: Callable = default_log,
        raw_start_fn: Optional[Callable] = None,
        raw_end_fn: Optional[Callable] = None,
        raw_write_fn: Optional[Callable] = None,
    ):
        self.socks5_port = socks5_port
        self.http_port = http_port
        self.log_fn = log_fn
        self.raw_start_fn = raw_start_fn
        self.raw_end_fn = raw_end_fn
        self.raw_write_fn = raw_write_fn
        self.proc: Optional[subprocess.Popen] = None
        self.active_config_path: Optional[str] = None
        self.active_server: Optional[str] = None
        self.config_dir: Optional[str] = self.resolve_config_dir()
        self.wireproxy_bin: Optional[str] = self.find_wireproxy_binary()
        self.raw_token: Optional[dict] = None
        self.shared_external_instance: bool = False

    @staticmethod
    def find_wireproxy_binary() -> Optional[str]:
        """
        Locate wireproxy binary using universal two-tier discovery:
        1. System PATH via shutil.which("wireproxy") -> return "wireproxy" to invoke directly by name
        2. Local script / repo fallback -> return absolute path if wireproxy(.exe) is co-located
        """
        bin_on_path = shutil.which("wireproxy")
        if bin_on_path:
            return bin_on_path

        # Standard installation fallback paths
        standard_locations = [
            "/opt/homebrew/bin/wireproxy",
            "/usr/local/bin/wireproxy",
            r"C:\tools\wireproxy\wireproxy.exe",
            r"C:\Program Files\wireproxy\wireproxy.exe",
        ]
        for loc in standard_locations:
            if os.path.isfile(loc) and os.access(loc, os.X_OK):
                return loc

        # Co-located script / module fallback
        script_dir = os.path.dirname(os.path.abspath(__file__))
        local_name = "wireproxy.exe" if os.name == "nt" else "wireproxy"
        candidates = [
            os.path.join(script_dir, local_name),
            os.path.join(os.path.dirname(script_dir), local_name),
            os.path.join(os.getcwd(), local_name),
        ]
        for c in candidates:
            if os.path.isfile(c):
                return c
        return None

    @staticmethod
    def resolve_config_dir() -> Optional[str]:
        """
        Locate wireguard_configs directory:
        1. Explicit environment variable WIREGUARD_CONFIG_DIR
        2. Grandparent directory (e.g. ~/Movies/GSE SMART IPTV/Recordings/wireguard_configs)
        3. Parent directory (e.g. ../wireguard_configs)
        4. Repo / script directory (e.g. ./wireguard_configs)
        """
        env_dir = os.environ.get("WIREGUARD_CONFIG_DIR")
        if env_dir and os.path.isdir(env_dir):
            return os.path.abspath(env_dir)

        script_dir = os.path.dirname(os.path.abspath(__file__))
        repo_dir = os.path.dirname(script_dir)
        manual_recordings_dir = os.path.dirname(repo_dir)
        grandparent_dir = os.path.dirname(manual_recordings_dir)

        candidates = [
            os.path.join(grandparent_dir, "wireguard_configs"),
            os.path.join(manual_recordings_dir, "wireguard_configs"),
            os.path.join(repo_dir, "wireguard_configs"),
            os.path.join(script_dir, "wireguard_configs"),
            os.path.join(os.getcwd(), "wireguard_configs"),
            os.path.expanduser("~/Movies/GSE SMART IPTV/Recordings/wireguard_configs"),
            r"C:\My PC Recordings\wireguard_configs",
            r"C:\Users\sumit\WIN_TV_RECORDINGS\wireguard_configs",
        ]
        for c in candidates:
            if os.path.isdir(c):
                return os.path.abspath(c)
        return None

    def get_server_conf_path(self, server_name: str) -> Optional[str]:
        """Find the raw .conf file for a given server name."""
        if not self.config_dir:
            return None
        base = server_name[:-5] if server_name.endswith(".conf") else server_name
        target = os.path.join(self.config_dir, f"{base}.conf")
        if os.path.isfile(target):
            return target
        return None

    def create_runtime_config(self, source_conf_path: str, temp_dir: str) -> str:
        """
        Create ephemeral runtime config inside temp_dir,
        appending [Socks5] and [http] blocks without modifying source file.
        """
        os.makedirs(temp_dir, exist_ok=True)
        runtime_conf_path = os.path.join(temp_dir, "_wireproxy_active.tmp.conf")

        with open(source_conf_path, "r", encoding="utf-8", errors="replace") as f:
            content = f.read()

        blocks = []
        if "[Socks5]" not in content and "[socks5]" not in content:
            blocks.append(f"\n[Socks5]\nBindAddress = 127.0.0.1:{self.socks5_port}\n")
        if "[http]" not in content and "[Http]" not in content:
            blocks.append(f"\n[http]\nBindAddress = 127.0.0.1:{self.http_port}\n")

        with open(runtime_conf_path, "w", encoding="utf-8", errors="replace") as f:
            f.write(content + "".join(blocks))

        return runtime_conf_path

    def start(self, server_name: str, temp_dir: str) -> bool:
        """
        Launch wireproxy with server_name.
        Returns True if process starts and port responds within 2.5s.
        If an existing instance is already running (multi-worker), reuses it cleanly.
        """
        # Idempotent port check: reuse existing active tunnel if available
        if self.check_port_responsive(timeout=0.2):
            self.active_server = server_name
            self.shared_external_instance = True
            self.export_environment()
            self.log_fn(
                f"Network Route     : Wireproxy tunnel active on 127.0.0.1:{self.http_port} (sharing active instance).",
                level="INFO",
            )
            return True

        if not self.wireproxy_bin:
            self.log_fn("WIREPROXY: wireproxy binary not found on PATH or in standard paths!", level="WARN")
            return False

        source_conf = self.get_server_conf_path(server_name)
        if not source_conf:
            self.log_fn(f"WIREPROXY: Configuration file for '{server_name}' not found in {self.config_dir}!", level="WARN")
            return False

        self.stop(silent=True)

        try:
            self.active_config_path = self.create_runtime_config(source_conf, temp_dir)
            cmd = [self.wireproxy_bin, "-c", self.active_config_path]

            if self.raw_start_fn:
                self.raw_token = self.raw_start_fn("Wireproxy", server_name)

            popen_kwargs = {
                "stdout": subprocess.PIPE,
                "stderr": subprocess.STDOUT,
                "text": True,
                "bufsize": 1,
            }
            if os.name == "nt":
                popen_kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", 0x08000000)

            self.proc = subprocess.Popen(cmd, **popen_kwargs)

            # Drain stdout/stderr in a dedicated background thread and stream to raw external log
            raw_token = self.raw_token
            raw_write = self.raw_write_fn

            def _drain_wireproxy_stdout(p, token):
                if not p or not p.stdout:
                    return
                try:
                    for line in iter(p.stdout.readline, ""):
                        if not line:
                            break
                        if raw_write and token:
                            raw_write(token, line.rstrip("\r\n"))
                except Exception:
                    pass
                finally:
                    try:
                        p.stdout.close()
                    except Exception:
                        pass

            drain_thread = threading.Thread(
                target=_drain_wireproxy_stdout,
                args=(self.proc, raw_token),
                daemon=True,
                name="wireproxy_log_drain",
            )
            drain_thread.start()

            deadline = time.monotonic() + 2.5
            while time.monotonic() < deadline:
                if self.proc.poll() is not None:
                    self.stop(silent=True)
                    return False
                if self.check_port_responsive(timeout=0.15):
                    self.active_server = server_name
                    self.shared_external_instance = False
                    self.export_environment()
                    return True
                time.sleep(0.1)

            self.stop(silent=True)
            return False
        except Exception as e:
            self.log_fn(f"WIREPROXY: Failed to launch wireproxy: {e}", level="ERROR")
            if self.raw_token and self.raw_end_fn:
                self.raw_end_fn(self.raw_token, status=f"launch exception {type(e).__name__}")
                self.raw_token = None
            self.stop(silent=True)
            return False

    def export_environment(self) -> None:
        """Export proxy environment variables for Python urllib and subprocesses."""
        proxy_url = f"http://127.0.0.1:{self.http_port}"
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        os.environ["http_proxy"] = proxy_url
        os.environ["https_proxy"] = proxy_url
        os.environ["ALL_PROXY"] = f"socks5://127.0.0.1:{self.socks5_port}"
        try:
            import urllib.request
            urllib.request._opener = None
            opener = urllib.request.build_opener(
                urllib.request.ProxyHandler({
                    "http": proxy_url,
                    "https": proxy_url,
                })
            )
            urllib.request.install_opener(opener)
        except Exception:
            pass
        try:
            NetworkRouteResolver.invalidate_cache()
        except Exception:
            pass

    def unexport_environment(self) -> None:
        """Remove proxy environment variables upon tunnel termination."""
        for var in ("HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy", "ALL_PROXY"):
            os.environ.pop(var, None)
        try:
            import urllib.request
            urllib.request._opener = None
            direct_opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
            urllib.request.install_opener(direct_opener)
        except Exception:
            pass
        try:
            NetworkRouteResolver.invalidate_cache()
        except Exception:
            pass

    def stop(self, silent: bool = False) -> None:
        """Clean termination of wireproxy child process and temporary config cleanup."""
        self.unexport_environment()
        was_running = False
        rc = None

        if self.shared_external_instance:
            self.shared_external_instance = False
            return

        if self.proc:
            try:
                if self.proc.poll() is None:
                    self.proc.terminate()
                    try:
                        rc = self.proc.wait(timeout=1.5)
                    except subprocess.TimeoutExpired:
                        self.proc.kill()
                        rc = self.proc.wait()
                    was_running = True
                else:
                    rc = self.proc.poll()
            except Exception:
                pass
            self.proc = None

        if self.raw_token and self.raw_end_fn:
            self.raw_end_fn(self.raw_token, returncode=rc)
            self.raw_token = None

        if self.active_config_path:
            try:
                if os.path.isfile(self.active_config_path):
                    os.remove(self.active_config_path)
            except Exception:
                pass
            self.active_config_path = None

        if was_running and not silent:
            self.log_fn("Network Route     : Wireproxy tunnel stopped cleanly.", level="INFO")

    def is_running(self) -> bool:
        """Check if wireproxy is actively running or responding."""
        if self.shared_external_instance:
            return self.check_port_responsive(timeout=0.15)
        return bool(self.proc and self.proc.poll() is None)

    def check_port_responsive(self, timeout: float = 0.2) -> bool:
        """Fast zero-traffic local TCP socket probe to verify listener is accepting connections."""
        try:
            s = socket.create_connection(("127.0.0.1", self.socks5_port), timeout=timeout)
            s.close()
            return True
        except (OSError, socket.timeout):
            return False

    def recover_3_2_1(
        self,
        country: str,
        priority_list: List[str],
        temp_dir: str,
        geo_verifier: Optional[Callable[[str], bool]] = None,
    ) -> bool:
        """
        Universal 3-2-1 Tiered Auto-Recovery Engine:
        - Tier 1 (Primary): 3 attempts
        - Tier 2 (Secondary): 2 attempts
        - Tier 3 (Tertiary): 1 attempt
        """
        if not priority_list:
            return False

        attempts_per_tier = [3, 2, 1]
        servers_to_try = list(priority_list)
        if self.active_server and self.active_server in servers_to_try:
            servers_to_try.remove(self.active_server)
            servers_to_try.insert(0, self.active_server)
        self.stop(silent=True)

        for tier_idx, server in enumerate(servers_to_try):
            max_attempts = attempts_per_tier[tier_idx] if tier_idx < len(attempts_per_tier) else 1
            tier_name = f"Tier {tier_idx + 1}"

            for attempt in range(1, max_attempts + 1):
                self.log_fn(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] {tier_name} ({server}) attempt {attempt}/{max_attempts}...", level="INFO")
                if self.start(server, temp_dir):
                    if geo_verifier:
                        if geo_verifier(server):
                            self.log_fn(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] Recovered successfully via {server} ({tier_name})!", level="INFO")
                            return True
                        else:
                            self.log_fn(f"NETWORK_GUARD: {server} port opened but geo verification failed.", level="WARN")
                            self.stop()
                    else:
                        self.log_fn(f"NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] Recovered successfully via {server} ({tier_name})!", level="INFO")
                        return True
                time.sleep(0.3)

        self.log_fn("NETWORK_GUARD: [STAGE 1 AUTO-RECOVERY] All 3-2-1 recovery tiers exhausted without restoration.", level="WARN")
        return False


def is_wireproxy_active(state: object) -> bool:
    """Return True if userspace wireproxy child process is actively running."""
    if not state:
        return False
    mgr = getattr(state, "wireproxy_manager", None)
    return bool(mgr and mgr.is_running())


# ==============================================================================
# NetworkRouteResolver: Sub-Millisecond Native Route & Geo Resolution
# ==============================================================================

class NetworkRouteResolver:
    """
    Cross-platform network route and interface resolver.
    """

    _geoip_cache_time: float = 0.0
    _geoip_cache_ip: Optional[str] = None
    _geoip_cache_country: Optional[str] = None

    @classmethod
    def invalidate_cache(cls) -> None:
        """Purge GeoIP cache on network interface transitions or proxy state changes."""
        cls._geoip_cache_time = 0.0
        cls._geoip_cache_ip = None
        cls._geoip_cache_country = None

    @staticmethod
    def get_default_route_info() -> Tuple[str, bool]:
        """
        Inspect the OS routing table to find the egress interface for default traffic.
        Returns: (interface_name, is_vpn_tunnel)
        """
        if sys.platform == "darwin":
            try:
                res = subprocess.run(
                    ["route", "-n", "get", "8.8.8.8"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                m = re.search(r'interface:\s*(\S+)', res.stdout)
                if not m:
                    res = subprocess.run(
                        ["route", "-n", "get", "default"],
                        capture_output=True,
                        text=True,
                        timeout=1.5,
                    )
                    m = re.search(r'interface:\s*(\S+)', res.stdout)
                if m:
                    iface = m.group(1).strip()
                    is_vpn = iface.startswith(("utun", "ppp", "tun", "ipsec"))
                    return iface, is_vpn
            except Exception:
                pass
            return "", False

        elif os.name == "nt":
            vpn_keywords = (
                "wireguard", "tap-windows", "wintun", "openvpn",
                "nordlynx", "tailscale", "vpn", "surfshark",
                "ikev2", "ipsec", "proton",
            )
            try:
                iphlpapi = ctypes.windll.iphlpapi
                best_if_index = ctypes.c_ulong()
                dest_ip = ctypes.c_ulong(0x08080808)  # 8.8.8.8 DWORD in network byte order
                if iphlpapi.GetBestInterface(dest_ip, ctypes.byref(best_if_index)) == 0:
                    if_idx = best_if_index.value

                    class MIB_IFROW(ctypes.Structure):
                        _fields_ = [
                            ("wszName", ctypes.c_uint16 * 256),
                            ("dwIndex", ctypes.c_uint32),
                            ("dwType", ctypes.c_uint32),
                            ("dwMtu", ctypes.c_uint32),
                            ("dwSpeed", ctypes.c_uint32),
                            ("dwPhysAddrLen", ctypes.c_uint32),
                            ("bPhysAddr", ctypes.c_uint8 * 8),
                            ("dwAdminStatus", ctypes.c_uint32),
                            ("dwOperStatus", ctypes.c_uint32),
                            ("dwLastChange", ctypes.c_uint32),
                            ("dwInOctets", ctypes.c_uint32),
                            ("dwInUcastPkts", ctypes.c_uint32),
                            ("dwInNUcastPkts", ctypes.c_uint32),
                            ("dwInDiscards", ctypes.c_uint32),
                            ("dwInErrors", ctypes.c_uint32),
                            ("dwInUnknownProtos", ctypes.c_uint32),
                            ("dwOutOctets", ctypes.c_uint32),
                            ("dwOutUcastPkts", ctypes.c_uint32),
                            ("dwOutNUcastPkts", ctypes.c_uint32),
                            ("dwOutDiscards", ctypes.c_uint32),
                            ("dwOutErrors", ctypes.c_uint32),
                            ("dwOutQLen", ctypes.c_uint32),
                            ("dwDescrLen", ctypes.c_uint32),
                            ("bDescr", ctypes.c_uint8 * 256),
                        ]

                    row = MIB_IFROW()
                    row.dwIndex = if_idx
                    if iphlpapi.GetIfEntry(ctypes.byref(row)) == 0:
                        descr = bytes(row.bDescr[:row.dwDescrLen]).decode("utf-8", errors="ignore").strip()
                        is_vpn = any(k in descr.lower() for k in vpn_keywords)
                        iface_name = descr or f"adapter_{if_idx}"
                        return iface_name, is_vpn
            except Exception:
                pass

            # Fallback for Windows netsh
            try:
                res = subprocess.run(
                    ["netsh", "interface", "ipv4", "show", "route", "0.0.0.0/0"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                stdout = res.stdout or ""
                for line in stdout.splitlines():
                    if "0.0.0.0/0" in line:
                        parts = line.split()
                        if parts:
                            iface_candidate = " ".join(parts[5:]) if len(parts) >= 6 else parts[-1]
                            is_vpn = any(k in iface_candidate.lower() for k in vpn_keywords)
                            return iface_candidate, is_vpn
            except Exception:
                pass

        return "", False

    @staticmethod
    def get_active_vpn_app_name() -> Optional[str]:
        """
        Detect running commercial VPN client applications by inspecting native OS
        service status and running process signatures.
        """
        vpn_signatures = [
            ("surfshark", "Surfshark"),
            ("nordvpn", "NordVPN"),
            ("nordsec", "NordVPN"),
            ("expressvpn", "ExpressVPN"),
            ("express", "ExpressVPN"),
            ("mullvad", "Mullvad"),
            ("protonvpn", "ProtonVPN"),
            ("proton", "ProtonVPN"),
            ("tailscale", "Tailscale"),
            ("windscribe", "Windscribe"),
            ("cyberghost", "CyberGhost"),
            ("pia-client", "Private Internet Access"),
            ("privateinternetaccess", "Private Internet Access"),
            ("cloudflare", "Cloudflare WARP"),
            ("warp", "Cloudflare WARP"),
            ("anyconnect", "Cisco AnyConnect"),
            ("wireguard", "WireGuard"),
            ("openvpn", "OpenVPN"),
        ]

        if sys.platform == "darwin":
            try:
                res = subprocess.run(
                    ["scutil", "--nc", "list"],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                if res.returncode == 0:
                    for line in (res.stdout or "").splitlines():
                        line_low = line.lower()
                        if "(connected)" in line_low:
                            for key, brand in vpn_signatures:
                                if key in line_low:
                                    return brand
            except Exception:
                pass

            try:
                pattern = "surfshark|nordvpn|expressvpn|mullvad|proton|tailscale|windscribe|cyberghost|wireguard|openvpn"
                res = subprocess.run(
                    ["pgrep", "-l", "-i", pattern],
                    capture_output=True,
                    text=True,
                    timeout=1.5,
                )
                if res.returncode == 0 and res.stdout:
                    proc_stdout = res.stdout.lower()
                    for key, brand in vpn_signatures:
                        if key in proc_stdout:
                            return brand
            except Exception:
                pass

        elif os.name == "nt":
            try:
                res = subprocess.run(
                    ["tasklist", "/NH", "/FO", "CSV"],
                    capture_output=True,
                    text=True,
                    timeout=2.0,
                )
                if res.returncode == 0 and res.stdout:
                    proc_stdout = res.stdout.lower()
                    for key, brand in vpn_signatures:
                        if key in proc_stdout:
                            return brand
            except Exception:
                pass

        return None

    @staticmethod
    def get_friendly_vpn_name(iface: str) -> str:
        """Extract a friendly VPN provider name from active app or adapter description."""
        app_name = NetworkRouteResolver.get_active_vpn_app_name()
        if app_name:
            return app_name

        low = (iface or "").lower()
        if "surfshark" in low:
            return "Surfshark"
        if "nord" in low or "nordlynx" in low:
            return "NordVPN"
        if "mullvad" in low:
            return "Mullvad"
        if "proton" in low:
            return "ProtonVPN"
        if "tailscale" in low:
            return "Tailscale"
        if "wireguard" in low:
            return "WireGuard"
        if "openvpn" in low:
            return "OpenVPN"
        if "express" in low or "expressvpn" in low:
            return "ExpressVPN"
        if low.startswith("utun"):
            return "System VPN"
        if "wintun" in low:
            return "Wintun VPN"
        if "tap" in low:
            return "TAP VPN"
        return iface or "System VPN"

    @classmethod
    def get_public_ip_and_country(
        cls,
        timeout: float = 2.5,
        proxy_url: Optional[str] = None,
        force_direct: bool = False,
    ) -> Tuple[Optional[str], Optional[str]]:
        """
        Fast, lightweight query to retrieve public IP and 2-letter ISO country code.
        Supports optional proxy_url for Wireproxy egress verification, or force_direct
        to bypass ambient proxy environment variables when probing host routes.
        Uses 10-second cache when query destination is identical.
        """
        now = time.monotonic()
        if not proxy_url and not force_direct and cls._geoip_cache_time and (now - cls._geoip_cache_time < 10.0):
            if cls._geoip_cache_ip and cls._geoip_cache_country:
                return cls._geoip_cache_ip, cls._geoip_cache_country

        opener = None
        if proxy_url:
            try:
                opener = build_opener(ProxyHandler({
                    "http": proxy_url,
                    "https": proxy_url,
                }))
            except Exception:
                opener = None
        elif force_direct:
            # Explicitly force direct egress without ambient proxy environment variables
            try:
                opener = build_opener(ProxyHandler({}))
            except Exception:
                opener = None

        def _do_open(req, to):
            if opener is not None:
                return opener.open(req, timeout=to)
            return urlopen(req, timeout=to)

        # Primary: ip-api.com
        try:
            req = Request(
                "http://ip-api.com/json",
                headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
            )
            with _do_open(req, timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                if data.get("status") == "success":
                    ip = str(data.get("query") or "").strip()
                    country = str(data.get("countryCode") or "").strip().upper()
                    if ip and country:
                        if not proxy_url:
                            cls._geoip_cache_time = now
                            cls._geoip_cache_ip = ip
                            cls._geoip_cache_country = country
                        return ip, country
        except Exception:
            pass

        # Secondary: api.country.is
        try:
            req = Request(
                "https://api.country.is/",
                headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
            )
            with _do_open(req, timeout) as resp:
                data = json.loads(resp.read().decode("utf-8", errors="ignore"))
                ip = str(data.get("ip") or "").strip()
                country = str(data.get("country") or "").strip().upper()
                if ip and country:
                    if not proxy_url:
                        cls._geoip_cache_time = now
                        cls._geoip_cache_ip = ip
                        cls._geoip_cache_country = country
                    return ip, country
        except Exception:
            pass

        # Fallback: Cloudflare trace
        for endpoint in ("https://1.1.1.1/cdn-cgi/trace", "https://cloudflare.com/cdn-cgi/trace"):
            try:
                req = Request(
                    endpoint,
                    headers={"User-Agent": "Mozilla/5.0 (RecorderNetworkGuard/1.0)"},
                )
                with _do_open(req, timeout) as resp:
                    data = resp.read().decode("utf-8", errors="ignore")
                    parsed = dict(
                        line.split("=", 1)
                        for line in data.strip().splitlines()
                        if "=" in line
                    )
                    ip = parsed.get("ip", "").strip()
                    loc = parsed.get("loc", "").strip().upper()
                    if ip and loc:
                        if not proxy_url:
                            cls._geoip_cache_time = now
                            cls._geoip_cache_ip = ip
                            cls._geoip_cache_country = loc
                        return ip, loc
            except Exception:
                continue

        return None, None

    @classmethod
    def is_internet_online(cls, timeout: float = 1.5) -> bool:
        """Fast low-overhead TCP socket connectivity test to public DNS servers."""
        endpoints = (
            ("1.1.1.1", 53),
            ("8.8.8.8", 53),
            ("1.1.1.1", 443),
        )
        for host, port in endpoints:
            try:
                s = socket.create_connection((host, port), timeout=timeout)
                s.close()
                return True
            except OSError:
                continue
        return False

    @classmethod
    def is_internet_online_debounced(
        cls,
        required_consecutive_failures: int = 2,
        retry_delay: float = 1.5,
        timeout_per_probe: float = 1.5,
    ) -> bool:
        """
        Verify outbound internet with confirmation retry.
        Returns True if internet is online on any attempt.
        Returns False only if ALL consecutive attempts fail, ensuring transient
        routing-table switches (1-2s during VPN disconnect on macOS/Windows)
        do not trigger false-positive disconnects.
        """
        for attempt in range(max(1, int(required_consecutive_failures))):
            if cls.is_internet_online(timeout=timeout_per_probe):
                return True
            if attempt < required_consecutive_failures - 1:
                time.sleep(retry_delay)
        return False


# ==============================================================================
# NetworkGuard: The Guardrail, Policy Engine & Circuit Breaker
# ==============================================================================

class NetworkGuard:
    """
    Engine-agnostic Network Guardrail & Rate-Limit Circuit Breaker.
    """

    def __init__(
        self,
        required_country: Optional[str] = None,
        drop_policy: Optional[str] = None,
        detection_source: str = "config",
        server_priority: Optional[Dict[str, List[str]]] = None,
        hold_timeout_min: int = 30,
        log_fn: Callable = default_log,
        render_progress_fn: Callable = default_render_progress,
        clear_progress_fn: Callable = default_clear_progress,
        alarm_start_fn: Optional[Callable] = None,
        alarm_stop_fn: Optional[Callable] = None,
        beep_good_fn: Optional[Callable] = None,
        beep_bad_fn: Optional[Callable] = None,
        log_good_beep_fn: Optional[Callable] = None,
        raw_start_fn: Optional[Callable] = None,
        raw_end_fn: Optional[Callable] = None,
        raw_write_fn: Optional[Callable] = None,
        resume_callback: Optional[Callable] = None,
        state=None,
    ):
        self.state = state
        self.required_country = str(required_country).strip().upper() if required_country else None
        if self.required_country in ("NONE", "UNRESTRICTED", "ANY", "OFF"):
            self.required_country = None

        self.drop_policy = (drop_policy or "LENIENT").upper()
        self.detection_source = detection_source
        self.server_priority = server_priority or {
            "IN": ["in-mum", "in-del"],
            "AU": ["au-syd", "au-mel"],
            "GB": ["gb-lon", "gb-man"],
        }
        self.hold_timeout_sec = float(hold_timeout_min * 60)
        self.is_armed = (self.required_country is not None)
        self.lenient_drop_active: bool = False

        # Logging & UI Callbacks
        self.log_fn = log_fn
        self.render_progress_fn = render_progress_fn
        self.clear_progress_fn = clear_progress_fn
        self.alarm_start_fn = alarm_start_fn
        self.alarm_stop_fn = alarm_stop_fn
        self.beep_good_fn = beep_good_fn
        self.beep_bad_fn = beep_bad_fn
        self.log_good_beep_fn = log_good_beep_fn
        self.raw_start_fn = raw_start_fn
        self.raw_end_fn = raw_end_fn
        self.raw_write_fn = raw_write_fn
        self.resume_callback = resume_callback

        # Routing and tunnel state
        self.wireproxy_manager: Optional[WireproxyManager] = None
        self.vpn_mode = "UNRESTRICTED"
        self.active_system_vpn: Optional[str] = None
        self.bypassed_wireproxy: bool = False
        self.vpn_conflict: bool = False
        self.conflict_country: Optional[str] = None
        self.conflict_iface: str = ""
        self.conflict_vpn_name: str = ""

        # Launch snapshot
        self.launch_ip: Optional[str] = None
        self.launch_country: Optional[str] = None
        self.launch_interface: str = ""
        self.launch_is_vpn: bool = False

        # Route tracking
        self.last_route_interface: str = ""
        self.last_route_is_vpn: bool = False
        self.last_route_check_mono: float = 0.0
        self.last_internet_check_mono: float = 0.0
        self.last_internet_status: bool = True
        self.hold_active: bool = False

        # Metrics
        self.vpn_drop_count: int = 0
        self.vpn_hold_total_duration: float = 0.0
        self.internet_drop_count: int = 0
        self.internet_hold_total_duration: float = 0.0
        self.rate_limit_429_count: int = 0
        self.last_429_incident_mono: float = 0.0

        # Real-time HTTP 429 & 403 tracking
        self.recent_429_timestamps: List[float] = []
        self.consecutive_429_count: int = 0
        self.rate_limit_strike: int = 0
        self.has_403_error: bool = False
        self.blocked_403_ip: Optional[str] = None
        self.last_403_incident_mono: float = 0.0

        # Double-VPN runtime tracking
        self._runtime_double_vpn_warned: bool = False
        self._last_double_vpn_provider: str = ""

        if self.is_armed:
            self._evaluate_tunnel_staging()

    def _evaluate_tunnel_staging(self) -> None:
        """Inspect host network and stage Wireproxy or activate Smart Double-VPN auto-bypass."""
        host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
        host_provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface) if host_is_vpn else ""

        wm = WireproxyManager(
            log_fn=self.log_fn,
            raw_start_fn=self.raw_start_fn,
            raw_end_fn=self.raw_end_fn,
            raw_write_fn=self.raw_write_fn,
        )
        server_list = self.server_priority.get(self.required_country, [])
        has_configs = any(wm.get_server_conf_path(s) for s in server_list) if server_list else False
        wireproxy_capable = bool(wm.wireproxy_bin and has_configs)

        if host_is_vpn:
            host_ip, host_loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
            if host_loc == self.required_country:
                # Target country matches: auto-bypass Wireproxy to prevent double-tunneling
                self.vpn_mode = "LEGACY_VPN"
                self.active_system_vpn = host_provider
                self.bypassed_wireproxy = wireproxy_capable
                self.launch_ip = host_ip
                self.launch_country = host_loc
                self.launch_interface = host_iface
                self.launch_is_vpn = True
                self.last_route_interface = host_iface
                self.last_route_is_vpn = True
            else:
                # Host VPN is on a conflicting country (e.g. US != IN)
                self.vpn_mode = "LEGACY_VPN"
                self.active_system_vpn = host_provider
                self.vpn_conflict = True
                self.conflict_country = host_loc
                self.conflict_iface = host_iface
                self.conflict_vpn_name = host_provider
                self.launch_ip = host_ip
                self.launch_country = host_loc
                self.launch_interface = host_iface
                self.launch_is_vpn = True
                self.last_route_interface = host_iface
                self.last_route_is_vpn = True
        elif wireproxy_capable:
            self.vpn_mode = "WIREPROXY"
            self.wireproxy_manager = wm
            if self.state:
                setattr(self.state, "wireproxy_manager", wm)
        else:
            self.vpn_mode = "LEGACY_VPN"

    def format_header_lines(self) -> List[str]:
        """Format standard lines for inclusion in the main startup header."""
        lines = []
        if self.vpn_conflict:
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            vpn_label = f"SYSTEM_VPN: {self.conflict_vpn_name}" if self.conflict_vpn_name else "SYSTEM_VPN"
            lines.append(f"Network Guard     : CONFLICT [{vpn_label}] (Connected: {self.conflict_country or 'Unknown'} != Required: {self.required_country} | Policy: {effective_policy} | Target: {self.detection_source})")
        elif self.vpn_mode == "WIREPROXY":
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            servers = self.server_priority.get(self.required_country, [])
            primary = servers[0] if servers else "unspecified"
            lines.append(f"Network Guard     : ARMED [WIREPROXY] (Required: {self.required_country} | Primary: {primary} | Policy: {effective_policy} | Target: {self.detection_source})")
            lines.append(f"Proxy Endpoints   : SOCKS5 127.0.0.1:{WIREPROXY_SOCKS5_PORT} | HTTP 127.0.0.1:{WIREPROXY_HTTP_PORT}")
        elif self.vpn_mode == "LEGACY_VPN":
            effective_policy = "LENIENT" if self.is_lenient_drop_allowed() else "STRICT"
            vpn_label = f"SYSTEM_VPN: {self.active_system_vpn}" if self.active_system_vpn else "SYSTEM_VPN"
            adapter_info = f" | Adapter: {self.launch_interface}" if self.launch_interface else ""
            lines.append(f"Network Guard     : ARMED [{vpn_label}] (Required: {self.required_country}{adapter_info} | Policy: {effective_policy} | Target: {self.detection_source})")
        else:
            lines.append("Network Guard     : UNRESTRICTED (Open / Generic Stream)")
        return lines

    def format_info_lines(self) -> List[str]:
        lines = []
        if self.is_armed:
            lines.append(f"Guardrail status   : ARMED (Required: {self.required_country})")
            if self.active_system_vpn and self.launch_interface and self.launch_interface != self.active_system_vpn:
                egress_desc = f"{self.launch_ip} via {self.active_system_vpn} ({self.launch_interface})"
            elif self.launch_interface:
                egress_desc = f"{self.launch_ip} via {self.launch_interface}"
            else:
                egress_desc = f"{self.launch_ip}"
            lines.append(f"Launch egress      : {egress_desc}")
            cur_iface, cur_vpn = NetworkRouteResolver.get_default_route_info()
            lines.append(f"Current route      : {cur_iface} ({'VPN' if cur_vpn else 'PHYSICAL / UNPROTECTED'})")
            lines.append(f"VPN drop events    : {self.vpn_drop_count}")
            lines.append(f"Total VPN hold     : {fmt_hms(self.vpn_hold_total_duration)}")
            lines.append(f"HTTP 429 incidents : {self.rate_limit_429_count}")
        else:
            lines.append("Guardrail status   : UNRESTRICTED")
            if self.internet_drop_count > 0:
                lines.append(f"Internet drop events: {self.internet_drop_count}")
                lines.append(f"Total Internet hold : {fmt_hms(self.internet_hold_total_duration)}")
        return lines

    def format_summary_lines(self) -> List[str]:
        lines = [
            "Network & VPN Guardrail",
            "-----------------------",
        ]
        if self.is_armed:
            lines.append(f"Target Country          : {self.required_country} ({self.detection_source})")
            if self.vpn_mode == "WIREPROXY":
                active_srv = getattr(self.wireproxy_manager, "active_server", None) or "wireproxy"
                via_desc = f"Wireproxy ({active_srv})"
            elif self.active_system_vpn:
                if self.launch_interface and self.launch_interface != self.active_system_vpn:
                    via_desc = f"System VPN: {self.active_system_vpn} ({self.launch_interface})"
                else:
                    via_desc = f"System VPN: {self.active_system_vpn}"
            elif self.launch_interface:
                via_desc = self.launch_interface
            else:
                via_desc = "direct"
            lines.append(f"Launch Network          : {self.launch_ip} ({self.launch_country}) via {via_desc}")
            lines.append(f"VPN Disconnect Events   : {self.vpn_drop_count}")
            lines.append(f"Total VPN Hold Duration : {fmt_hms(self.vpn_hold_total_duration)}")
            lines.append(f"HTTP 429 Incidents      : {self.rate_limit_429_count}")
            lines.append("Guardrail Violations    : 0 (0 packets leaked outside VPN)")
        else:
            lines.append("Guardrail Status        : UNRESTRICTED (Open / Generic Stream)")
            if self.internet_drop_count > 0:
                lines.append(f"Internet Drop Events    : {self.internet_drop_count}")
                lines.append(f"Total Internet Hold     : {fmt_hms(self.internet_hold_total_duration)}")
            else:
                lines.append("Internet Outages        : 0")
        return lines

    def is_lenient_drop_allowed(self) -> bool:
        """LENIENT mode is permitted for N_m3u8DL-RE when drop policy is LENIENT."""
        return self.drop_policy == "LENIENT"

    def attempt_wireguard_auto_recovery(self, blocked_ip: Optional[str] = None, temp_dir: str = "./_temp_chunks") -> bool:
        """Stage 1: Silent WireGuard Auto-Recovery across configured servers."""
        if getattr(self.state, "stop_flag", False):
            return False

        if self.vpn_mode != "WIREPROXY" or not self.wireproxy_manager:
            return False

        priority_list = self.server_priority.get(self.required_country, [])
        if not priority_list:
            return False

        def _geo_check(server_name: str) -> bool:
            proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5, proxy_url=proxy_url)
            if loc != self.required_country:
                return False
            if blocked_ip and ip == blocked_ip:
                return False
            self.launch_ip = ip
            self.launch_country = loc
            return True

        recovered = self.wireproxy_manager.recover_3_2_1(
            country=self.required_country,
            priority_list=priority_list,
            temp_dir=temp_dir,
            geo_verifier=_geo_check,
        )
        if recovered:
            self.last_route_is_vpn = True
            return True

        self.last_route_is_vpn = False
        return False

    def preflight_check(self, temp_dir: str = "./_temp_chunks", is_scheduled: bool = False) -> bool:
        """Verify network route and geo egress before connecting to broadcaster endpoints."""
        if self.vpn_mode == "UNRESTRICTED":
            if not NetworkRouteResolver.is_internet_online():
                self.log_fn("Network Route     : Outbound internet connectivity unreachable!", level="WARN")
                return self.handle_internet_hold_state(is_startup=True)
            return True

        if self.vpn_conflict:
            self.log_fn(
                f"Network Route     : Conflict detected! Host system VPN ({self.conflict_vpn_name}) is active on '{self.conflict_iface}' "
                f"(Country: {self.conflict_country or 'Unknown'}), but stream requires {self.required_country}.",
                level="WARN",
            )
            self.log_fn(
                f"Network Route     : Nesting Wireproxy inside another VPN causes packet drop. "
                f"Please disconnect {self.conflict_vpn_name} or switch it to {self.required_country}.",
                level="WARN",
            )
            return self.handle_vpn_hold_state(is_startup=True)

        if self.bypassed_wireproxy:
            if not self.launch_ip or not self.launch_country:
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5)
                self.launch_ip = ip or self.launch_ip
                self.launch_country = loc or self.launch_country
            if not self.launch_interface:
                iface, _ = NetworkRouteResolver.get_default_route_info()
                self.launch_interface = iface

            if is_scheduled:
                self.log_fn(f"Network Route     : Ready [SYSTEM_VPN: {self.active_system_vpn}] (Staged for scheduled start | Adapter: {self.launch_interface})", level="INFO")
                return True
            self.log_fn(f"Network Route     : Active System VPN detected on host ({self.active_system_vpn} | IP: {self.launch_ip} | Country: {self.launch_country})", level="INFO")
            self.log_fn(f"Network Route     : Wireproxy bypassed to avoid double-VPN conflict. Using {self.active_system_vpn} directly.", level="INFO")
            self.log_fn(
                f"Network Route     : Connected [SYSTEM_VPN: {self.active_system_vpn}] "
                f"(Country: {self.launch_country} | Adapter: {self.launch_interface} | IP: {self.launch_ip})",
                level="INFO",
            )
            return True

        if self.vpn_mode == "WIREPROXY":
            server_list = self.server_priority.get(self.required_country, [])
            primary_server = server_list[0] if server_list else "default"

            if not NetworkRouteResolver.is_internet_online():
                self.log_fn("Network Route     : Outbound internet connectivity unreachable!", level="WARN")
                if not self.handle_internet_hold_state(is_startup=True):
                    return False

            if is_scheduled:
                self.log_fn(f"Network Route     : Ready [WIREPROXY] (Staged for scheduled start | Server: {primary_server})", level="INFO")
                return True

            self.log_fn(f"Network Route     : Launching Wireproxy ({primary_server})...", level="INFO")
            if not self.wireproxy_manager.start(primary_server, temp_dir):
                self.log_fn(f"Network Route     : Initial launch of {primary_server} failed. Executing auto-recovery...", level="WARN")
                if not self.attempt_wireguard_auto_recovery(temp_dir=temp_dir):
                    return self.handle_vpn_hold_state(is_startup=True)

            proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=3.0, proxy_url=proxy_url)
            self.launch_ip = ip
            self.launch_country = loc
            self.last_route_is_vpn = True

            if loc == self.required_country:
                self.log_fn(
                    f"Network Route     : Connected [WIREPROXY] (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip})",
                    level="INFO",
                )
                return True
            else:
                self.log_fn(
                    f"Network Route     : Country mismatch via Wireproxy! Connected to {loc or 'Unknown'} != Required: {self.required_country}",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(is_startup=True)

        elif self.vpn_mode == "LEGACY_VPN":
            iface, is_vpn = NetworkRouteResolver.get_default_route_info()
            ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.5)

            self.last_route_interface = iface
            self.last_route_is_vpn = is_vpn
            provider = NetworkRouteResolver.get_friendly_vpn_name(iface)
            self.active_system_vpn = provider if is_vpn else None

            if not is_vpn:
                self.log_fn(
                    f"Network Route     : Default route is not a VPN interface ({iface or 'unknown'})!",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(is_startup=True)

            if is_scheduled:
                self.log_fn(f"Network Route     : Ready [SYSTEM_VPN: {provider}] (Staged for scheduled start | Adapter: {iface})", level="INFO")
                return True

            if loc == self.required_country:
                self.launch_ip = ip
                self.launch_country = loc
                self.launch_interface = iface
                self.launch_is_vpn = is_vpn
                self.log_fn(
                    f"Network Route     : Connected [SYSTEM_VPN: {provider}] (Country: {loc} | Adapter: {iface} | IP: {ip})",
                    level="INFO",
                )
                return True
            else:
                self.log_fn(
                    f"Network Route     : Country mismatch! Connected to {loc or 'Unknown'} ({iface}) != Required: {self.required_country}",
                    level="WARN",
                )
                return self.handle_vpn_hold_state(is_startup=True)

        return True

    def check_runtime_route(self) -> bool:
        """
        Zero-traffic route check (called every 1s during recording).
        In Wireproxy mode: checks wireproxy child process & local port 10808 (0 internet packets).
        In Legacy mode: checks OS kernel default route (utun* / Windows adapter).
        Returns False if the VPN connection has dropped.
        """
        if self.state and getattr(self.state, "stop_flag", False):
            return True

        if self.vpn_mode == "UNRESTRICTED":
            return True

        now = time.monotonic()
        if now - self.last_route_check_mono < 1.0:
            return self.last_route_is_vpn
        self.last_route_check_mono = now

        if self.vpn_mode == "WIREPROXY":
            is_ok = bool(
                self.wireproxy_manager
                and self.wireproxy_manager.is_running()
                and self.wireproxy_manager.check_port_responsive(timeout=0.15)
            )
            prev_ok = self.last_route_is_vpn
            self.last_route_is_vpn = is_ok
            if not is_ok and (prev_ok or prev_ok is None):
                self.log_fn("NETWORK_GUARD: Wireproxy connection severed or port 10808 unresponsive!", level="WARN")

            # Runtime Double-VPN detection: alert if host system VPN app auto-connected in background
            if is_ok:
                host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
                if host_is_vpn:
                    provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface)
                    self._last_double_vpn_provider = provider
                    if not getattr(self, "_runtime_double_vpn_warned", False):
                        self._runtime_double_vpn_warned = True
                        self.log_fn(
                            f"NETWORK_GUARD: Active System VPN ({provider}) detected on host ({host_iface}) "
                            f"while Wireproxy is running! Nesting Wireproxy inside {provider} creates a double-VPN conflict. "
                            f"Please disconnect {provider} to maintain clean Wireproxy routing.",
                            level="WARN",
                        )
                        if self.beep_bad_fn:
                            self.beep_bad_fn(self.state)
                else:
                    if getattr(self, "_runtime_double_vpn_warned", False):
                        # Ensure internet is online so a transient link drop isn't mistaken for VPN turnoff
                        if NetworkRouteResolver.is_internet_online(timeout=1.0):
                            prev_provider = getattr(self, "_last_double_vpn_provider", "") or "System VPN"
                            self._runtime_double_vpn_warned = False
                            self._last_double_vpn_provider = ""
                            self.log_fn(
                                f"NETWORK_GUARD: Host System VPN ({prev_provider}) disconnected. "
                                f"Double-VPN conflict resolved; clean Wireproxy routing restored.",
                                level="INFO",
                            )
                            if self.log_good_beep_fn:
                                self.log_good_beep_fn("double_vpn_resolved")
                            if self.beep_good_fn:
                                self.beep_good_fn(self.state)
            return is_ok

        elif self.vpn_mode == "LEGACY_VPN":
            prev_is_vpn = self.last_route_is_vpn
            iface, is_vpn = NetworkRouteResolver.get_default_route_info()
            self.last_route_interface = iface
            self.last_route_is_vpn = is_vpn

            if not is_vpn:
                if prev_is_vpn or prev_is_vpn is None:
                    self.log_fn(f"NETWORK_GUARD: Default route severed from VPN interface ({iface or 'unknown'})!", level="WARN")
                return False

            return True

        return True

    def is_wireproxy_nested_in_vpn(self) -> Tuple[bool, str]:
        """
        Check if Wireproxy is currently running nested inside an active host system VPN.
        Returns: (is_nested, host_vpn_name)
        """
        if self.vpn_mode == "WIREPROXY":
            host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
            if host_is_vpn:
                provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface)
                return True, provider
        return False, ""

    def inter_chunk_gate_check(self, temp_dir: str = "./_temp_chunks") -> bool:
        """
        Inter-chunk route re-assessment (evaluates network route between chunks / attempts).
        - If Wireproxy was running and a host VPN (e.g. Surfshark) auto-connected:
          * If country matches required_country: seamlessly transitions to direct LEGACY_VPN,
            stops Wireproxy (unexporting proxy env vars), avoiding nested double-tunneling.
          * If country conflicts: flags conflict and enters VPN Hold.
        - If LEGACY_VPN was active and host VPN dropped:
          * If Wireproxy configs are available: automatically starts Wireproxy and transitions to WIREPROXY.
          * Otherwise enters VPN Hold.
        - If physical internet dropped: enters Internet Hold.
        Returns True if route is valid to launch next attempt, False if recording should stop.
        """
        if self.state and getattr(self.state, "stop_flag", False):
            return False

        if self.vpn_mode == "UNRESTRICTED":
            if not self.check_runtime_internet():
                time.sleep(1.5)
                if not self.check_runtime_internet():
                    self.log_fn("NETWORK_GUARD: Internet offline before chunk launch! Entering Hold state...", level="WARN")
                    return self.handle_internet_hold_state(is_startup=False)
            return True

        if not self.is_armed:
            return True

        # Check physical internet first
        if not NetworkRouteResolver.is_internet_online(timeout=1.5):
            time.sleep(1.5)
            if not NetworkRouteResolver.is_internet_online(timeout=1.5):
                self.log_fn("NETWORK_GUARD: Outbound internet not reachable before chunk launch! Entering Internet Hold...", level="WARN")
                return self.handle_internet_hold_state(is_startup=False)

        host_iface, host_is_vpn = NetworkRouteResolver.get_default_route_info()
        host_provider = NetworkRouteResolver.get_friendly_vpn_name(host_iface) if host_is_vpn else ""

        # Case 1: Currently in WIREPROXY mode, but host system VPN connected in background
        if self.vpn_mode == "WIREPROXY":
            if host_is_vpn:
                # Host VPN appeared! Invalidate GeoIP cache to prevent reading Wireproxy's cached entry
                NetworkRouteResolver.invalidate_cache()
                # Probe country to check for target match or conflict via direct host route,
                # bypassing Wireproxy's active proxy environment variables
                host_ip, host_loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0, force_direct=True)
                if host_loc == self.required_country:
                    self.log_fn(
                        f"NETWORK_GUARD: Active System VPN ({host_provider} | {host_iface}) verified on target country ({host_loc}). "
                        f"Seamlessly transitioning to direct VPN mode for next attempt.",
                        level="INFO",
                    )
                    self.vpn_mode = "LEGACY_VPN"
                    self.active_system_vpn = host_provider
                    self.launch_ip = host_ip
                    self.launch_country = host_loc
                    self.launch_interface = host_iface
                    self.launch_is_vpn = True
                    self.last_route_interface = host_iface
                    self.last_route_is_vpn = True
                    self.bypassed_wireproxy = True
                    if self.wireproxy_manager:
                        self.wireproxy_manager.stop()
                    for h_line in self.format_header_lines():
                        self.log_fn(h_line)
                    self.log_fn(
                        f"Network Route     : Connected [SYSTEM_VPN: {self.active_system_vpn}] "
                        f"(Country: {self.launch_country} | Adapter: {self.launch_interface} | IP: {self.launch_ip})",
                        level="INFO",
                    )
                    return True
                else:
                    self.log_fn(
                        f"NETWORK_GUARD: Host System VPN ({host_provider} | {host_iface}) connected to conflicting country "
                        f"({host_loc or 'Unknown'} != {self.required_country})! Diverting to VPN Hold...",
                        level="WARN",
                    )
                    self.vpn_conflict = True
                    self.conflict_country = host_loc
                    self.conflict_iface = host_iface
                    self.conflict_vpn_name = host_provider
                    return self.handle_vpn_hold_state(is_startup=False)
            else:
                # No host VPN, continue with Wireproxy check
                if not self.check_runtime_route():
                    time.sleep(1.5)
                    if not self.check_runtime_route():
                        self.log_fn("NETWORK_GUARD: Wireproxy route not verified before chunk launch! Entering Hold state...", level="WARN")
                        return self.handle_vpn_hold_state(is_startup=False)
                return True

        # Case 2: Currently in LEGACY_VPN mode
        elif self.vpn_mode == "LEGACY_VPN":
            if not host_is_vpn:
                # Host VPN dropped! Check if Wireproxy can auto-recover
                server_list = self.server_priority.get(self.required_country, [])
                wm = self.wireproxy_manager or WireproxyManager(
                    log_fn=self.log_fn,
                    raw_start_fn=self.raw_start_fn,
                    raw_end_fn=self.raw_end_fn,
                    raw_write_fn=self.raw_write_fn,
                )
                has_configs = any(wm.get_server_conf_path(s) for s in server_list) if server_list else False
                wireproxy_capable = bool(wm.wireproxy_bin and has_configs)
                if wireproxy_capable:
                    self.log_fn("NETWORK_GUARD: Host System VPN disconnected between chunks. Auto-engaging Wireproxy tunnel...", level="INFO")
                    primary_server = server_list[0] if server_list else "default"
                    self.wireproxy_manager = wm
                    if self.state:
                        setattr(self.state, "wireproxy_manager", wm)
                    if self.wireproxy_manager.start(primary_server, temp_dir):
                        proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                        ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=3.0, proxy_url=proxy_url)
                        if loc == self.required_country:
                            self.vpn_mode = "WIREPROXY"
                            self.active_system_vpn = None
                            self.launch_ip = ip
                            self.launch_country = loc
                            self.last_route_is_vpn = True
                            self.bypassed_wireproxy = False
                            for h_line in self.format_header_lines():
                                self.log_fn(h_line)
                            self.log_fn(
                                f"Network Route     : Connected [WIREPROXY] (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip})",
                                level="INFO",
                            )
                            return True
                        else:
                            self.wireproxy_manager.stop()
                    return self.handle_vpn_hold_state(is_startup=False)
                else:
                    self.log_fn(f"NETWORK_GUARD: Default route severed from VPN interface ({host_iface or 'unknown'})! Diverting to VPN Hold...", level="WARN")
                    return self.handle_vpn_hold_state(is_startup=False)
            else:
                # Host is still on VPN, verify route
                if not self.check_runtime_route():
                    time.sleep(1.5)
                    if not self.check_runtime_route():
                        self.log_fn("NETWORK_GUARD: VPN route not verified before chunk launch! Entering Hold state...", level="WARN")
                        return self.handle_vpn_hold_state(is_startup=False)
                return True

        return True

    def check_runtime_internet(self) -> bool:
        """In-flight internet connectivity test throttled to 5.0 seconds with debounced drop confirmation."""
        now = time.monotonic()
        if now - self.last_internet_check_mono < 5.0:
            return self.last_internet_status

        self.last_internet_check_mono = now
        status = NetworkRouteResolver.is_internet_online(timeout=1.5)
        if not status:
            # Debounce: brief 1.0s backoff and second confirmation probe to filter transient packet loss / jitter
            time.sleep(1.0)
            status = NetworkRouteResolver.is_internet_online(timeout=1.5)
        self.last_internet_status = status
        return status

    def inspect_stdout_line(self, line: str) -> bool:
        """
        Inspect downloader stdout line for HTTP 429 or 403 Forbidden.
        Returns True if sustained rate-limit threshold is crossed.
        """
        now = time.monotonic()
        raw = line.lower()

        if "403" in raw and ("forbidden" in raw or "status code does not indicate success: 403" in raw or "http error 403" in raw):
            self.last_403_incident_mono = now
            self.has_403_error = True
            if not self.blocked_403_ip:
                self.blocked_403_ip = self.launch_ip

        if "429" in raw and ("too many requests" in raw or "status code does not indicate success: 429" in raw or "http error 429" in raw):
            self.recent_429_timestamps.append(now)
            self.recent_429_timestamps = [t for t in self.recent_429_timestamps if now - t <= RATE_LIMIT_WINDOW_SEC]
            self.consecutive_429_count += 1

            if len(self.recent_429_timestamps) >= RATE_LIMIT_ERROR_THRESHOLD or self.consecutive_429_count >= 5:
                self.rate_limit_429_count += 1
                self.last_429_incident_mono = now
                self.recent_429_timestamps.clear()
                self.consecutive_429_count = 0
                return True
            return False

        if "recording" in raw or "waiting" in raw or "100%" in raw:
            self.consecutive_429_count = 0

        return False

    def handle_vpn_hold_state(self, is_startup: bool = False, blocked_ip: Optional[str] = None) -> bool:
        """
        30-Minute VPN Hold State: freezes traffic, sounds alarm,
        renders live countdown, and recovers automatically upon VPN restoration.
        """
        if self.state:
            setattr(self.state, "lenient_alarm_active", False)
        if self.vpn_mode == "WIREPROXY":
            if self.wireproxy_manager and self.wireproxy_manager.is_running() and self.wireproxy_manager.check_port_responsive():
                proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0, proxy_url=proxy_url)
                if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                    self.launch_ip = ip
                    self.launch_country = loc
                    self.last_route_is_vpn = True
                    self.blocked_403_ip = None
                    if self.state:
                        setattr(self.state, "network_guard_hold_active", False)
                        setattr(self.state, "vpn_drop_post_ack_silent", False)
                    self.log_fn(f"NETWORK_GUARD: Wireproxy route verified healthy (Country: {loc} | Server: {self.wireproxy_manager.active_server} | IP: {ip}). Bypassing Hold state.", level="INFO")
                    if not is_startup and self.resume_callback:
                        self.resume_callback(self.state)
                    return True
        else:
            cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
            if cur_is_vpn:
                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
                if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                    self.launch_ip = ip
                    self.launch_country = loc
                    self.launch_interface = cur_iface
                    self.launch_is_vpn = cur_is_vpn
                    self.last_route_interface = cur_iface
                    self.last_route_is_vpn = cur_is_vpn
                    provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                    self.active_system_vpn = provider
                    self.blocked_403_ip = None
                    if self.state:
                        setattr(self.state, "network_guard_hold_active", False)
                        setattr(self.state, "vpn_drop_post_ack_silent", False)
                    self.log_fn(f"NETWORK_GUARD: VPN route verified healthy (Country: {loc} | Provider: {provider} | IP: {ip}). Bypassing Hold state.", level="INFO")
                    if not is_startup and self.resume_callback:
                        self.resume_callback(self.state)
                    return True

        self.hold_active = True
        if self.state:
            setattr(self.state, "network_guard_hold_active", True)
            setattr(self.state, "lenient_alarm_active", False)
        self.vpn_drop_count += 1
        hold_start_mono = time.monotonic()

        is_scheduled_run = bool(self.state and getattr(self.state, "is_scheduled", False))
        should_alarm = (is_scheduled_run if is_startup else True)
        provider_name = self.active_system_vpn or "System VPN"

        if blocked_ip:
            banner = [
                "====================================================================",
                f"[NETWORK_GUARD_HOLD] Egress IP {blocked_ip} rejected with HTTP 403 Forbidden!",
                f"Broadcaster CDN has flagged this {provider_name} server IP.",
                f"Please reconnect or switch location in {provider_name} to acquire a fresh IP.",
                f"Target Country : {self.required_country}",
                "Holding for up to 30 minutes for IP rotation / VPN restoration...",
            ]
        else:
            banner = [
                "====================================================================",
                "[NETWORK_GUARD_HOLD] VPN route severed or country mismatch!",
                f"Target Country : {self.required_country}",
                "Active Chunk   : Safely sealed to disk (0 packets leaked outside VPN)",
                "Holding for up to 30 minutes for VPN restoration...",
            ]
        already_silenced = bool(self.state and getattr(self.state, "vpn_drop_post_ack_silent", False))
        if should_alarm and not already_silenced:
            banner.append("Press 'A' to silence audio alarm.")
        elif already_silenced:
            banner.append("Audio alarm already silenced by operator.")
        else:
            banner.append("Connect VPN to resume, or press Ctrl+C to cancel.")
        banner.append("====================================================================")

        for b_line in banner:
            self.log_fn(b_line, level="WARN")

        internet_active = bool(self.state and (getattr(self.state, "internet_hold_active", False) or getattr(self.state, "internet_drop_active", False)))
        if should_alarm and not already_silenced and not internet_active and self.alarm_start_fn:
            self.alarm_start_fn(self.state, alarm_type="vpn_drop")
        else:
            if not already_silenced and not internet_active and self.beep_bad_fn:
                self.beep_bad_fn(self.state)

        last_wait_len = 0
        alarm_silenced = already_silenced

        while True:
            now = time.monotonic()

            if self.state and getattr(self.state, "stop_flag", False):
                self.vpn_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if self.state:
                    setattr(self.state, "network_guard_hold_active", False)
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                self.clear_progress_fn()
                return False

            if self.state and getattr(self.state, "alarm_ack_requested", False):
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                setattr(self.state, "alarm_ack_requested", False)
                setattr(self.state, "vpn_drop_post_ack_silent", True)
                alarm_silenced = True
                self.clear_progress_fn()
                self.log_fn("ALARM_ACK (VPN Hold audio alarm silenced by operator)", level="INFO")

            elapsed = now - hold_start_mono
            if elapsed >= self.hold_timeout_sec:
                self.vpn_hold_total_duration += elapsed
                self.hold_active = False
                if self.state:
                    setattr(self.state, "network_guard_hold_active", False)
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                self.clear_progress_fn()
                self.log_fn("NETWORK_GUARD: 30-minute VPN Hold timed out without restoration. Finalizing recording.", level="ERROR")
                return False

            rem = max(0.0, self.hold_timeout_sec - elapsed)
            alarm_hint = " (Alarm silenced)" if (alarm_silenced or (self.state and getattr(self.state, "vpn_drop_post_ack_silent", False))) else (" (Press 'A' to silence alarm)" if should_alarm else "")
            if blocked_ip:
                hold_text = f"[NETWORK_GUARD_HOLD] Waiting for fresh IP != {blocked_ip} ({self.required_country})... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            else:
                hold_text = f"[NETWORK_GUARD_HOLD] Waiting for VPN ({self.required_country})... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            pad = max(0, last_wait_len - len(hold_text))
            self.render_progress_fn(hold_text, pad=pad)
            last_wait_len = len(hold_text)

            if self.vpn_mode == "WIREPROXY":
                if int(elapsed) % 10 == 0:
                    if self.attempt_wireguard_auto_recovery(blocked_ip=blocked_ip):
                        self.clear_progress_fn()
                        self.vpn_hold_total_duration += (now - hold_start_mono)
                        self.hold_active = False
                        self.blocked_403_ip = None
                        if self.state:
                            setattr(self.state, "network_guard_hold_active", False)
                            setattr(self.state, "vpn_drop_post_ack_silent", False)
                        if self.alarm_stop_fn:
                            self.alarm_stop_fn(self.state)
                        if self.log_good_beep_fn:
                            self.log_good_beep_fn("vpn_restored")
                        if self.beep_good_fn:
                            self.beep_good_fn(self.state)
                        self.log_fn(
                            f"NETWORK_GUARD: Wireproxy connection restored (Server: {self.wireproxy_manager.active_server} | IP: {self.launch_ip}). Guardrail cleared.",
                            level="INFO",
                        )
                        if not is_startup and self.resume_callback:
                            self.resume_callback(self.state)
                        return True
            else:
                cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
                if cur_is_vpn:
                    ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=2.0)
                    if loc == self.required_country and (not blocked_ip or ip != blocked_ip):
                        self.clear_progress_fn()
                        self.vpn_hold_total_duration += (now - hold_start_mono)
                        self.hold_active = False
                        self.launch_ip = ip
                        self.launch_country = loc
                        self.launch_interface = cur_iface
                        self.launch_is_vpn = cur_is_vpn
                        self.last_route_interface = cur_iface
                        self.last_route_is_vpn = cur_is_vpn
                        provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                        self.active_system_vpn = provider
                        self.blocked_403_ip = None
                        if self.state:
                            setattr(self.state, "network_guard_hold_active", False)
                            setattr(self.state, "vpn_drop_post_ack_silent", False)
                        if self.alarm_stop_fn:
                            self.alarm_stop_fn(self.state)
                        if self.log_good_beep_fn:
                            self.log_good_beep_fn("vpn_restored")
                        if self.beep_good_fn:
                            self.beep_good_fn(self.state)
                        self.log_fn(
                            f"NETWORK_GUARD: VPN connection restored (Country: {loc} | Provider: {provider} | IP: {ip}). Guardrail cleared.",
                            level="INFO",
                        )
                        if not is_startup and self.resume_callback:
                            self.resume_callback(self.state)
                        return True

            time.sleep(1.0)

    def handle_403_recovery(self, is_startup: bool = False) -> bool:
        """Unified 403 Forbidden (CDN IP-Block / Geo-Block) Recovery Protocol."""
        blocked_ip = self.blocked_403_ip or self.launch_ip
        if not blocked_ip:
            ip, _ = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5)
            blocked_ip = ip or "unknown"
            self.blocked_403_ip = blocked_ip

        provider_name = self.active_system_vpn or "System VPN"
        self.log_fn(f"NETWORK_GUARD: Broadcaster CDN rejected egress IP ({blocked_ip}) with HTTP 403 Forbidden!", level="WARN")

        if self.vpn_mode == "WIREPROXY" and self.wireproxy_manager:
            self.log_fn(f"NETWORK_GUARD: Initiating Wireproxy 3-2-1 IP pool rotation to evade 403 block on {blocked_ip}...", level="INFO")
            if self.attempt_wireguard_auto_recovery(blocked_ip=blocked_ip):
                self.log_fn(f"NETWORK_GUARD: Successfully acquired fresh IP ({self.launch_ip}) != {blocked_ip} via Wireproxy. Resuming recording.", level="INFO")
                self.has_403_error = False
                self.blocked_403_ip = None
                return True
            self.log_fn("NETWORK_GUARD: Wireproxy 3-2-1 pool rotation failed to find unblocked IP.", level="WARN")

        self.log_fn(f"NETWORK_GUARD: Diverting to 30-minute VPN Hold for IP refresh in {provider_name}...", level="WARN")
        recovered = self.handle_vpn_hold_state(is_startup=is_startup, blocked_ip=blocked_ip)
        if recovered:
            self.has_403_error = False
            self.blocked_403_ip = None
            return True
        return False

    def handle_rate_limit_cooldown(self) -> bool:
        """Three-Strike HTTP 429 Lifecycle."""
        self.rate_limit_strike += 1
        provider_name = self.active_system_vpn or "System VPN"
        current_ip = self.launch_ip or "unknown"

        if self.rate_limit_strike == 1:
            cooldown_start = time.monotonic()
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 1/3] HTTP 429 detected from Broadcaster CDN edge.",
                "Active chunk safely sealed to disk.",
                f"Halting requests for {int(RATE_LIMIT_COOLDOWN_SEC)}s cooldown to allow server rate limits to reset...",
                "====================================================================",
            ]
            for b_line in banner:
                self.log_fn(b_line, level="WARN")

            total_cd = float(RATE_LIMIT_COOLDOWN_SEC)
            last_cd_len = 0

            while True:
                now = time.monotonic()
                if self.state and getattr(self.state, "stop_flag", False):
                    self.clear_progress_fn()
                    return False

                elapsed = now - cooldown_start
                rem = max(0.0, total_cd - elapsed)
                if rem <= 0.0:
                    break

                cd_text = f"[RATE_LIMIT_COOLDOWN] Strike 1/3: CDN 429 backoff... Cooldown remaining: {int(rem)}s"
                pad = max(0, last_cd_len - len(cd_text))
                self.render_progress_fn(cd_text, pad=pad)
                last_cd_len = len(cd_text)
                time.sleep(1.0)

            self.clear_progress_fn()
            self.log_fn("RATE_LIMIT: 90s cooldown completed. Resuming next chunk attempt.", level="INFO")
            return True

        elif self.rate_limit_strike == 2:
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 2/3] HTTP 429 persisted after 90s cooldown!",
                f"Broadcaster CDN is enforcing persistent rate-limiting on egress IP ({current_ip}).",
                "Rotating egress IP to clear rate limit bucket...",
                "====================================================================",
            ]
            for b_line in banner:
                self.log_fn(b_line, level="WARN")

            if self.vpn_mode == "WIREPROXY" and self.wireproxy_manager:
                self.log_fn(f"NETWORK_GUARD: Initiating Wireproxy 3-2-1 IP rotation to clear rate limit on {current_ip}...", level="INFO")
                if self.attempt_wireguard_auto_recovery(blocked_ip=current_ip):
                    self.log_fn(f"NETWORK_GUARD: Fresh IP ({self.launch_ip}) acquired via Wireproxy. Strike 2 cleared; resuming next chunk.", level="INFO")
                    return True
                self.log_fn("NETWORK_GUARD: Wireproxy pool rotation exhausted.", level="WARN")

            self.log_fn(f"NETWORK_GUARD: Diverting to 30-minute VPN Hold for IP refresh in {provider_name}...", level="WARN")
            recovered = self.handle_vpn_hold_state(is_startup=False, blocked_ip=current_ip)
            if recovered:
                self.log_fn(f"NETWORK_GUARD: Fresh IP ({self.launch_ip}) acquired via {provider_name}. Strike 2 cleared; resuming next chunk.", level="INFO")
                return True
            return False

        else:
            banner = [
                "====================================================================",
                "[RATE-LIMIT STRIKE 3/3] HTTP 429 persisted across multiple IP addresses!",
                "Broadcaster CDN has locked account concurrency or expired stream token.",
                "Further IP rotation cannot resolve account-level concurrency limits.",
                "Terminating recording with critical alarm.",
                "====================================================================",
            ]
            for b_line in banner:
                self.log_fn(b_line, level="ERROR")

            if self.alarm_start_fn:
                self.alarm_start_fn(self.state, alarm_type="critical")
            return False

    def handle_internet_hold_state(self, is_startup: bool = False) -> bool:
        """30-Minute General Internet Hold for general outages."""
        if self.state:
            setattr(self.state, "lenient_alarm_active", False)
        if NetworkRouteResolver.is_internet_online(timeout=2.0):
            if self.state:
                setattr(self.state, "internet_hold_active", False)
                setattr(self.state, "internet_drop_active", False)
            self.log_fn("NETWORK_GUARD: Outbound internet verified online. Bypassing Internet Hold state.", level="INFO")
            if not is_startup and self.resume_callback:
                self.resume_callback(self.state)
            return True

        self.hold_active = True
        if self.state:
            setattr(self.state, "internet_hold_active", True)
            setattr(self.state, "lenient_alarm_active", False)
            setattr(self.state, "vpn_drop_post_ack_silent", False)
        self.internet_drop_count += 1
        hold_start_mono = time.monotonic()
        is_scheduled_run = bool(self.state and getattr(self.state, "is_scheduled", False))
        should_alarm = (is_scheduled_run if is_startup else True)

        banner = [
            "====================================================================",
            "[INTERNET_HOLD] Outbound internet connectivity lost!",
            "Active Chunk   : Safely sealed to disk",
            "Holding for up to 30 minutes for internet restoration...",
        ]
        already_silenced = bool(self.state and getattr(self.state, "internet_drop_post_ack_silent", False))
        if should_alarm and not already_silenced:
            banner.append("Press 'A' to silence audio alarm.")
        elif already_silenced:
            banner.append("Audio alarm already silenced by operator.")
        else:
            banner.append("Restore internet to resume, or press Ctrl+C to cancel.")
        banner.append("====================================================================")

        for b_line in banner:
            self.log_fn(b_line, level="WARN")

        if should_alarm and not already_silenced and self.alarm_start_fn:
            self.alarm_start_fn(self.state, alarm_type="internet_drop")
        else:
            if not already_silenced and self.beep_bad_fn:
                self.beep_bad_fn(self.state)

        last_wait_len = 0
        alarm_silenced = already_silenced

        while True:
            now = time.monotonic()
            if self.state and getattr(self.state, "stop_flag", False):
                self.internet_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if self.state:
                    setattr(self.state, "internet_hold_active", False)
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                self.clear_progress_fn()
                return False

            if self.state and getattr(self.state, "alarm_ack_requested", False):
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                setattr(self.state, "alarm_ack_requested", False)
                setattr(self.state, "internet_drop_post_ack_silent", True)
                alarm_silenced = True
                self.clear_progress_fn()
                self.log_fn("ALARM_ACK (Internet Hold audio alarm silenced by operator)", level="INFO")

            elapsed = now - hold_start_mono
            if elapsed >= self.hold_timeout_sec:
                self.internet_hold_total_duration += elapsed
                self.hold_active = False
                if self.state:
                    setattr(self.state, "internet_hold_active", False)
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                self.clear_progress_fn()
                self.log_fn("NETWORK_GUARD: 30-minute Internet Hold timed out without restoration. Finalizing recording.", level="ERROR")
                return False

            rem = max(0.0, self.hold_timeout_sec - elapsed)
            alarm_hint = " (Alarm silenced)" if (alarm_silenced or (self.state and getattr(self.state, "internet_drop_post_ack_silent", False))) else (" (Press 'A' to silence alarm)" if should_alarm else "")
            hold_text = f"[INTERNET_HOLD] Waiting for internet restoration... Timeout remaining: {fmt_hms(rem)}{alarm_hint}"
            pad = max(0, last_wait_len - len(hold_text))
            self.render_progress_fn(hold_text, pad=pad)
            last_wait_len = len(hold_text)

            if NetworkRouteResolver.is_internet_online(timeout=1.5):
                self.clear_progress_fn()

                # If stream requires a VPN, settle routing table / allow VPN client up to 15s to establish tunnel
                if self.is_armed:
                    vpn_settle_start = time.monotonic()
                    last_settle_len = 0
                    vpn_settled = False
                    internet_re_dropped = False
                    self.log_fn("NETWORK_GUARD: Outbound internet connectivity restored! Waiting up to 15s for VPN tunnel to settle...", level="INFO")
                    while True:
                        now_s = time.monotonic()
                        if self.state and getattr(self.state, "stop_flag", False):
                            self.internet_hold_total_duration += (now - hold_start_mono)
                            self.hold_active = False
                            return False

                        # If internet cuts out again while waiting for VPN, drop back to internet hold loop
                        if not NetworkRouteResolver.is_internet_online(timeout=1.0):
                            self.clear_progress_fn()
                            self.log_fn("NETWORK_GUARD: Internet dropped again during VPN settling window! Resuming Internet Hold...", level="WARN")
                            internet_re_dropped = True
                            break

                        # Check if VPN tunnel is established and valid country
                        if self.vpn_mode == "WIREPROXY":
                            if self.wireproxy_manager and self.wireproxy_manager.is_running() and self.wireproxy_manager.check_port_responsive(timeout=0.2):
                                proxy_url = f"http://127.0.0.1:{self.wireproxy_manager.http_port}"
                                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5, proxy_url=proxy_url)
                                if loc == self.required_country:
                                    self.launch_ip = ip
                                    self.launch_country = loc
                                    self.last_route_is_vpn = True
                                    vpn_settled = True
                                    break
                        else:
                            cur_iface, cur_is_vpn = NetworkRouteResolver.get_default_route_info()
                            if cur_is_vpn:
                                ip, loc = NetworkRouteResolver.get_public_ip_and_country(timeout=1.5)
                                if loc == self.required_country:
                                    self.launch_ip = ip
                                    self.launch_country = loc
                                    self.launch_interface = cur_iface
                                    self.launch_is_vpn = cur_is_vpn
                                    self.last_route_interface = cur_iface
                                    self.last_route_is_vpn = cur_is_vpn
                                    provider = NetworkRouteResolver.get_friendly_vpn_name(cur_iface)
                                    self.active_system_vpn = provider
                                    vpn_settled = True
                                    break

                        s_elapsed = now_s - vpn_settle_start
                        if s_elapsed >= 15.0:
                            break

                        s_rem = max(0.0, 15.0 - s_elapsed)
                        settle_text = f"[NETWORK_GUARD] Internet online. Waiting for VPN ({self.required_country})... Window remaining: {int(s_rem)}s"
                        pad_s = max(0, last_settle_len - len(settle_text))
                        self.render_progress_fn(settle_text, pad=pad_s)
                        last_settle_len = len(settle_text)
                        time.sleep(0.5)

                    self.clear_progress_fn()
                    if internet_re_dropped:
                        continue

                    if not vpn_settled:
                        # VPN failed to reconnect within 15 seconds. Transition smoothly to VPN Hold!
                        self.internet_hold_total_duration += (time.monotonic() - hold_start_mono)
                        self.hold_active = False
                        if self.state:
                            setattr(self.state, "internet_hold_active", False)
                            setattr(self.state, "internet_drop_active", False)
                        if self.alarm_stop_fn:
                            self.alarm_stop_fn(self.state)
                        self.log_fn("NETWORK_GUARD: Internet restored but VPN tunnel did not establish within 15s. Diverting to VPN Hold...", level="WARN")
                        return self.handle_vpn_hold_state(is_startup=is_startup)

                # Both Internet (and VPN if armed) are verified healthy!
                self.internet_hold_total_duration += (now - hold_start_mono)
                self.hold_active = False
                if self.state:
                    setattr(self.state, "internet_hold_active", False)
                    setattr(self.state, "internet_drop_active", False)
                    setattr(self.state, "internet_drop_post_ack_silent", False)
                if self.alarm_stop_fn:
                    self.alarm_stop_fn(self.state)
                if self.log_good_beep_fn:
                    self.log_good_beep_fn("recovery_healthy")
                if self.beep_good_fn:
                    self.beep_good_fn(self.state)
                if self.is_armed:
                    if self.vpn_mode == "WIREPROXY":
                        server_name = self.wireproxy_manager.active_server if self.wireproxy_manager else "wireproxy"
                        vpn_desc = f"[WIREPROXY] (Country: {self.launch_country} | Server: {server_name} | IP: {self.launch_ip})"
                    else:
                        vpn_desc = f"[SYSTEM_VPN: {self.active_system_vpn or 'VPN'}] (Country: {self.launch_country} | Adapter: {self.launch_interface or 'default'} | IP: {self.launch_ip})"
                    self.log_fn(f"NETWORK_GUARD: Outbound internet and network route verified healthy {vpn_desc}! Guardrail cleared.", level="INFO")
                else:
                    self.log_fn("NETWORK_GUARD: Outbound internet verified online! Guardrail cleared.", level="INFO")
                if not is_startup and self.resume_callback:
                    self.resume_callback(self.state)
                return True

            time.sleep(1.0)

    def supervise_in_flight(
        self,
        proc: Optional[subprocess.Popen] = None,
        temp_dir: str = "./_temp_chunks",
        stop_event: Optional[threading.Event] = None,
    ) -> Optional[str]:
        """
        In-stream zero-traffic route monitoring and universal internet supervision.
        Called once per second while child downloader is actively writing chunks.

        Returns:
            None: Stream route and physical internet are healthy (or absorbing lenient retry).
            "vpn_drop": Strict route drop occurred; child process terminated to seal chunk.
            "internet_drop": Physical internet died during lenient retry; process terminated to seal chunk.
        """
        # 1. Route monitoring (if armed and session not already stopped)
        if self.is_armed and not (self.state and getattr(self.state, "stop_flag", False)):
            if not self.check_runtime_route():
                # STAGE 1: Attempt Silent WireGuard Auto-Recovery
                if self.attempt_wireguard_auto_recovery(temp_dir=temp_dir):
                    pass
                # STAGE 2: Fallback Guardrail (STRICT vs LENIENT)
                elif self.is_lenient_drop_allowed():
                    # Check physical internet FIRST before sounding any VPN drop alarm!
                    if not NetworkRouteResolver.is_internet_online_debounced(
                        required_consecutive_failures=2,
                        retry_delay=1.5,
                        timeout_per_probe=1.5,
                    ):
                        self.log_fn(
                            "NETWORK_GUARD: [LENIENT] Outbound internet severed while in VPN drop! Aborting retry to seal chunk...",
                            level="WARN",
                        )
                        if stop_event:
                            stop_event.set()
                        if proc:
                            proc.terminate()
                            try:
                                proc.wait(timeout=3)
                            except subprocess.TimeoutExpired:
                                proc.kill()
                                proc.wait()
                        return "internet_drop"

                    # Outbound internet is verified UP: this is a genuine VPN-only drop
                    if not (self.state and getattr(self.state, "lenient_alarm_active", False)):
                        if self.state:
                            setattr(self.state, "lenient_alarm_active", True)
                        if not (self.state and getattr(self.state, "vpn_drop_post_ack_silent", False)):
                            self.log_fn(
                                "NETWORK_GUARD: [STAGE 2 - LENIENT] VPN dropped! Sounding alarm. Downloader allowed to continue retrying...",
                                level="WARN",
                            )
                            if self.alarm_start_fn:
                                self.alarm_start_fn(self.state, alarm_type="vpn_drop")
                else:
                    # In STRICT mode, terminate child process immediately to prevent non-compliant egress
                    if stop_event:
                        stop_event.set()
                    if proc:
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except subprocess.TimeoutExpired:
                            proc.kill()
                            proc.wait()

                    # Classify root cause post-termination: if physical internet died, divert to internet_drop
                    if not NetworkRouteResolver.is_internet_online_debounced(
                        required_consecutive_failures=2,
                        retry_delay=1.5,
                        timeout_per_probe=1.5,
                    ):
                        self.log_fn(
                            "NETWORK_GUARD: [STRICT] Outbound internet severed! Sealed chunk to disk.",
                            level="WARN",
                        )
                        return "internet_drop"

                    if self.state:
                        setattr(self.state, "network_guard_triggered", True)
                    self.log_fn(
                        "NETWORK_GUARD: [STAGE 2 - STRICT] Default route severed from VPN interface! Terminating child process to seal chunk...",
                        level="WARN",
                    )
                    return "vpn_drop"
            else:
                # Route verified healthy: clear lenient alarm if it was active
                if self.state and (
                    getattr(self.state, "lenient_alarm_active", False)
                    or getattr(self.state, "vpn_drop_post_ack_silent", False)
                ):
                    setattr(self.state, "lenient_alarm_active", False)
                    setattr(self.state, "vpn_drop_post_ack_silent", False)
                    if self.alarm_stop_fn:
                        self.alarm_stop_fn(self.state)
                    if self.log_good_beep_fn:
                        self.log_good_beep_fn("vpn_restored")
                    if self.beep_good_fn:
                        self.beep_good_fn(self.state)
                    self.log_fn(
                        "NETWORK_GUARD: [LENIENT] VPN connection restored while downloader active! Resuming seamless recording.",
                        level="INFO",
                    )

        # 2. Universal physical internet supervision across all stream modes
        if not (self.state and getattr(self.state, "stop_flag", False)):
            if not self.check_runtime_internet():
                if not (self.state and getattr(self.state, "internet_drop_active", False)):
                    if self.state:
                        setattr(self.state, "internet_drop_active", True)
                    if not (self.state and getattr(self.state, "internet_drop_post_ack_silent", False)):
                        self.log_fn(
                            "NETWORK_GUARD: [LENIENT] Outbound internet dropped! Sounding alarm. Downloader allowed to continue retrying...",
                            level="WARN",
                        )
                        if self.alarm_start_fn:
                            self.alarm_start_fn(self.state, alarm_type="internet_drop")
            else:
                if self.state and (
                    getattr(self.state, "internet_drop_active", False)
                    or getattr(self.state, "internet_drop_post_ack_silent", False)
                ):
                    setattr(self.state, "internet_drop_active", False)
                    setattr(self.state, "internet_drop_post_ack_silent", False)
                    if self.alarm_stop_fn:
                        self.alarm_stop_fn(self.state)
                    if self.log_good_beep_fn:
                        self.log_good_beep_fn("internet_restored")
                    if self.beep_good_fn:
                        self.beep_good_fn(self.state)
                    self.log_fn(
                        "NETWORK_GUARD: [LENIENT] Internet connection restored while downloader active! Resuming seamless recording.",
                        level="INFO",
                    )

        return None


def create_network_guard(
    state,
    *,
    required_country: Optional[str] = None,
    drop_policy: str = "LENIENT",
    detection_source: str = "config",
    server_priority: Optional[Dict[str, List[str]]] = None,
    log_fn: Callable = default_log,
    render_progress_fn: Callable = default_render_progress,
    clear_progress_fn: Callable = default_clear_progress,
    alarm_start_fn: Optional[Callable] = None,
    alarm_stop_fn: Optional[Callable] = None,
    beep_good_fn: Optional[Callable] = None,
    beep_bad_fn: Optional[Callable] = None,
    log_good_beep_fn: Optional[Callable] = None,
    raw_start_fn: Optional[Callable] = None,
    raw_end_fn: Optional[Callable] = None,
    raw_write_fn: Optional[Callable] = None,
    temp_dir: str = "./_temp_chunks",
) -> Tuple[NetworkGuard, bool]:
    """
    Factory helper to instantiate NetworkGuard, attach to state,
    print contiguous startup header lines, and execute preflight check.
    """
    guard = NetworkGuard(
        required_country=required_country,
        drop_policy=drop_policy,
        detection_source=detection_source,
        server_priority=server_priority,
        log_fn=log_fn,
        render_progress_fn=render_progress_fn,
        clear_progress_fn=clear_progress_fn,
        alarm_start_fn=alarm_start_fn,
        alarm_stop_fn=alarm_stop_fn,
        beep_good_fn=beep_good_fn,
        beep_bad_fn=beep_bad_fn,
        log_good_beep_fn=log_good_beep_fn,
        raw_start_fn=raw_start_fn,
        raw_end_fn=raw_end_fn,
        raw_write_fn=raw_write_fn,
        state=state,
    )
    if state:
        setattr(state, "network_guard", guard)
        setattr(state, "wireproxy_manager", guard.wireproxy_manager)

    for g_line in guard.format_header_lines():
        log_fn(g_line)

    guard_ok = guard.preflight_check(temp_dir=temp_dir, is_scheduled=False)
    return guard, guard_ok

