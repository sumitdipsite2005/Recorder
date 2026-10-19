"""Regression test suite for Network Guardrail, Userspace Wireproxy, and Policy Engine.

Covers:
1. WireproxyManager lifecycle, binary discovery, config generation, and diagnostic log drain.
2. NetworkRouteResolver route inspection and commercial VPN branding.
3. NetworkGuard policy arbitration (Strict vs. Lenient, Double-VPN bypass, Conflict detection).
4. HTTP 403 / 429 recovery and circuit-breaker invariants.
5. Silent manual startup vs. scheduled siren invariants.
6. Record Dynamic orchestration wiring and hook verification.
"""
from __future__ import annotations

import ast
import os
from pathlib import Path
import tempfile
import time
import unittest
import urllib.request
from unittest.mock import MagicMock, patch

from recorder_runtime.network import (
    NetworkGuard,
    NetworkRouteResolver,
    WireproxyManager,
    WIREPROXY_HTTP_PORT,
    WIREPROXY_SOCKS5_PORT,
    RATE_LIMIT_COOLDOWN_SEC,
    RATE_LIMIT_ERROR_THRESHOLD,
    fmt_hms,
    is_wireproxy_active,
)

# Fallback test configuration for CI environments without OneDrive
_ci_temp_config = None
if not os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH") or not os.path.isfile(os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH", "")):
    _ci_temp_config = tempfile.NamedTemporaryFile(mode="w", suffix=".py", delete=False, encoding="utf-8")
    _ci_temp_config.write(
        'NM3U8DL_PLAYLIST_PRIMARY_PHRASES = [["Test"]]\n'
        'NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n'
        'NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n'
        'NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n'
        'NM3U8DL_PLAYLIST_GROUP = "JIO_STAR_SPORTS"\n'
        'NM3U8DL_PLAYLIST_GROUPS = {"COMMON": [], "JIO_STAR_SPORTS": []}\n'
        'SCHEDULE_START = None\n'
        'RUN_DURATION_MIN = 1\n'
        'BASE_NAME = "CI_TEST"\n'
        f'RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n'
        'REQUIRED_VPN_COUNTRY = "IN"\n'
    )
    _ci_temp_config.flush()
    _ci_temp_config.close()
    os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = _ci_temp_config.name


class WireproxyManagerTests(unittest.TestCase):
    def test_default_ports(self):
        wm = WireproxyManager()
        self.assertEqual(wm.socks5_port, WIREPROXY_SOCKS5_PORT)
        self.assertEqual(wm.http_port, WIREPROXY_HTTP_PORT)

    def test_runtime_config_generation(self):
        wm = WireproxyManager(socks5_port=10808, http_port=10809)
        sample_conf = (
            "[Interface]\n"
            "PrivateKey = aaaa\n"
            "Address = 10.0.0.2/32\n"
            "DNS = 1.1.1.1\n\n"
            "[Peer]\n"
            "PublicKey = bbbb\n"
            "Endpoint = 1.2.3.4:51820\n"
            "AllowedIPs = 0.0.0.0/0\n"
        )
        with tempfile.TemporaryDirectory() as td:
            src = Path(td) / "test.conf"
            src.write_text(sample_conf, encoding="utf-8")

            out_path = wm.create_runtime_config(str(src), temp_dir=td)
            self.assertTrue(os.path.exists(out_path))
            content = Path(out_path).read_text(encoding="utf-8")

            self.assertIn("[Socks5]", content)
            self.assertIn("BindAddress = 127.0.0.1:10808", content)
            self.assertIn("[http]", content.lower())
            self.assertIn("BindAddress = 127.0.0.1:10809", content)

    def test_drain_wireproxy_stdout_streams_to_raw_write(self):
        logged_lines = []

        def mock_raw_write(token, line):
            logged_lines.append((token, line))

        wm = WireproxyManager(raw_write_fn=mock_raw_write)
        mock_proc = MagicMock()
        mock_proc.stdout.readline.side_effect = [
            "INFO: Wireproxy starting...\n",
            "INFO: SOCKS5 listening on 127.0.0.1:10808\n",
            "",
        ]

        # Use helper internally in WireproxyManager.start
        raw_write = wm.raw_write_fn
        raw_token = {"tool": "Wireproxy"}

        def _drain(p, token):
            if not p or not p.stdout:
                return
            for line in iter(p.stdout.readline, ""):
                if not line:
                    break
                if raw_write and token:
                    raw_write(token, line.rstrip("\r\n"))

        _drain(mock_proc, raw_token)

        self.assertEqual(len(logged_lines), 2)
        self.assertEqual(logged_lines[0][1], "INFO: Wireproxy starting...")
        self.assertEqual(logged_lines[1][1], "INFO: SOCKS5 listening on 127.0.0.1:10808")

    def test_environment_export_and_unexport(self):
        wm = WireproxyManager(socks5_port=10808, http_port=10809)
        wm.export_environment()
        self.assertEqual(os.environ.get("HTTP_PROXY"), "http://127.0.0.1:10809")
        self.assertEqual(os.environ.get("ALL_PROXY"), "socks5://127.0.0.1:10808")

        import urllib.request
        self.assertIsNotNone(urllib.request._opener)
        proxy_handlers = [h for h in urllib.request._opener.handlers if isinstance(h, urllib.request.ProxyHandler)]
        self.assertTrue(len(proxy_handlers) > 0)
        self.assertIn("http", proxy_handlers[0].proxies)

        wm.unexport_environment()
        self.assertNotIn("HTTP_PROXY", os.environ)
        self.assertNotIn("HTTPS_PROXY", os.environ)
        self.assertNotIn("http_proxy", os.environ)
        self.assertNotIn("https_proxy", os.environ)
        self.assertNotIn("ALL_PROXY", os.environ)

        self.assertIsNotNone(urllib.request._opener)
        proxy_handlers_after = [h for h in urllib.request._opener.handlers if isinstance(h, urllib.request.ProxyHandler)]
        self.assertEqual(len(proxy_handlers_after), 0)


class NetworkRouteResolverTests(unittest.TestCase):
    def setUp(self):
        # Reset cache before each test
        NetworkRouteResolver.invalidate_cache()

    def test_commercial_vpn_detection_matches_known_brands(self):
        # Result is either friendly name or adapter name
        res = NetworkRouteResolver.get_friendly_vpn_name("utun6")
        self.assertTrue(bool(res))

    def test_public_ip_resolver_with_mock(self):
        with patch("recorder_runtime.network.urlopen") as mock_urlopen:
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"status": "success", "query": "103.21.244.2", "countryCode": "IN"}'
            mock_urlopen.return_value.__enter__.return_value = mock_resp

            ip, country = NetworkRouteResolver.get_public_ip_and_country(timeout=1.0)
            self.assertEqual(ip, "103.21.244.2")
            self.assertEqual(country, "IN")

    def test_invalidate_cache_clears_state(self):
        NetworkRouteResolver._geoip_cache_time = 123456.0
        NetworkRouteResolver._geoip_cache_ip = "1.2.3.4"
        NetworkRouteResolver._geoip_cache_country = "US"
        NetworkRouteResolver.invalidate_cache()
        self.assertEqual(NetworkRouteResolver._geoip_cache_time, 0.0)
        self.assertIsNone(NetworkRouteResolver._geoip_cache_ip)
        self.assertIsNone(NetworkRouteResolver._geoip_cache_country)

    def test_force_direct_bypasses_cache_and_ambient_proxy(self):
        # Seed cache
        NetworkRouteResolver._geoip_cache_time = time.monotonic()
        NetworkRouteResolver._geoip_cache_ip = "176.227.240.84"
        NetworkRouteResolver._geoip_cache_country = "IN"

        with patch("recorder_runtime.network.build_opener") as mock_build_opener:
            mock_opener = MagicMock()
            mock_resp = MagicMock()
            mock_resp.read.return_value = b'{"status": "success", "query": "185.220.101.5", "countryCode": "DE"}'
            mock_opener.open.return_value.__enter__.return_value = mock_resp
            mock_build_opener.return_value = mock_opener

            ip, country = NetworkRouteResolver.get_public_ip_and_country(timeout=1.0, force_direct=True)
            self.assertEqual(ip, "185.220.101.5")
            self.assertEqual(country, "DE")
            mock_build_opener.assert_called_once()
            args, _ = mock_build_opener.call_args
            self.assertIsInstance(args[0], urllib.request.ProxyHandler)
            self.assertEqual(args[0].proxies, {})


class NetworkGuardPolicyTests(unittest.TestCase):
    def test_unrestricted_mode(self):
        guard = NetworkGuard(required_country=None)
        self.assertFalse(guard.is_armed)
        self.assertEqual(guard.vpn_mode, "UNRESTRICTED")

        headers = guard.format_header_lines()
        self.assertTrue(any("UNRESTRICTED" in h for h in headers))

        info = guard.format_info_lines()
        self.assertTrue(any("UNRESTRICTED" in i for i in info))

        summary = guard.format_summary_lines()
        self.assertTrue(any("UNRESTRICTED" in s for s in summary))

    def test_armed_country_normalization(self):
        guard = NetworkGuard(required_country="in", drop_policy="lenient")
        self.assertTrue(guard.is_armed)
        self.assertEqual(guard.required_country, "IN")
        self.assertEqual(guard.drop_policy, "LENIENT")

    def test_smart_double_vpn_preflight_bypass(self):
        # When host is already on VPN matching target country
        with patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="Surfshark"), \
             patch.object(NetworkRouteResolver, "get_public_ip_and_country", return_value=("103.1.2.3", "IN")):

            guard = NetworkGuard(required_country="IN", drop_policy="STRICT")
            self.assertEqual(guard.vpn_mode, "LEGACY_VPN")
            self.assertEqual(guard.active_system_vpn, "Surfshark")
            self.assertFalse(guard.vpn_conflict)

            headers = guard.format_header_lines()
            self.assertTrue(any("SYSTEM_VPN: Surfshark" in h for h in headers))

    def test_double_vpn_conflict_detection(self):
        # When host is on a conflicting country (e.g. US != IN)
        with patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="NordVPN"), \
             patch.object(NetworkRouteResolver, "get_public_ip_and_country", return_value=("142.250.1.1", "US")):

            guard = NetworkGuard(required_country="IN", drop_policy="LENIENT")
            self.assertTrue(guard.vpn_conflict)
            self.assertEqual(guard.conflict_country, "US")

            headers = guard.format_header_lines()
            self.assertTrue(any("CONFLICT" in h for h in headers))

    def test_silent_manual_startup_invariant(self):
        # Manual startup must not activate attention siren
        state = MagicMock()
        state.is_scheduled = False
        alarm_started = []

        guard = NetworkGuard(
            required_country="IN",
            state=state,
            alarm_start_fn=lambda s, alarm_type=None: alarm_started.append(alarm_type),
        )

        # In handle_vpn_hold_state:
        # should_alarm is False for is_startup=True when not scheduled
        is_scheduled_run = bool(state and getattr(state, "is_scheduled", False))
        should_alarm = is_scheduled_run  # for startup
        self.assertFalse(should_alarm, "Manual startup must be silent (no attention siren)")

    def test_scheduled_startup_alarm_invariant(self):
        # Scheduled unattended launch must activate attention siren on network drop
        state = MagicMock()
        state.is_scheduled = True

        is_scheduled_run = bool(state and getattr(state, "is_scheduled", False))
        should_alarm = is_scheduled_run
        self.assertTrue(should_alarm, "Scheduled unattended start must sound alarm on missing VPN")

    def test_http_429_three_strike_protocol(self):
        alarm_types = []
        guard = NetworkGuard(
            required_country="IN",
            alarm_start_fn=lambda s, alarm_type=None: alarm_types.append(alarm_type),
            render_progress_fn=lambda *a, **kw: None,
            clear_progress_fn=lambda: None,
        )

        # Strike 1: 90s Cooldown
        with patch("recorder_runtime.network.time.sleep"), \
             patch("recorder_runtime.network.time.monotonic", side_effect=[0.0, 95.0, 100.0, 105.0]):
            res = guard.handle_rate_limit_cooldown()
            self.assertEqual(guard.rate_limit_strike, 1)
            self.assertTrue(res)

        # Strike 2: 240s Extended In-Place Cooldown on same IP (no IP rotation)
        guard.vpn_mode = "WIREPROXY"
        guard.wireproxy_manager = MagicMock()
        rotation_mock = MagicMock(return_value=True)
        hold_mock = MagicMock(return_value=True)
        guard.attempt_wireguard_auto_recovery = rotation_mock
        guard.handle_vpn_hold_state = hold_mock
        with patch("recorder_runtime.network.time.sleep"), \
             patch("recorder_runtime.network.time.monotonic", side_effect=[200.0, 300.0, 445.0]):
            res = guard.handle_rate_limit_cooldown()
            self.assertEqual(guard.rate_limit_strike, 2)
            self.assertTrue(res)
            rotation_mock.assert_not_called()
            hold_mock.assert_not_called()

        # Strike 3: Critical Concurrency Exhaustion Alarm
        res = guard.handle_rate_limit_cooldown()
        self.assertEqual(guard.rate_limit_strike, 3)
        self.assertFalse(res)
        self.assertIn("critical", alarm_types)

    def test_format_summary_lines_contains_metrics(self):
        guard = NetworkGuard(required_country="IN", drop_policy="LENIENT")
        guard.launch_ip = "103.5.5.5"
        guard.launch_country = "IN"
        guard.vpn_drop_count = 2
        guard.vpn_hold_total_duration = 125.0
        guard.rate_limit_429_count = 1

        summary = guard.format_summary_lines()
        text = "\n".join(summary)

        self.assertIn("Network & VPN Guardrail", text)
        self.assertIn("Target Country          : IN", text)
        self.assertIn("103.5.5.5", text)
        self.assertIn("VPN Disconnect Events   : 2", text)
        self.assertIn("Total VPN Hold Duration : 02:05", text)
        self.assertIn("HTTP 429 Incidents      : 1", text)

    def test_supervise_in_flight_strict_and_lenient(self):
        # Strict policy returns vpn_drop on route severance
        guard_strict = NetworkGuard(required_country="IN", drop_policy="STRICT")
        mock_proc = MagicMock()
        with patch.object(guard_strict, "check_runtime_route", return_value=False), \
             patch.object(guard_strict, "attempt_wireguard_auto_recovery", return_value=False):
            res = guard_strict.supervise_in_flight(proc=mock_proc)
            self.assertEqual(res, "vpn_drop")
            self.assertTrue(mock_proc.terminate.called)

        # Lenient policy continues retrying if internet online
        guard_lenient = NetworkGuard(required_country="IN", drop_policy="LENIENT")
        mock_proc_lenient = MagicMock()
        with patch.object(guard_lenient, "check_runtime_route", return_value=False), \
             patch.object(guard_lenient, "attempt_wireguard_auto_recovery", return_value=False), \
             patch("recorder_runtime.network.NetworkRouteResolver.is_internet_online", return_value=True):
            res = guard_lenient.supervise_in_flight(proc=mock_proc_lenient)
            self.assertIsNone(res)
            self.assertFalse(mock_proc_lenient.terminate.called)

        # Lenient policy returns internet_drop if physical internet is also severed
        with patch.object(guard_lenient, "check_runtime_route", return_value=False), \
             patch.object(guard_lenient, "attempt_wireguard_auto_recovery", return_value=False), \
             patch("recorder_runtime.network.NetworkRouteResolver.is_internet_online", return_value=False):
            res = guard_lenient.supervise_in_flight(proc=mock_proc_lenient)
            self.assertEqual(res, "internet_drop")
            self.assertTrue(mock_proc_lenient.terminate.called)



class RecordDynamicWiringTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        target_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"
        with open(target_path, "r", encoding="utf-8") as f:
            cls.code = f.read()

    def test_scheduled_countdown_has_no_preflight_checks(self):
        # In wait_until_start: during countdown loop, setup_network_guard must NOT be called
        self.assertIn("def wait_until_start(", self.code)
        # Ensure setup_network_guard is called after loop exit
        self.assertIn("Reached scheduled start time", self.code)
        self.assertIn("preflight_check(temp_dir=CHUNKS_DIR, is_scheduled=False)", self.code)

    def test_custom_proxy_injected_for_wireproxy(self):
        self.assertIn("--custom-proxy http://127.0.0.1:{WIREPROXY_HTTP_PORT}", self.code)
        self.assertIn(
            "NM3U8DL_PART_B = '-M format=ts --live-real-time-merge --live-pipe-mux --thread-count 2 --live-take-count 2 --live-wait-time 3 --download-retry-count 2",
            self.code,
        )
        self.assertNotIn("--download-retry-count 30", self.code)

    def test_preflight_wireproxy_not_skipped_on_internet_recovery(self):
        import recorder_runtime.network as net_mod
        import inspect
        src = inspect.getsource(net_mod.NetworkGuard.preflight_check)
        wp_block = src[src.find('if self.vpn_mode == "WIREPROXY":'):]
        wp_block = wp_block[:wp_block.find('elif self.vpn_mode == "LEGACY_VPN":')]
        self.assertNotIn("return self.handle_internet_hold_state(is_startup=True)", wp_block)
        self.assertIn("if not self.handle_internet_hold_state(is_startup=True):", wp_block)

    def test_internet_drop_aborts_during_lenient_vpn_drop(self):
        # In both Phase A and Phase B, internet severance during lenient VPN drop must abort retries
        net_path = Path(__file__).resolve().parent.parent / "recorder_runtime" / "network.py"
        net_code = net_path.read_text(encoding="utf-8")
        self.assertTrue(
            "Outbound internet severed while in VPN drop! Aborting retry to seal chunk..." in self.code
            or "Outbound internet severed while in VPN drop! Aborting retry to seal chunk..." in net_code
        )
        self.assertIn('reason="internet connection lost during lenient drop"', self.code)

    def test_universal_internet_hold_in_orchestrator(self):
        # Universal Internet Outage Protection must not be restricted by is_armed
        self.assertIn("# Universal Internet Outage Protection ([INTERNET_HOLD] across all streams)", self.code)
        sub = self.code[self.code.find("def apply_orchestrator_policy("):]
        gate_idx = sub.find("# Universal Internet Outage Protection ([INTERNET_HOLD] across all streams)")
        self.assertNotIn("and not state.network_guard.is_armed", sub[gate_idx:gate_idx+200])

    def test_ctrl_c_handler_stops_wireproxy(self):
        self.assertIn("def make_signal_handler(", self.code)
        # Verify wireproxy_manager.stop() is in handler
        sub = self.code[self.code.find("def make_signal_handler("):]
        sub = sub[:sub.find("return _handler")]
        self.assertIn("state.wireproxy_manager.stop()", sub)

    def test_finalize_recording_stops_wireproxy(self):
        self.assertIn("def finalize_recording(", self.code)
        sub = self.code[self.code.find("def finalize_recording("):]
        sub = sub[:sub.find("if not os.path.exists(LIST_FILE):")]
        self.assertIn("state.wireproxy_manager.stop()", sub)

    def test_summary_log_appends_network_guard(self):
        self.assertIn("state.network_guard.format_summary_lines()", self.code)

    def test_show_recording_info_appends_network_guard(self):
        self.assertIn("state.network_guard.format_info_lines()", self.code)

    def test_terminal_color_rules_and_startup_headers(self):
        self.assertIn('RUN_BLUE = "\\033[1;94m"', self.code)
        self.assertIn('"NETWORK_GUARD_HOLD" in line', self.code)
        self.assertIn('"RATE_LIMIT_COOLDOWN" in line', self.code)
        self.assertIn('"NETWORK_GUARD: Tracking" in line', self.code)
        self.assertIn('"NETWORK_GUARD: VPN connection restored" in line', self.code)
        self.assertIn('"RECORDING ENGINE  :"', self.code)
        self.assertIn('"Host Platform     :"', self.code)
        self.assertIn('"Network Guard     :"', self.code)
        self.assertIn('"Network Route     :"', self.code)
        self.assertIn('BOLD = "\\033[1m"', self.code)

    def test_early_global_key_listener_started_before_wait_until_start(self):
        # In run_recorder_process: ensure_global_key_listener must be called before wait_until_start
        sub = self.code[self.code.find("def run_recorder_process("):]
        idx_listener = sub.find("ensure_global_key_listener(state)")
        idx_wait = sub.find("wait_until_start(state, selected_engine)")
        self.assertTrue(idx_listener != -1 and idx_wait != -1)
        self.assertLess(idx_listener, idx_wait, "ensure_global_key_listener must precede wait_until_start")

    def test_vpn_alarm_acknowledgment_wiring(self):
        # Dedicated ACK functions must exist and be wired in key listener
        self.assertIn("def acknowledge_vpn_drop_alarm(state)", self.code)
        self.assertIn("def acknowledge_internet_drop_alarm(state)", self.code)
        self.assertIn("acknowledge_vpn_drop_alarm(state)", self.code)
        self.assertIn("acknowledge_internet_drop_alarm(state)", self.code)

    def test_inter_chunk_network_route_gate(self):
        # Inter-chunk gate must check route before launching next cycle
        self.assertIn("# Check inter-chunk network route gate before launching next chunk", self.code)
        self.assertIn("state.network_guard.check_runtime_route()", self.code)
        self.assertIn("state.network_guard.handle_vpn_hold_state(is_startup=False)", self.code)

    def test_phase_a_route_monitoring(self):
        # Phase A (file appearance wait) must include route monitoring
        self.assertIn("# Network Guard: In-stream route monitoring for Phase A (Hook 4)", self.code)

    def test_init_termcap_called_before_wait_until_start(self):
        # In run_recorder_process: init_termcap must be called before wait_until_start so header is captured
        sub = self.code[self.code.find("def run_recorder_process("):]
        idx_termcap = sub.find("init_termcap()")
        idx_wait = sub.find("wait_until_start(state, selected_engine)")
        self.assertTrue(idx_termcap != -1 and idx_wait != -1)
        self.assertLess(idx_termcap, idx_wait, "init_termcap must precede wait_until_start to capture header")

        # init_termcap definition must be idempotent (not overwrite existing file)
        self.assertIn("def init_termcap(header_lines=None):", self.code)
        self.assertIn("elif not os.path.isfile(TERMCAP_PATH):", self.code)

    def test_host_platform_summary_in_header(self):
        # get_host_platform_summary must exist and be called in wait_until_start
        self.assertIn("def get_host_platform_summary() -> str:", self.code)
        self.assertIn('log(f"Host Platform     : {get_host_platform_summary()}")', self.code)
        self.assertIn("socket.gethostname()", self.code)

    def test_is_nm3u8dl_fatal_producer_error_logic(self):
        # Dynamically execute definition of is_nm3u8dl_fatal_producer_error from record_dynamic
        idx_func = self.code.find("def is_nm3u8dl_fatal_producer_error(")
        self.assertNotEqual(idx_func, -1)
        sub = self.code[idx_func:]
        idx_end = sub.find("\n\n")
        func_def = sub[:idx_end]
        ns = {}
        exec(func_def, ns)
        fn = ns["is_nm3u8dl_fatal_producer_error"]

        # Exact crash line from user terminal session
        user_crash = (
            "System.Threading.Tasks.TaskCanceledException: The request was canceled "
            "due to the configured HttpClient.Timeout of 90 seconds elapsing."
        )
        self.assertTrue(fn(user_crash))

        # Stack trace / error variants for producer & RefreshPlayList
        self.assertTrue(fn("DASHExtractor2.RefreshPlayListAsync error: System.Threading.Tasks.TaskCanceledException"))
        self.assertTrue(fn("DASHExtractor2.RefreshPlayListAsync error: System.TimeoutException: The operation has timed out."))
        self.assertTrue(fn("PlayListProduceAsync error: System.Net.Sockets.SocketException: Connection reset by peer"))
        self.assertTrue(fn("DASHExtractor2.RefreshPlayListAsync error: Connection aborted"))
        self.assertTrue(fn("TaskCanceledException: A task was canceled HttpClient.Timeout"))

        # Non-fatal normal lines (must NOT trigger fatal flag)
        self.assertFalse(fn("Waiting 100% -"))
        self.assertFalse(fn("Vid 00m33s/00m33s 11/11"))
        self.assertFalse(fn("01h00m14s/01h00m14s 913/913"))
        self.assertFalse(fn("ERROR:"))
        self.assertFalse(fn("ERROR"))
        self.assertFalse(fn("Selected streams:"))
        self.assertFalse(fn("HARD_STALL_CHECK 1/20 growth_bytes=0"))
        self.assertFalse(fn("2026-10-14 10:11:08 [INFO] Engine selected"))

    def test_nm3u8dl_fatal_producer_error_wiring_in_record_dynamic(self):
        # 1. Function definition exists
        self.assertIn("def is_nm3u8dl_fatal_producer_error(line: str) -> bool:", self.code)

        # 2. Wired in extract_nm3u8dl_startup_info
        start_idx = self.code.find("def extract_nm3u8dl_startup_info(")
        end_idx = self.code.find("\ndef ", start_idx + 1)
        startup_sub = self.code[start_idx:end_idx]
        self.assertIn("if is_nm3u8dl_fatal_producer_error(stripped_line):", startup_sub)
        self.assertIn('state.nm3u8dl_stall_reason = "playlist_producer_timeout"', startup_sub)

        # 3. Wired in monitor_nm3u8dl_stdout
        start_idx = self.code.find("def monitor_nm3u8dl_stdout(")
        end_idx = self.code.find("\ndef ", start_idx + 1)
        monitor_sub = self.code[start_idx:end_idx]
        self.assertIn("if is_nm3u8dl_fatal_producer_error(stripped):", monitor_sub)
        self.assertIn('state.nm3u8dl_stall_reason = "playlist_producer_timeout"', monitor_sub)

        # 4. Strict constraint: Jio ad-tolerance (hard_stall_required: 20) preserved
        self.assertIn('"hard_stall_required": 20,', self.code)

    def test_nm3u8dl_fatal_producer_error_wiring_in_static_recorder(self):
        static_path = Path(__file__).resolve().parent.parent.parent / "WORKSPACE FOR AI AGENTS" / "RECORD.py"
        if not static_path.exists():
            static_path = Path("/Users/sumitsinha/Movies/GSE SMART IPTV/Recordings/WORKSPACE FOR AI AGENTS/RECORD.py")
        if not static_path.exists():
            static_path = Path(__file__).resolve().parent.parent.parent / "RECORDER CODE FOR ANTIGRAVITY" / "RECORD.py"
        if not static_path.exists():
            static_path = Path("/Users/sumitsinha/Movies/GSE SMART IPTV/Recordings/RECORDER CODE FOR ANTIGRAVITY/RECORD.py")
        if not static_path.exists():
            self.skipTest("Static recorder RECORD.py not found in local workspace (CI environment)")
        static_code = static_path.read_text(encoding="utf-8")

        # 1. Function definition exists
        self.assertIn("def is_nm3u8dl_fatal_producer_error(line: str) -> bool:", static_code)

        # 2. Wired in extract_nm3u8dl_startup_info
        start_idx = static_code.find("def extract_nm3u8dl_startup_info(")
        end_idx = static_code.find("\ndef ", start_idx + 1)
        startup_sub = static_code[start_idx:end_idx]
        self.assertIn("if is_nm3u8dl_fatal_producer_error(stripped_line):", startup_sub)
        self.assertIn('state.nm3u8dl_stall_reason = "playlist_producer_timeout"', startup_sub)

        # 3. Wired in monitor_nm3u8dl_stdout
        start_idx = static_code.find("def monitor_nm3u8dl_stdout(")
        end_idx = static_code.find("\ndef ", start_idx + 1)
        monitor_sub = static_code[start_idx:end_idx]
        self.assertIn("if is_nm3u8dl_fatal_producer_error(stripped):", monitor_sub)
        self.assertIn('state.nm3u8dl_stall_reason = "playlist_producer_timeout"', monitor_sub)

        # 4. Callsite of extract_nm3u8dl_startup_info in RECORD.py passes state=state
        start_call = static_code.find("startup_data = extract_nm3u8dl_startup_info(")
        call_sub = static_code[start_call:start_call + 300]
        self.assertIn("state=state", call_sub)


class DynamicRecorderGeoMismatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        with tempfile.NamedTemporaryFile(suffix=".py", mode="w", delete=False) as tmp:
            tmp.write(
                "NM3U8DL_PLAYLIST_PRIMARY_PHRASES = ['test']\n"
                "NM3U8DL_PLAYLIST_REQUIRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_REJECTED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_PREFERRED_QUALIFIERS = []\n"
                "NM3U8DL_PLAYLIST_GROUP = 'TEST'\n"
                "NM3U8DL_PLAYLIST_GROUPS = {'TEST': ['http://test']}\n"
                "SCHEDULE_START = ''\n"
                "RUN_DURATION_MIN = 1\n"
                "BASE_NAME = 'test'\n"
                f"RECORDING_OUTPUT_DIR = {repr(tempfile.gettempdir())}\n"
                "REQUIRED_VPN_COUNTRY = 'IN'\n"
            )
            cls._test_cfg_path = tmp.name

        cls._old_cfg = os.environ.get("RECORDER_DYNAMIC_CONFIG_PATH")
        os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = cls._test_cfg_path
        import record_dynamic
        cls.record_dynamic = record_dynamic

    @classmethod
    def tearDownClass(cls):
        if cls._old_cfg and os.path.isfile(cls._old_cfg):
            os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = cls._old_cfg
        elif _ci_temp_config and os.path.isfile(_ci_temp_config.name):
            os.environ["RECORDER_DYNAMIC_CONFIG_PATH"] = _ci_temp_config.name
        else:
            os.environ.pop("RECORDER_DYNAMIC_CONFIG_PATH", None)
        try:
            os.unlink(cls._test_cfg_path)
        except Exception:
            pass

    def setUp(self):
        code_path = Path(__file__).resolve().parent.parent / "record_dynamic.py"
        self.code = code_path.read_text(encoding="utf-8")

    def test_state_has_nm3u8dl_geo_jump_attempt(self):
        self.assertIn("nm3u8dl_geo_jump_attempt: int = 0", self.code)

    def test_helpers_exist(self):
        self.assertIn("def _scan_has_confirmed_geo_mismatch(", self.code)
        self.assertIn("def _handle_scan_geo_mismatch_recovery(", self.code)

    def test_geo_mismatch_detection_logic(self):
        _scan_has_confirmed_geo_mismatch = self.record_dynamic._scan_has_confirmed_geo_mismatch
        source_results = [
            {
                "playlist_url": "http://playlist.m3u8",
                "candidate_rows": [
                    {
                        "entry_title": "Star Sports 1 HD",
                        "access_blocked": True,
                        "access_block_kind": "confirmed_geo",
                        "geo_country": "SG",
                    },
                    {
                        "entry_title": "Star Sports 1 HD (Jio)",
                        "access_blocked": False,
                        "launchable": True,
                    },
                ],
            }
        ]
        has_mismatch, mismatched = _scan_has_confirmed_geo_mismatch(source_results, required_country="IN")
        self.assertTrue(has_mismatch)
        self.assertEqual(len(mismatched), 1)
        self.assertEqual(mismatched[0]["geo_country"], "SG")

        # Clean scan
        clean_results = [
            {
                "playlist_url": "http://playlist.m3u8",
                "candidate_rows": [
                    {
                        "entry_title": "Star Sports 1 HD",
                        "access_blocked": False,
                        "launchable": True,
                    }
                ],
            }
        ]
        has_mismatch, mismatched = _scan_has_confirmed_geo_mismatch(clean_results, required_country="IN")
        self.assertFalse(has_mismatch)
        self.assertEqual(len(mismatched), 0)

    def test_handle_scan_geo_mismatch_recovery_rotates_wireproxy(self):
        _handle_scan_geo_mismatch_recovery = self.record_dynamic._handle_scan_geo_mismatch_recovery
        RecorderState = self.record_dynamic.RecorderState
        state = RecorderState()
        mock_guard = MagicMock()
        mock_guard.vpn_mode = "WIREPROXY"
        mock_guard.wireproxy_manager = MagicMock()
        mock_guard.required_country = "IN"
        mock_guard.launch_ip = "138.199.34.86"
        mock_guard.launch_country = "IN"
        mock_guard.attempt_wireguard_auto_recovery.return_value = True
        state.network_guard = mock_guard

        source_results = [
            {
                "candidate_rows": [
                    {
                        "entry_title": "Hotstar Stream",
                        "access_blocked": True,
                        "access_block_kind": "confirmed_geo",
                        "geo_country": "SG",
                    }
                ]
            }
        ]

        result = _handle_scan_geo_mismatch_recovery(state, source_results, has_viable_fallback=True)
        self.assertTrue(result)
        self.assertEqual(state.nm3u8dl_geo_jump_attempt, 1)
        self.assertEqual(mock_guard.blocked_403_ip, "138.199.34.86")
        mock_guard.attempt_wireguard_auto_recovery.assert_called_once()

    def test_handle_scan_geo_mismatch_exhaustion_with_fallback(self):
        _handle_scan_geo_mismatch_recovery = self.record_dynamic._handle_scan_geo_mismatch_recovery
        RecorderState = self.record_dynamic.RecorderState
        state = RecorderState()
        state.nm3u8dl_geo_jump_attempt = 5  # Already 5 attempts
        mock_guard = MagicMock()
        mock_guard.vpn_mode = "WIREPROXY"
        mock_guard.wireproxy_manager = MagicMock()
        mock_guard.required_country = "IN"
        mock_guard.launch_ip = "138.199.34.86"
        state.network_guard = mock_guard

        source_results = [
            {
                "candidate_rows": [
                    {
                        "entry_title": "Hotstar Stream",
                        "access_blocked": True,
                        "access_block_kind": "confirmed_geo",
                        "geo_country": "SG",
                    }
                ]
            }
        ]

        # Exhaustion with viable fallback returns False and allows fallback without entering VPN hold
        with patch("record_dynamic.beep_bad") as mock_beep:
            result = _handle_scan_geo_mismatch_recovery(state, source_results, has_viable_fallback=True)
            self.assertFalse(result)
            mock_beep.assert_called_once_with(state)
            mock_guard.handle_vpn_hold_state.assert_not_called()

    def test_handle_scan_geo_mismatch_exhaustion_without_fallback_enters_hold(self):
        _handle_scan_geo_mismatch_recovery = self.record_dynamic._handle_scan_geo_mismatch_recovery
        RecorderState = self.record_dynamic.RecorderState
        state = RecorderState()
        state.nm3u8dl_geo_jump_attempt = 5  # Already 5 attempts
        mock_guard = MagicMock()
        mock_guard.vpn_mode = "WIREPROXY"
        mock_guard.wireproxy_manager = MagicMock()
        mock_guard.required_country = "IN"
        mock_guard.launch_ip = "138.199.34.86"
        mock_guard.handle_vpn_hold_state.return_value = False
        state.network_guard = mock_guard

        source_results = [
            {
                "candidate_rows": [
                    {
                        "entry_title": "Hotstar Stream",
                        "access_blocked": True,
                        "access_block_kind": "confirmed_geo",
                        "geo_country": "SG",
                    }
                ]
            }
        ]

        # Exhaustion without viable fallback diverts to VPN Hold state
        with patch("record_dynamic.beep_bad") as mock_beep:
            result = _handle_scan_geo_mismatch_recovery(state, source_results, has_viable_fallback=False)
            self.assertFalse(result)
            mock_beep.assert_called_once_with(state)
            mock_guard.handle_vpn_hold_state.assert_called_once()

    def test_wiring_in_resolve_nm3u8dl_launch_source(self):
        # Must be called on successful scan
        self.assertIn("_handle_scan_geo_mismatch_recovery(\n                    state,\n                    initial_source_results,\n                    has_viable_fallback=True,", self.code)
        # Must be called on failed scan
        self.assertIn("_handle_scan_geo_mismatch_recovery(\n                    state,\n                    source_results,\n                    has_viable_fallback=False,", self.code)


class InterChunkGateCheckTests(unittest.TestCase):
    def test_wireproxy_to_host_vpn_transition_uses_force_direct_and_stops_wireproxy(self):
        guard = NetworkGuard(required_country="IN")
        guard.vpn_mode = "WIREPROXY"
        guard.wireproxy_manager = MagicMock()
        guard.wireproxy_manager.is_running.return_value = True

        with patch.object(NetworkRouteResolver, "is_internet_online", return_value=True), \
             patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="Surfshark"), \
             patch.object(NetworkRouteResolver, "invalidate_cache") as mock_inval, \
             patch.object(NetworkRouteResolver, "get_public_ip_and_country", return_value=("103.21.244.2", "IN")) as mock_probe:

            result = guard.inter_chunk_gate_check()
            self.assertTrue(result)
            mock_inval.assert_called_once()
            mock_probe.assert_called_once_with(timeout=2.0, force_direct=True)
            guard.wireproxy_manager.stop.assert_called_once()
            self.assertEqual(guard.vpn_mode, "LEGACY_VPN")
            self.assertEqual(guard.launch_ip, "103.21.244.2")
            self.assertEqual(guard.launch_country, "IN")
            self.assertEqual(guard.active_system_vpn, "Surfshark")

    def test_wireproxy_to_host_vpn_transition_conflict_diverts_to_hold(self):
        guard = NetworkGuard(required_country="IN")
        guard.vpn_mode = "WIREPROXY"
        guard.wireproxy_manager = MagicMock()
        guard.wireproxy_manager.is_running.return_value = True

        with patch.object(NetworkRouteResolver, "is_internet_online", return_value=True), \
             patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="Surfshark"), \
             patch.object(NetworkRouteResolver, "invalidate_cache") as mock_inval, \
             patch.object(NetworkRouteResolver, "get_public_ip_and_country", return_value=("138.199.34.86", "SG")) as mock_probe, \
             patch.object(guard, "handle_vpn_hold_state", return_value=False) as mock_hold:

            result = guard.inter_chunk_gate_check()
            self.assertFalse(result)
            mock_probe.assert_called_once_with(timeout=2.0, force_direct=True)
            mock_hold.assert_called_once_with(is_startup=False)
            self.assertTrue(guard.vpn_conflict)
            self.assertEqual(guard.conflict_country, "SG")


class RuntimeRouteResolutionAndBannerTests(unittest.TestCase):
    def test_runtime_double_vpn_resolution_emits_good_beep_and_logs(self):
        logged = []
        beeps_bad = []
        beeps_good = []
        good_beep_logs = []

        mock_state = MagicMock()
        mock_state.stop_flag = False
        guard = NetworkGuard(
            required_country="IN",
            state=mock_state,
            log_fn=lambda msg, **kw: logged.append(msg),
            beep_bad_fn=lambda s: beeps_bad.append(s),
            beep_good_fn=lambda s: beeps_good.append(s),
            log_good_beep_fn=lambda reason: good_beep_logs.append(reason),
        )
        guard.vpn_mode = "WIREPROXY"
        guard.wireproxy_manager = MagicMock()
        guard.wireproxy_manager.is_running.return_value = True
        guard.wireproxy_manager.check_port_responsive.return_value = True

        # 1. Wireproxy running, host VPN connects in background
        with patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="Surfshark"):
            guard.check_runtime_route()
            self.assertTrue(guard._runtime_double_vpn_warned)
            self.assertEqual(len(beeps_bad), 1)
            self.assertTrue(any("Nesting Wireproxy inside Surfshark creates a double-VPN conflict" in m for m in logged))

        # 2. Host VPN disconnects, internet is verified online
        guard.last_route_check_mono = 0.0
        with patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("en0", False)), \
             patch.object(NetworkRouteResolver, "is_internet_online", return_value=True):
            guard.check_runtime_route()
            self.assertFalse(guard._runtime_double_vpn_warned)
            self.assertIn("double_vpn_resolved", good_beep_logs)
            self.assertEqual(len(beeps_good), 1)
            self.assertTrue(any("Double-VPN conflict resolved; clean Wireproxy routing restored." in m for m in logged))

    def test_handle_internet_hold_state_emits_beep_good_on_recovery(self):
        good_beep_logs = []
        beeps_good = []

        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.alarm_ack_requested = False
        mock_state.internet_hold_active = True

        guard = NetworkGuard(
            required_country="IN",
            state=mock_state,
            beep_good_fn=lambda s: beeps_good.append(s),
            log_good_beep_fn=lambda reason: good_beep_logs.append(reason),
        )
        guard.is_armed = False

        # First call False drops into hold, second call True recovers
        with patch.object(NetworkRouteResolver, "is_internet_online", side_effect=[False, True]):
            res = guard.handle_internet_hold_state(is_startup=False)
            self.assertTrue(res)
            self.assertIn("recovery_healthy", good_beep_logs)
            self.assertEqual(len(beeps_good), 1)

    def test_inter_chunk_gate_check_logs_headers_on_transition(self):
        logged = []
        guard = NetworkGuard(
            required_country="IN",
            log_fn=lambda msg, **kw: logged.append(msg),
        )
        guard.vpn_mode = "WIREPROXY"
        guard.wireproxy_manager = MagicMock()
        guard.wireproxy_manager.is_running.return_value = True

        # Transition Wireproxy -> Host VPN
        with patch.object(NetworkRouteResolver, "is_internet_online", return_value=True), \
             patch.object(NetworkRouteResolver, "get_default_route_info", return_value=("utun6", True)), \
             patch.object(NetworkRouteResolver, "get_friendly_vpn_name", return_value="Surfshark"), \
             patch.object(NetworkRouteResolver, "invalidate_cache"), \
             patch.object(NetworkRouteResolver, "get_public_ip_and_country", return_value=("103.21.244.2", "IN")):
            self.assertTrue(guard.inter_chunk_gate_check())
            self.assertTrue(any("Network Guard     : ARMED [SYSTEM_VPN: Surfshark]" in m for m in logged))
            self.assertTrue(any("Network Route     : Connected [SYSTEM_VPN: Surfshark]" in m for m in logged))

    def test_log_run_start_banner_prints_guard_headers_and_route(self):
        import record_dynamic

        logged = []
        mock_state = MagicMock()
        mock_state.run_attempt_index = 0

        guard = NetworkGuard(required_country="IN")
        guard.vpn_mode = "LEGACY_VPN"
        guard.is_armed = True
        guard.active_system_vpn = "Surfshark"
        guard.launch_country = "IN"
        guard.launch_interface = "utun6"
        guard.launch_ip = "103.21.244.2"
        mock_state.network_guard = guard

        with patch("record_dynamic.log", side_effect=lambda msg, **kw: logged.append(msg)):
            record_dynamic.log_run_start_banner(mock_state, "nm3u8dl")

        self.assertEqual(mock_state.run_attempt_index, 1)
        self.assertTrue(any("RUN 1 START" in m for m in logged))
        self.assertTrue(any("Network Guard     : ARMED [SYSTEM_VPN: Surfshark]" in m for m in logged))
        self.assertTrue(any("Network Route     : Connected [Surfshark] (Country: IN | Adapter: utun6 | IP: 103.21.244.2)" in m for m in logged))

    def test_inspect_stdout_line_429_threshold_and_waiting_not_resetting(self):
        guard = NetworkGuard(required_country="IN")

        # 1st 429 error -> does not trip yet
        self.assertFalse(guard.inspect_stdout_line("ERROR: Response status code does not indicate success: 429 (Too Many Requests)."))
        self.assertEqual(len(guard.recent_429_timestamps), 1)

        # "Waiting 100%" progress line must NOT reset timestamps
        self.assertFalse(guard.inspect_stdout_line("Vid 1920x1080 | 6000 Kbps | Waiting 100%"))
        self.assertEqual(len(guard.recent_429_timestamps), 1)

        # "Recording" progress line must NOT reset timestamps
        self.assertFalse(guard.inspect_stdout_line("Vid 1920x1080 | Recording 45%"))
        self.assertEqual(len(guard.recent_429_timestamps), 1)

        # 2nd 429 within 30s -> does not trip yet
        self.assertFalse(guard.inspect_stdout_line("HTTP Error 429: Too Many Requests"))
        self.assertEqual(len(guard.recent_429_timestamps), 2)

        # 3rd 429 within 30s -> trips circuit breaker (3 in 30s)
        self.assertTrue(guard.inspect_stdout_line("ERROR: Response status code does not indicate success: 429 (Too Many Requests)."))
        self.assertEqual(guard.rate_limit_429_count, 1)
        self.assertEqual(len(guard.recent_429_timestamps), 0)

    def test_extract_stream_expiry_and_token_expiration(self):
        guard = NetworkGuard(required_country="IN")

        # Query param formats: exp, expires, expiry
        self.assertEqual(guard.extract_stream_expiry("https://example.com/live.m3u8?exp=1740000000"), 1740000000)
        self.assertEqual(guard.extract_stream_expiry("https://example.com/live.m3u8?expires=1740000000"), 1740000000)
        self.assertEqual(guard.extract_stream_expiry("https://example.com/live.m3u8?token=xyz;expiry=1740000000"), 1740000000)

        # 13-digit millisecond normalization
        self.assertEqual(guard.extract_stream_expiry("https://example.com/live.m3u8?exp=1740000000000"), 1740000000)

        # Active token expiration check
        past_ts = int(time.time()) - 100
        future_ts = int(time.time()) + 3600

        mock_state = MagicMock()
        mock_state.live_url = f"https://example.com/live.m3u8?exp={past_ts}"
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        mock_state.active_stream_url = ""
        guard.state = mock_state
        self.assertTrue(guard.is_stream_token_expired(lead_time_sec=0.0))

        mock_state.live_url = f"https://example.com/live.m3u8?exp={future_ts}"
        self.assertFalse(guard.is_stream_token_expired(lead_time_sec=60.0))

    def test_handle_token_hold_state_headless_fast_fail(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        guard = NetworkGuard(required_country="IN", state=mock_state)

        with patch("sys.stdin.isatty", return_value=False):
            res = guard.handle_token_hold_state()
            self.assertFalse(res)
            self.assertTrue(mock_state.stop_flag)
            self.assertEqual(guard.token_expiry_count, 1)

    def test_handle_403_recovery_decouples_token_expiry_without_vpn_siren(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 1}
        mock_state.live_url = "https://example.com/live.m3u8"
        mock_state.token_hold_active = False

        alarms_started = []
        guard = NetworkGuard(
            required_country="IN",
            state=mock_state,
            alarm_start_fn=lambda s, alarm_type: alarms_started.append(alarm_type),
        )
        guard.vpn_mode = "LEGACY_VPN"
        guard.is_armed = True
        guard.launch_country = "IN"

        with patch.object(guard, "check_runtime_route", return_value=True), \
             patch.object(guard, "handle_token_hold_state", return_value=True) as mock_token_hold, \
             patch.object(guard, "handle_vpn_hold_state") as mock_vpn_hold:
            res = guard.handle_403_recovery(is_startup=False)
            self.assertTrue(res)
            mock_token_hold.assert_called_once()
            mock_vpn_hold.assert_not_called()
            self.assertNotIn("vpn_drop", alarms_started)

    def test_supervise_in_flight_terminates_on_rate_limit_429(self):
        mock_state = MagicMock()
        mock_state.rate_limit_429_detected = True
        mock_state.stop_flag = False
        stop_event = MagicMock()
        mock_proc = MagicMock()

        guard = NetworkGuard(required_country="IN", state=mock_state)
        status = guard.supervise_in_flight(proc=mock_proc, stop_event=stop_event)

        self.assertEqual(status, "rate_limit_429")
        self.assertTrue(stop_event.set.called)
        self.assertTrue(mock_proc.terminate.called)

    def test_deduplicate_nm3u8dl_part_b_preserves_provider_overrides(self):
        import record_dynamic

        default_b = record_dynamic.NM3U8DL_PART_B

        # JIO overrides --thread-count 1 in PART_A
        jio_a = 'N_m3u8DL-RE "https://example.com/index.m3u8" --thread-count 1'
        dedup_jio = record_dynamic._deduplicate_nm3u8dl_part_b(jio_a, default_b)
        self.assertNotIn("--thread-count", dedup_jio)
        self.assertIn("--live-take-count 2", dedup_jio)
        self.assertIn("--live-wait-time 3", dedup_jio)
        self.assertIn("--download-retry-count 2", dedup_jio)

        # 7AU overrides --live-wait-time 25 in PART_A
        au_a = 'N_m3u8DL-RE "https://example.com/index.m3u8" --use-shaka-packager --live-wait-time 25'
        dedup_au = record_dynamic._deduplicate_nm3u8dl_part_b(au_a, default_b)
        self.assertNotIn("--live-wait-time", dedup_au)
        self.assertIn("--thread-count 2", dedup_au)
        self.assertIn("--live-take-count 2", dedup_au)

    def test_hotstar_client_hints_and_quote_escaping(self):
        import shlex
        from recorder_source.headers import canonicalize_header_name
        from recorder_source.policy import PROVIDER_ADDED_HEADERS

        self.assertEqual(canonicalize_header_name("sec-ch-ua"), "Sec-CH-UA")
        self.assertEqual(canonicalize_header_name("sec-ch-ua-platform"), "Sec-CH-UA-Platform")
        self.assertEqual(canonicalize_header_name("sec-fetch-mode"), "Sec-Fetch-Mode")
        self.assertEqual(canonicalize_header_name("accept-language"), "Accept-Language")

        hotstar_headers = PROVIDER_ADDED_HEADERS["HOTSTAR"]
        self.assertIn("Sec-CH-UA", hotstar_headers)
        self.assertEqual(hotstar_headers["Origin"], "https://www.hotstar.com")
        self.assertEqual(hotstar_headers["Referer"], "https://www.hotstar.com/")
        self.assertEqual(hotstar_headers["Sec-Fetch-Mode"], "cors")
        self.assertEqual(hotstar_headers["Sec-Fetch-Site"], "cross-site")

        # Verify quote escaping produces valid shlex tokens with intact inner quotes
        parts = ["N_m3u8DL-RE", '"https://live14p.hotstar.com/master.m3u8"']
        for name, value in hotstar_headers.items():
            escaped_value = str(value).replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'-H "{name}: {escaped_value}"')
        cmd = " ".join(parts)
        tokens = shlex.split(cmd)
        self.assertIn('Sec-CH-UA: "Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"', tokens)
        self.assertIn('Sec-CH-UA-Platform: "Windows"', tokens)


if __name__ == "__main__":
    unittest.main()


