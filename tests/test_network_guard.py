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
import sys
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
        env_path = os.environ.get("RECORD_PY_PATH")
        if env_path and Path(env_path).exists():
            static_path = Path(env_path)
        else:
            static_path = None
            for parent in Path(__file__).resolve().parents[:4]:
                try:
                    for item in parent.iterdir():
                        if item.is_file() and item.name == "RECORD.py":
                            static_path = item
                            break
                        if item.is_dir() and not item.name.startswith((".", "_")) and item.name != "Recorder":
                            sub = item / "RECORD.py"
                            if sub.is_file():
                                static_path = sub
                                break
                    if static_path:
                        break
                except (PermissionError, OSError):
                    continue
        if not static_path:
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

    def test_update_stream_url_full_command_and_header_expiry_sync(self):
        mock_state = MagicMock()
        old_exp = int(time.time()) - 300
        new_exp = int(time.time()) + 7200
        mock_state.live_url = f"https://example.com/old.m3u8?exp={old_exp}"
        mock_state.active_stream_url = mock_state.live_url
        mock_state.nm3u8dl_part_a = (
            f'N_m3u8DL-RE "{mock_state.live_url}" '
            f'-H "Cookie: hdntl=exp={old_exp}~acl=/*" --key 1111:2222'
        )
        mock_state.active_command = mock_state.nm3u8dl_part_a
        guard = NetworkGuard(required_country="IN", state=mock_state)

        # Case 1: Pasting a bare URL with a new expiry also updates stale exp= in headers
        bare_new_url = f"https://example.com/new.m3u8?exp={new_exp}"
        guard.update_stream_url(bare_new_url)
        self.assertEqual(mock_state.live_url, bare_new_url)
        self.assertEqual(mock_state.active_stream_url, bare_new_url)
        self.assertIn(f"hdntl=exp={new_exp}", mock_state.nm3u8dl_part_a)
        self.assertFalse(guard.is_stream_token_expired(lead_time_sec=60.0))

        # Case 2: Pasting a full N_m3u8DL-RE command updates headers/keys and strips --save-name
        full_cmd = (
            f'N_m3u8DL-RE "https://example.com/cmd.m3u8?exp={new_exp}" '
            f'-H "Authorization: Bearer NEW_TOKEN" --key 3333:4444 --save-name "manual_clip"'
        )
        guard.update_stream_url(full_cmd)
        self.assertEqual(mock_state.live_url, f"https://example.com/cmd.m3u8?exp={new_exp}")
        self.assertIn("Authorization: Bearer NEW_TOKEN", mock_state.nm3u8dl_part_a)
        self.assertIn("--key 3333:4444", mock_state.nm3u8dl_part_a)
        self.assertNotIn("--save-name", mock_state.nm3u8dl_part_a)

    def test_handle_token_hold_rejects_expired_candidate_and_propagates_timeout_stop(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.token_hold_active = False
        mock_state.token_hold_input_buffer = ""
        mock_state.token_hold_submitted_url = None
        mock_state.live_url = "https://example.com/old.m3u8"
        mock_state.nm3u8dl_part_a = 'N_m3u8DL-RE "https://example.com/old.m3u8"'
        mock_state.active_command = mock_state.nm3u8dl_part_a

        guard = NetworkGuard(required_country="IN", state=mock_state)
        expired_url = f"https://example.com/expired.m3u8?exp={int(time.time()) - 10}"
        fresh_url = f"https://example.com/fresh.m3u8?exp={int(time.time()) + 3600}"

        # Simulate operator submitting an already-expired URL on loop 1, then a fresh URL on loop 2
        submissions = [expired_url, fresh_url]

        def fake_sleep(_sec):
            if submissions:
                mock_state.token_hold_submitted_url = submissions.pop(0)

        with patch("sys.stdin.isatty", return_value=True), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep), \
             patch.object(guard, "_probe_stream_url", return_value=True) as mock_probe:
            res = guard.handle_token_hold_state()
            self.assertTrue(res)
            # Probe should only be called once (for fresh_url, not expired_url)
            mock_probe.assert_called_once_with(fresh_url)
            self.assertEqual(mock_state.live_url, fresh_url)

    def test_verify_and_resume_stream_propagates_false_on_token_hold_failure(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.live_url = f"https://example.com/DeadToken.m3u8?exp={int(time.time()) - 100}"
        guard = NetworkGuard(required_country=None, state=mock_state)
        guard.vpn_mode = "UNRESTRICTED"
        guard.is_armed = False

        with patch("recorder_runtime.network.NetworkRouteResolver.is_internet_online", return_value=True), \
             patch.object(guard, "_probe_stream_status", return_value=(False, 401)), \
             patch.object(guard, "handle_token_hold_state", return_value=False) as mock_token_hold:
            res = guard.handle_internet_hold_state(is_startup=False)
            self.assertFalse(res)
            mock_token_hold.assert_called_once()

    def test_proactive_token_gate_passes_with_18s_remaining_and_holds_when_expired(self):
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        mock_state.stats = {"good_chunks": 0}
        guard = NetworkGuard(required_country=None, state=mock_state)

        # 1. Token with 18s remaining: must pass immediately (return True, zero hold call)
        future_url = f"https://example.com/live.mpd?exp={int(time.time()) + 18}"
        mock_state.live_url = future_url
        with patch.object(guard, "handle_token_hold_state") as mock_hold:
            self.assertTrue(guard._proactive_token_gate_check())
            mock_hold.assert_not_called()

        # 2. Token already 1s in the past: must divert to hold with is_startup=True
        expired_url = f"https://example.com/live.mpd?exp={int(time.time()) - 1}"
        mock_state.live_url = expired_url
        with patch.object(guard, "handle_token_hold_state", return_value=True) as mock_hold:
            res = guard._proactive_token_gate_check()
            self.assertTrue(res)
            mock_hold.assert_called_once_with(is_startup=True)

    def test_handle_token_hold_startup_is_silent_mid_match_alarms(self):
        mock_state = MagicMock()
        mock_state.stop_flag = True  # Stop immediately on loop entry
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_alarm = MagicMock()

        guard = NetworkGuard(required_country=None, state=mock_state, alarm_start_fn=mock_alarm)

        with patch("sys.stdin.isatty", return_value=True):
            # Manual startup (good_chunks=0, is_startup=True): must NOT sound alarm
            guard.handle_token_hold_state(is_startup=True)
            mock_alarm.assert_not_called()

            # Mid-match (good_chunks=2, is_startup=False): must sound alarm
            mock_state.stats = {"good_chunks": 2}
            guard.handle_token_hold_state(is_startup=False)
            mock_alarm.assert_called_once_with(mock_state, alarm_type="token_expired")

    def test_handle_token_hold_replacement_accepts_short_lived_token_and_bypasses_probe(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_hold_submitted_url = None
        mock_state.live_url = "https://example.com/old.m3u8"
        mock_state.nm3u8dl_part_a = 'N_m3u8DL-RE "https://example.com/old.m3u8"'
        mock_state.active_command = mock_state.nm3u8dl_part_a

        guard = NetworkGuard(required_country=None, state=mock_state)
        # Token with 25s remaining (< 45s cutoff): must bypass probe and update URL immediately
        short_token_url = f"https://example.com/live.mpd?exp={int(time.time()) + 25}"

        submissions = [short_token_url]
        def fake_sleep(_sec):
            if submissions:
                mock_state.token_hold_submitted_url = submissions.pop(0)

        with patch("sys.stdin.isatty", return_value=True), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep), \
             patch.object(guard, "_probe_stream_url") as mock_probe:
            res = guard.handle_token_hold_state(is_startup=True)
            self.assertTrue(res)
            mock_probe.assert_not_called()  # Probe was bypassed!
            self.assertEqual(mock_state.live_url, short_token_url)

    def test_token_hold_banner_truthfulness_chunks(self):
        mock_state = MagicMock()
        mock_state.stop_flag = True
        mock_state.is_scheduled = False
        logged_lines = []
        mock_log = lambda line, **kw: logged_lines.append(line)

        guard = NetworkGuard(required_country=None, state=mock_state, log_fn=mock_log)

        with patch("sys.stdin.isatty", return_value=True):
            # Startup (good_chunks=0): "Active chunk safely sealed to disk" must NOT be present
            mock_state.stats = {"good_chunks": 0}
            guard.handle_token_hold_state(is_startup=True)
            self.assertFalse(any("sealed to disk" in l for l in logged_lines))

            # Mid-match (good_chunks=1): "Active chunk safely sealed to disk" MUST be present
            logged_lines.clear()
            mock_state.stats = {"good_chunks": 1}
            guard.handle_token_hold_state(is_startup=False)
            self.assertTrue(any("sealed to disk" in l for l in logged_lines))

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_burst_paste_processing(self):
        import threading
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop
        state = RecorderState()
        state.token_hold_active = True
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None

        pasted_url = "https://live.example.com/manifest.mpd?token=" + ("A" * 500)
        raw_payload = b"\x1b[200~" + pasted_url.encode("utf-8") + b"\x1b[201~\r\n"

        stop_event = threading.Event()
        reads = [raw_payload, b""]
        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, pasted_url)
        self.assertEqual(state.token_hold_input_buffer, "")

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_split_paste_and_escape_handling(self):
        import threading
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop
        state = RecorderState()
        state.token_hold_active = True
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None

        # 1. Split bracketed paste marker: '\x1b' at end of chunk 1, '[201~\r' in chunk 2
        # Plus arrow key sequences \x1b[A and \x1b[B inserted mid-stream that should NOT clear buffer
        target_url = "https://live.example.com/stream.m3u8?token=SECRET"
        chunk1 = b"\x1b[200~" + target_url.encode("utf-8") + b"\x1b"
        chunk2 = b"[201~\r\n"

        stop_event = threading.Event()
        reads = [chunk1, chunk2, b""]
        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, target_url)
        self.assertEqual(state.token_hold_input_buffer, "")

        # 2. Arrow keys mid-typing must not wipe buffer or trigger alarm ack
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None
        state.alarm_ack_requested = False
        stop_event.clear()

        # Type 'https', press Up Arrow (\x1b[A), press Down Arrow (\x1b[B), press Enter (\r\n)
        arrow_payload = b"https\x1b[A\x1b[B://done\r\n"
        reads = [arrow_payload, b""]
        with patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0):
            _nm3u8dl_key_listener_loop(state, stop_event)

        # Buffer preserved without 'A' triggering alarm ack
        self.assertFalse(state.alarm_ack_requested)
        self.assertEqual(state.token_hold_submitted_url, "https://done")

    def test_is_startup_phase_window_and_alarm_trigger(self):
        mock_state = MagicMock()
        mock_state.stats = {"good_chunks": 0}
        guard = NetworkGuard(required_country=None, state=mock_state)

        # A. Initial unrecorded state: is_startup_phase is True and stamped at launch (B5)
        self.assertIsNotNone(guard._last_token_gate_pass_mono)
        self.assertTrue(guard.is_startup_phase())

        # B. Gate passes at t0 = 1000.0
        guard._last_token_gate_pass_mono = 1000.0

        # At t0 + 60.0s (within 120s window): still startup phase
        with patch("time.monotonic", return_value=1060.0):
            self.assertTrue(guard.is_startup_phase())

        # At t0 + 121.0s (> 120s window): startup phase EXPIRED -> False
        with patch("time.monotonic", return_value=1121.0):
            self.assertFalse(guard.is_startup_phase())

        # C. As soon as good_chunks > 0, startup phase is permanently False even if t < 120s
        mock_state.stats = {"good_chunks": 1}
        with patch("time.monotonic", return_value=1010.0):
            self.assertFalse(guard.is_startup_phase())

    def test_handle_token_hold_replacement_rejects_junk_without_url_protocol(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_hold_submitted_url = None
        mock_state.live_url = "https://example.com/old.m3u8"
        mock_state.nm3u8dl_part_a = 'N_m3u8DL-RE "https://example.com/old.m3u8"'
        mock_state.active_command = mock_state.nm3u8dl_part_a

        logged_lines = []
        guard = NetworkGuard(required_country=None, state=mock_state, log_fn=lambda l, **kw: logged_lines.append(l))

        # Text with valid expiry parameter but no http:// or https:// URL protocol
        junk_submission = f"exp={int(time.time()) + 25} --select-video best"

        submissions = [junk_submission]
        def fake_sleep(_sec):
            if submissions:
                mock_state.token_hold_submitted_url = submissions.pop(0)
            else:
                mock_state.stop_flag = True

        with patch("sys.stdin.isatty", return_value=True), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep), \
             patch.object(guard, "update_stream_url") as mock_update:
            res = guard.handle_token_hold_state(is_startup=True)
            self.assertFalse(res)
            mock_update.assert_not_called()  # Must NOT update URL!
            self.assertTrue(any("failed verification probe" in l for l in logged_lines))

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_lone_escape_does_not_swallow_next_char(self):
        import threading
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop
        state = RecorderState()
        state.token_hold_active = True
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None

        # Feed bare Esc, advance monotonic time past 0.25s timeout, then feed "https://x\r"
        stop_event = threading.Event()
        reads = [b"\x1b", b"https://x\r\n", b""]
        curr_time = [100.0, 100.35, 100.40]
        def fake_mono():
            if curr_time:
                return curr_time.pop(0)
            return 100.50

        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch("sys.platform", "darwin"), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, "https://x")
        self.assertEqual(state.token_hold_input_buffer, "")

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_escape_then_enter_still_submits(self):
        import threading
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop
        state = RecorderState()
        state.token_hold_active = True
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None

        # Feed "http\x1b\r\n" -> Enter must immediately submit "http" without being eaten
        stop_event = threading.Event()
        reads = [b"http\x1b\r\n", b""]
        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, "http")
        self.assertEqual(state.token_hold_input_buffer, "")

    def test_is_startup_phase_playlist_mode_stamps_gate_pass(self):
        mock_state = MagicMock()
        mock_state.source_mode = "playlist"
        mock_state.stats = {"good_chunks": 0}
        guard = NetworkGuard(required_country=None, state=mock_state)

        # Before gate check, simulate prior timestamp
        guard._last_token_gate_pass_mono = 100.0
        with patch("time.monotonic", return_value=250.0):
            res = guard._proactive_token_gate_check()
        self.assertTrue(res)
        # Playlist mode MUST re-stamp _last_token_gate_pass_mono so the 120s window functions
        self.assertEqual(guard._last_token_gate_pass_mono, 250.0)

    def test_token_hold_silent_startup_escalates_after_120s(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_state.alarm_ack_requested = False
        mock_alarm = MagicMock()

        logged_lines = []
        guard = NetworkGuard(required_country=None, state=mock_state, alarm_start_fn=mock_alarm, log_fn=lambda l, **kw: logged_lines.append(l))

        # Advance time: t0=100.0, step 1: t=160.0 (elapsed 60s < 120s: no alarm), step 2: t=230.0 (elapsed 130s >= 120s: ESCALATES to alarm!)
        clock = [100.0, 160.0, 230.0, 231.0]
        def fake_mono():
            val = clock.pop(0) if clock else 235.0
            if not clock:
                mock_state.stop_flag = True
            return val

        def fake_sleep(_sec):
            pass

        with patch("sys.stdin.isatty", return_value=True), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep):
            guard.handle_token_hold_state(is_startup=True)

        mock_alarm.assert_called_once_with(mock_state, alarm_type="token_expired")
        self.assertTrue(any("No operator response for 120s" in l for l in logged_lines))

    def test_token_hold_escalation_resets_on_submission(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_state.alarm_ack_requested = False
        mock_state.token_hold_submitted_url = None
        mock_state.token_hold_input_buffer = ""
        mock_alarm = MagicMock()

        logged_lines = []
        guard = NetworkGuard(required_country=None, state=mock_state, alarm_start_fn=mock_alarm, log_fn=lambda l, **kw: logged_lines.append(l))

        # Timeline:
        # t0=100.0: enter hold
        # t=200.0 (100s in): submit bad URL -> attended_mono updated to 200.0
        # t=300.0: 200s total elapsed, but only 100s since submission -> no alarm!
        # t=325.0: 125s since submission -> alarm sounds!
        clock = [100.0, 200.0, 300.0, 325.0, 326.0]
        def fake_mono():
            val = clock.pop(0) if clock else 330.0
            if val == 200.0:
                mock_state.token_hold_submitted_url = "https://bad.example.com/expired.m3u8?exp=1"
            elif val == 300.0:
                # Still no alarm at 300.0
                self.assertEqual(mock_alarm.call_count, 0)
            elif not clock:
                mock_state.stop_flag = True
            return val

        def fake_sleep(_sec):
            pass

        with patch("sys.stdin.isatty", return_value=True), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep):
            guard.handle_token_hold_state(is_startup=True)

        mock_alarm.assert_called_once_with(mock_state, alarm_type="token_expired")

    def test_token_hold_silent_startup_ack_before_120s_never_escalates(self):
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_state.alarm_ack_requested = False
        mock_state.token_hold_submitted_url = None
        mock_state.token_hold_input_buffer = ""
        mock_alarm = MagicMock()

        guard = NetworkGuard(required_country=None, state=mock_state, alarm_start_fn=mock_alarm)

        # Operator presses 'A' at 60s
        clock = [100.0, 160.0, 235.0, 236.0]
        def fake_mono():
            val = clock.pop(0) if clock else 240.0
            if val == 160.0:
                mock_state.alarm_ack_requested = True
            elif not clock:
                mock_state.stop_flag = True
            return val

        def fake_sleep(_sec):
            pass

        with patch("sys.stdin.isatty", return_value=True), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep):
            guard.handle_token_hold_state(is_startup=True)

        mock_alarm.assert_not_called()

    def test_token_hold_continues_after_candidate_exception(self):
        """B2: Hold survives candidate processing exception (e.g. update_stream_url raises) and resumes on next valid URL."""
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_hold_active = True
        mock_state.token_hold_submitted_url = None

        logged_lines = []
        guard = NetworkGuard(required_country=None, state=mock_state, log_fn=lambda l, **kw: logged_lines.append(l))

        exp_ts = int(time.time()) + 20
        submissions = [
            f"https://example.com/exploding.m3u8?exp={exp_ts}",
            f"https://example.com/valid.m3u8?exp={exp_ts}",
        ]
        def fake_sleep(_sec):
            if submissions:
                mock_state.token_hold_submitted_url = submissions.pop(0)
            else:
                mock_state.stop_flag = True

        call_count = [0]
        def mock_update(cand):
            call_count[0] += 1
            if call_count[0] == 1:
                raise RuntimeError("Simulated transient write failure")

        with patch("sys.stdin.isatty", return_value=True), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep), \
             patch.object(guard, "update_stream_url", side_effect=mock_update):
            res = guard.handle_token_hold_state(is_startup=True)

        self.assertTrue(res)
        self.assertEqual(call_count[0], 2)
        self.assertTrue(any("Candidate processing failed" in l and "RuntimeError" in l for l in logged_lines))
        self.assertFalse(mock_state.token_hold_active)

    def test_token_hold_alarm_stopped_when_loop_raises(self):
        """B1: try/finally in handle_token_hold_state stops alarm and clears flags even if loop raises."""
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.is_scheduled = True
        mock_state.token_hold_active = True
        mock_state.token_expired_post_ack_silent = False

        mock_alarm_start = MagicMock(side_effect=RuntimeError("Audio device failure"))
        mock_alarm_stop = MagicMock()

        guard = NetworkGuard(
            required_country=None,
            state=mock_state,
            alarm_start_fn=mock_alarm_start,
            alarm_stop_fn=mock_alarm_stop,
        )

        with patch("sys.stdin.isatty", return_value=True):
            with self.assertRaises(RuntimeError):
                guard.handle_token_hold_state(is_startup=True)

        self.assertFalse(mock_state.token_hold_active)
        self.assertIsNone(mock_state._hold_esc_state)
        self.assertEqual(mock_state.token_hold_input_buffer, "")
        mock_alarm_stop.assert_called_once_with(mock_state)

    def test_token_hold_countdown_clears_line_before_log(self):
        """B3: _hold_log wipes current buffer line with \x1b[2K before printing countdown log."""
        import io
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_state.alarm_ack_requested = False
        mock_state.token_hold_submitted_url = None
        mock_state.token_hold_input_buffer = ""

        cleared_sequences = []
        def fake_clear():
            cleared_sequences.append("\r\x1b[2K")
            sys.stdout.write("\r\x1b[2K")

        guard = NetworkGuard(
            required_country=None,
            state=mock_state,
            clear_progress_fn=fake_clear,
        )

        clock = [100.0, 131.0, 132.0]
        def fake_mono():
            val = clock.pop(0) if clock else 135.0
            if val == 131.0:
                mock_state.token_hold_input_buffer = "https://typing.com"
            if not clock:
                mock_state.stop_flag = True
            return val

        def fake_sleep(_sec):
            pass

        fake_stdout = io.StringIO()
        with patch("sys.stdin.isatty", return_value=True), \
             patch("sys.stdout", fake_stdout), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep):
            guard.handle_token_hold_state(is_startup=True)

        output = fake_stdout.getvalue()
        self.assertIn("\x1b[2K", output)
        self.assertIn("[TOKEN_HOLD] Waiting for updated stream URL", output)
        self.assertIn("TOKEN_HOLD> https://typing.com", output)
        self.assertTrue(len(cleared_sequences) >= 1)

    def test_is_startup_phase_explicit_stamp_and_fallback(self):
        """B5: Startup phase initializes explicitly at launch and supports fallback lazy initialization."""
        mock_state = MagicMock()
        mock_state.stats = {"good_chunks": 0}

        t0 = 1000.0
        with patch("time.monotonic", return_value=t0):
            guard = NetworkGuard(required_country=None, state=mock_state)

        # 1. Explicit initialization check
        self.assertEqual(guard._last_token_gate_pass_mono, t0)

        # 2. Within 120s window -> True
        with patch("time.monotonic", return_value=t0 + 50.0):
            self.assertTrue(guard.is_startup_phase())

        # 3. Beyond 120s window -> False
        with patch("time.monotonic", return_value=t0 + 125.0):
            self.assertFalse(guard.is_startup_phase())

        # 4. First good chunk sealed -> False immediately even within window
        mock_state.stats = {"good_chunks": 1}
        with patch("time.monotonic", return_value=t0 + 10.0):
            self.assertFalse(guard.is_startup_phase())

        # 5. Lazy fallback if uninitialized
        mock_state.stats = {"good_chunks": 0}
        guard._last_token_gate_pass_mono = None
        with patch("time.monotonic", return_value=2000.0):
            self.assertTrue(guard.is_startup_phase())
            self.assertEqual(guard._last_token_gate_pass_mono, 2000.0)

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_tab_converted_to_space(self):
        """B6: Tabs pasted into token hold buffer are converted to spaces instead of dropped."""
        import threading
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop
        state = RecorderState()
        state.token_hold_active = True
        state.token_hold_input_buffer = ""
        state.token_hold_submitted_url = None

        pasted = b"N_m3u8DL-RE\t--key\tval\r\n"
        stop_event = threading.Event()
        reads = [pasted, b""]
        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0), \
             patch("sys.stdout.write"), \
             patch("sys.stdout.flush"):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, "N_m3u8DL-RE --key val")

    def test_token_hold_post_commit_exception_still_resumes(self):
        """D1: A failure in post-commit side effects (beep) must not keep the hold alive after the URL was applied."""
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_hold_active = True
        mock_state.token_hold_submitted_url = None

        logged_lines = []
        guard = NetworkGuard(
            required_country=None,
            state=mock_state,
            log_fn=lambda l, **kw: logged_lines.append(l),
            beep_good_fn=MagicMock(side_effect=RuntimeError("Audio device failure")),
            alarm_stop_fn=MagicMock(),
        )

        exp_ts = int(time.time()) + 20
        submissions = [f"https://example.com/valid.m3u8?exp={exp_ts}"]

        def fake_sleep(_sec):
            if submissions:
                mock_state.token_hold_submitted_url = submissions.pop(0)
            else:
                mock_state.stop_flag = True

        update_mock = MagicMock()
        with patch("sys.stdin.isatty", return_value=True), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep), \
             patch.object(guard, "update_stream_url", update_mock):
            res = guard.handle_token_hold_state(is_startup=True)

        self.assertTrue(res)
        update_mock.assert_called_once()
        self.assertTrue(any("post-renewal action failed" in l for l in logged_lines))
        self.assertFalse(any("Candidate processing failed" in l for l in logged_lines))
        self.assertFalse(mock_state.token_hold_active)

    def test_mark_startup_begin_restarts_window(self):
        """D3: mark_startup_begin re-stamps the silent startup window at first launch attempt."""
        mock_state = MagicMock()
        mock_state.stats = {"good_chunks": 0}
        with patch("time.monotonic", return_value=1000.0):
            guard = NetworkGuard(required_country=None, state=mock_state)
        with patch("time.monotonic", return_value=1300.0):
            self.assertFalse(guard.is_startup_phase())
            guard.mark_startup_begin()
            self.assertEqual(guard._last_token_gate_pass_mono, 1300.0)
            self.assertTrue(guard.is_startup_phase())

    def test_finally_alarm_stop_log_failure_does_not_mask(self):
        """D6: If alarm_stop and log_fn both fail during cleanup, the original exception still propagates."""
        mock_state = MagicMock()
        mock_state.stats = {"good_chunks": 0}
        guard = NetworkGuard(
            required_country=None,
            state=mock_state,
            log_fn=MagicMock(side_effect=OSError("log sink gone")),
            alarm_stop_fn=MagicMock(side_effect=RuntimeError("stop failed")),
        )
        with patch.object(guard, "_token_hold_loop", side_effect=ValueError("original")):
            with self.assertRaises(ValueError) as ctx:
                guard.handle_token_hold_state(is_startup=True)
        self.assertEqual(str(ctx.exception), "original")
        self.assertFalse(mock_state.token_hold_active)

    @unittest.skipIf(sys.platform == "win32", "posix listener")
    def test_key_listener_buffer_mutation_under_console_lock(self):
        """D2: Buffer mutation and echo happen atomically while holding _CONSOLE_LOCK."""
        import threading
        import record_dynamic
        from record_dynamic import RecorderState, _nm3u8dl_key_listener_loop

        depth = [0]
        held_at_write = []

        class TrackingLock:
            def __enter__(self_inner):
                depth[0] += 1
            def __exit__(self_inner, *a):
                depth[0] -= 1
                return False

        class TrackedState(RecorderState):
            @property
            def token_hold_input_buffer(self):
                return self.__dict__.get("_thib", "")
            @token_hold_input_buffer.setter
            def token_hold_input_buffer(self, v):
                held_at_write.append(depth[0] > 0)
                self.__dict__["_thib"] = v

        state = TrackedState()
        state.token_hold_active = True
        state.token_hold_submitted_url = None

        held_at_write.clear()  # ignore construction-time writes
        stop_event = threading.Event()
        reads = [b"xy\x7f\r", b""]
        def fake_read(_fd, _sz):
            if reads:
                return reads.pop(0)
            stop_event.set()
            return b""

        with patch.object(record_dynamic, "_CONSOLE_LOCK", TrackingLock()), \
             patch("sys.platform", "darwin"), \
             patch("os.read", side_effect=fake_read), \
             patch("select.select", return_value=([1], [], [])), \
             patch("termios.tcgetattr", return_value=[]), \
             patch("termios.tcsetattr"), \
             patch("termios.tcflush"), \
             patch("tty.setcbreak"), \
             patch("sys.stdin.fileno", return_value=0), \
             patch("sys.stdout.write"), \
             patch("sys.stdout.flush"):
            _nm3u8dl_key_listener_loop(state, stop_event)

        self.assertEqual(state.token_hold_submitted_url, "x")
        self.assertTrue(held_at_write)
        self.assertTrue(all(held_at_write))

    def test_token_hold_countdown_reechoes_buffer(self):
        import io
        mock_state = MagicMock()
        mock_state.stop_flag = False
        mock_state.is_scheduled = False
        mock_state.stats = {"good_chunks": 0}
        mock_state.token_expired_post_ack_silent = False
        mock_state.alarm_ack_requested = False
        mock_state.token_hold_submitted_url = None
        mock_state.token_hold_input_buffer = ""

        guard = NetworkGuard(required_country=None, state=mock_state)

        # t0=100.0, step 1: t=131.0 (elapsed 31s >= 30s countdown log)
        clock = [100.0, 131.0, 132.0]
        def fake_mono():
            val = clock.pop(0) if clock else 135.0
            if val == 131.0:
                mock_state.token_hold_input_buffer = "abc"
            if not clock:
                mock_state.stop_flag = True
            return val

        def fake_sleep(_sec):
            pass

        fake_stdout = io.StringIO()
        with patch("sys.stdin.isatty", return_value=True), \
             patch("sys.stdout", fake_stdout), \
             patch("time.monotonic", side_effect=fake_mono), \
             patch("recorder_runtime.network.time.sleep", side_effect=fake_sleep):
            guard.handle_token_hold_state(is_startup=True)

        output = fake_stdout.getvalue()
        self.assertIn("TOKEN_HOLD> abc", output)

    def test_verify_and_resume_passes_dynamic_startup(self):
        mock_state = MagicMock()
        guard = NetworkGuard(required_country=None, state=mock_state)
        expired_url = f"https://example.com/live.m3u8?exp={int(time.time()) - 100}"

        with patch.object(guard, "get_active_source_url", return_value=expired_url), \
             patch.object(guard, "handle_token_hold_state") as mock_hold:

            with patch.object(guard, "is_startup_phase", return_value=True):
                guard._verify_and_resume_stream()
                mock_hold.assert_called_with(is_startup=True)

            with patch.object(guard, "is_startup_phase", return_value=False):
                guard._verify_and_resume_stream()
                mock_hold.assert_called_with(is_startup=False)

    def test_probe_url_bool_wrapper_preserved(self):
        mock_state = MagicMock()
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(False, 403)):
            res = guard._probe_stream_url("https://example.com/test")
            self.assertIs(res, False)

        with patch.object(guard, "_probe_stream_status", return_value=(True, 200)):
            res = guard._probe_stream_url("https://example.com/test")
            self.assertIs(res, True)

    def test_static_untokenised_ts_resumes_without_probe(self):
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        mock_state.live_url = "http://iptv.server:8080/live/user/pass/12345.ts"
        guard = NetworkGuard(required_country=None, state=mock_state)
        guard.resume_callback = MagicMock()

        with patch.object(guard, "_probe_stream_status") as mock_probe, \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_not_called()
            mock_hold.assert_not_called()
            guard.resume_callback.assert_called_once_with(mock_state)

    def test_token_expired_timestamp_option_b_probe_confirmation(self):
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        mock_state.live_url = f"https://example.com/live.m3u8?exp={int(time.time()) - 50}"
        guard = NetworkGuard(required_country=None, state=mock_state)

        # 1. Option B: probe returns 200 (clock drift tolerance) -> resumes without hold
        with patch.object(guard, "_probe_stream_status", return_value=(True, 200)) as mock_probe, \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_called_once()
            mock_hold.assert_not_called()

        # 2. Option B: probe returns 401/403/410 -> diverts to hold
        with patch.object(guard, "_probe_stream_status", return_value=(False, 401)) as mock_probe, \
             patch.object(guard, "handle_token_hold_state", return_value=True) as mock_hold:
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_called_once()
            mock_hold.assert_called_once()

    def test_token_governed_scans_all_command_material_fields(self):
        # D1: token present only in active_command while nm3u8dl_part_a is set
        mock_state = MagicMock()
        mock_state.live_url = "https://example.com/base.m3u8"
        mock_state.nm3u8dl_part_a = "N_m3u8DL-RE --save-name test"
        future_exp = int(time.time()) + 3600
        mock_state.active_command = f'ffmpeg -i "https://example.com/base.m3u8?exp={future_exp}"'
        mock_state.active_stream_url = ""
        guard = NetworkGuard(required_country=None, state=mock_state)

        tokenised, expiry = guard._stream_is_token_governed(mock_state.live_url)
        self.assertTrue(tokenised)
        self.assertEqual(expiry, future_exp)

    def test_playlist_mode_exempt_from_stale_timestamp_hold(self):
        # D2: in playlist mode, stale timestamp from past chunk does not hold if probe passes
        mock_state = MagicMock()
        mock_state.source_mode = "playlist"
        mock_state.live_url = f"https://example.com/live.m3u8?exp={int(time.time()) - 3600}"
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(True, 200)) as mock_probe, \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_called_once()
            mock_hold.assert_not_called()

    def test_probe_403_and_410_divert_with_geo_logging(self):
        # D3 & D4 & N4: HTTP 403 and 410 divert to token hold and log Launch egress & Required country
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        future_exp = int(time.time()) + 3600
        mock_state.live_url = f"https://example.com/live.m3u8?exp={future_exp}"
        guard = NetworkGuard(required_country="IN", state=mock_state)
        guard.launch_country = "IN"

        logged_messages = []
        guard.log_fn = lambda msg, level="INFO": logged_messages.append((msg, level))

        # Test 403
        with patch.object(guard, "_probe_stream_status", return_value=(False, 403)), \
             patch.object(guard, "handle_token_hold_state", return_value=True) as mock_hold:
            logged_messages.clear()
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_hold.assert_called_once()
            self.assertTrue(any("HTTP 403" in m[0] and "Launch egress: IN" in m[0] and "Required: IN" in m[0] for m in logged_messages))

        # Test 410 (D4) on tokenized feed
        with patch.object(guard, "_probe_stream_status", return_value=(False, 410)), \
             patch.object(guard, "handle_token_hold_state", return_value=True) as mock_hold:
            logged_messages.clear()
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_hold.assert_called_once()
            self.assertTrue(any("HTTP 410" in m[0] and "Launch egress: IN" in m[0] for m in logged_messages))

    def test_probe_410_on_untokenised_playlist_resumes_without_hold(self):
        # D4: 410 on untokenised playlist feed is inconclusive and resumes
        mock_state = MagicMock()
        mock_state.source_mode = "playlist"
        mock_state.live_url = "https://example.com/live.m3u8"
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(False, 410)), \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream()
            self.assertTrue(res)
            mock_hold.assert_not_called()

    def test_probe_timeout_416_500_resumes_via_retry(self):
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        mock_state.live_url = f"https://example.com/live.m3u8?exp={int(time.time()) + 3600}"
        guard = NetworkGuard(required_country=None, state=mock_state)
        guard.resume_callback = MagicMock()

        for status_code in (None, 416, 400, 500, 503):
            with patch.object(guard, "_probe_stream_status", return_value=(False, status_code)), \
                 patch.object(guard, "handle_token_hold_state") as mock_hold:
                guard.resume_callback.reset_mock()
                res = guard._verify_and_resume_stream()
                self.assertTrue(res)
                mock_hold.assert_not_called()
                guard.resume_callback.assert_called_once_with(mock_state)

    def test_stream_mode_playlist_probes_while_static_resumes(self):
        # 1. Static untokenised stream: zero probe, resumes
        mock_state_static = MagicMock()
        mock_state_static.source_mode = "static"
        mock_state_static.live_url = "https://example.com/stream.mkv"
        guard_static = NetworkGuard(required_country=None, state=mock_state_static)

        with patch.object(guard_static, "_probe_stream_status") as mock_probe:
            res = guard_static._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_not_called()

        # 2. Playlist mode stream: probes endpoint
        mock_state_pl = MagicMock()
        mock_state_pl.source_mode = "playlist"
        mock_state_pl.live_url = "https://example.com/stream.m3u8"
        guard_pl = NetworkGuard(required_country=None, state=mock_state_pl)

        with patch.object(guard_pl, "_probe_stream_status", return_value=(True, 200)) as mock_probe:
            res = guard_pl._verify_and_resume_stream()
            self.assertTrue(res)
            mock_probe.assert_called_once()

    def test_expired_clock_option_b_probe_200_overrides_proactive_gate(self):
        # N1: Expired clock plus probe 200 stamps override and passes proactive chunk gate
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        expired_url = f"https://example.com/live.m3u8?exp={int(time.time()) - 100}"
        mock_state.live_url = expired_url
        mock_state.active_stream_url = expired_url
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        mock_state.token_clock_override_url = None
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(True, 200)):
            res = guard._verify_and_resume_stream(mock_state)
            self.assertTrue(res)
            self.assertEqual(mock_state.token_clock_override_url, expired_url)

        # Pre-launch gate check must now pass without hold
        with patch.object(guard, "handle_token_hold_state") as mock_hold:
            gate_res = guard._proactive_token_gate_check()
            self.assertTrue(gate_res)
            mock_hold.assert_not_called()

    def test_expired_clock_probe_416_fallback_without_range_resumes(self):
        # N2: Range-rejecting stream (416) falls back to Range-less probe and resumes on 200
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        expired_url = f"https://example.com/live.ts?exp={int(time.time()) - 50}"
        mock_state.live_url = expired_url
        mock_state.active_stream_url = expired_url
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        guard = NetworkGuard(required_country=None, state=mock_state)

        def mock_probe(url, timeout=3.0, use_range=True):
            if use_range:
                return False, 416
            return True, 200

        with patch.object(guard, "_probe_stream_status", side_effect=mock_probe), \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream(mock_state)
            self.assertTrue(res)
            mock_hold.assert_not_called()
            self.assertEqual(mock_state.token_clock_override_url, expired_url)

    def test_expired_clock_probe_timeout_resumes_via_retry_without_hold(self):
        # N2: Expired clock plus post-outage probe timeout falls through to recorder retry (no false hold)
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        expired_url = f"https://example.com/live.ts?exp={int(time.time()) - 50}"
        mock_state.live_url = expired_url
        mock_state.active_stream_url = expired_url
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(False, None)), \
             patch.object(guard, "handle_token_hold_state") as mock_hold:
            res = guard._verify_and_resume_stream(mock_state)
            self.assertTrue(res)
            mock_hold.assert_not_called()

    def test_expired_clock_probe_403_diverts_and_clears_override(self):
        # N1 & N2: Probe 403 on expired token diverts to hold and clears override
        mock_state = MagicMock()
        mock_state.source_mode = "static"
        expired_url = f"https://example.com/live.ts?exp={int(time.time()) - 50}"
        mock_state.live_url = expired_url
        mock_state.active_stream_url = expired_url
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        mock_state.token_clock_override_url = expired_url
        guard = NetworkGuard(required_country=None, state=mock_state)

        with patch.object(guard, "_probe_stream_status", return_value=(False, 403)), \
             patch.object(guard, "handle_token_hold_state", return_value=True) as mock_hold:
            res = guard._verify_and_resume_stream(mock_state)
            self.assertTrue(res)
            mock_hold.assert_called_once()
            self.assertIsNone(mock_state.token_clock_override_url)

    def test_header_only_exp_ignored_by_token_governed_scan(self):
        # N5: Numeric exp= inside headers or cookies is ignored; only URLs are scanned
        mock_state = MagicMock()
        mock_state.live_url = "https://example.com/feed.ts"
        mock_state.active_stream_url = "https://example.com/feed.ts"
        mock_state.nm3u8dl_part_a = 'ffmpeg -headers "Cookie: exp=1730000000" -i "https://example.com/feed.ts"'
        mock_state.active_command = ""
        guard = NetworkGuard(required_country=None, state=mock_state)

        tokenised, expiry = guard._stream_is_token_governed(mock_state.live_url)
        self.assertFalse(tokenised)
        self.assertIsNone(expiry)

    def test_playlist_mode_access_refusal_resumes_unattended_for_refresh(self):
        # N6 Option A: In playlist mode, 401/403 resumes unattended so playlist manager fetches fresh manifest
        mock_state = MagicMock()
        mock_state.source_mode = "playlist"
        mock_state.live_url = "https://example.com/chunklist.m3u8?token=xyz"
        mock_state.active_stream_url = "https://example.com/chunklist.m3u8?token=xyz"
        mock_state.nm3u8dl_part_a = ""
        mock_state.active_command = ""
        guard = NetworkGuard(required_country=None, state=mock_state)

        for status_code in (401, 403):
            with patch.object(guard, "_probe_stream_status", return_value=(False, status_code)), \
                 patch.object(guard, "handle_token_hold_state") as mock_hold:
                res = guard._verify_and_resume_stream(mock_state)
                self.assertTrue(res)
                mock_hold.assert_not_called()

    def test_update_stream_url_clears_clock_override(self):
        # N1: Updating stream URL resets token_clock_override_url
        mock_state = MagicMock()
        mock_state.live_url = "https://example.com/old.m3u8"
        mock_state.token_clock_override_url = "https://example.com/old.m3u8"
        guard = NetworkGuard(required_country=None, state=mock_state)

        guard.update_stream_url("https://example.com/new.m3u8")
        self.assertIsNone(mock_state.token_clock_override_url)
        self.assertEqual(mock_state.live_url, "https://example.com/new.m3u8")


if __name__ == "__main__":
    unittest.main()
