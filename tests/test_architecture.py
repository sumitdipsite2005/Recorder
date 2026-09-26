from __future__ import annotations

import ast
from pathlib import Path
import unittest

ROOT = Path(__file__).resolve().parents[1]


class ArchitectureGuardTests(unittest.TestCase):
    def test_coordinator_does_not_import_mature_recorder(self):
        text=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        tree=ast.parse(text)
        imported=[]
        for node in ast.walk(tree):
            if isinstance(node,ast.Import): imported.extend(alias.name for alias in node.names)
            elif isinstance(node,ast.ImportFrom) and node.module: imported.append(node.module)
        self.assertNotIn("record_dynamic", imported)

    def test_coordinator_does_not_launch_worker_or_subprocess_at_checkpoint_2(self):
        text=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        self.assertNotIn("subprocess", text)
        self.assertIn("from recorder_coordinator.terminal import", text)
        self.assertNotIn("winsound", text)
        self.assertNotIn('os.system("cls"', text)
        self.assertNotIn("record_dynamic_event_worker", text)

    def test_manual_launch_keeps_command_building_inside_mature_recorder(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        coordinator=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        worker=(ROOT/"recorder_coordinator"/"worker.py").read_text(encoding="utf-8")
        request=(ROOT/"recorder_runtime"/"identity_launch.py").read_text(encoding="utf-8")

        self.assertIn("Using Coordinator-selected startup source", recorder)
        self.assertIn("source = resolve_nm3u8dl_launch_source(state)", recorder)
        self.assertIn('"N_m3u8DL-RE"', recorder)
        self.assertNotIn("N_m3u8DL-RE", coordinator)
        self.assertNotIn("N_m3u8DL-RE", worker)
        self.assertNotIn("N_m3u8DL-RE", request)

    def test_identity_initial_source_returns_before_any_playlist_rescan(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        start=recorder.index("if state.identity_initial_source is not None:")
        end=recorder.index("elif direct_retry_source is not None:", start)
        branch=recorder[start:end]
        self.assertIn("return source", branch)
        self.assertNotIn("resolve_nm3u8dl_playlist_source(", branch)

    def test_identity_worker_launcher_does_not_create_separate_console_windows(self):
        worker=(ROOT/"recorder_coordinator"/"worker.py").read_text(encoding="utf-8")
        terminal_host=(ROOT/"recorder_runtime"/"terminal_host.py").read_text(encoding="utf-8")
        self.assertNotIn("CREATE_NEW_CONSOLE", worker)
        self.assertIn('"wt.exe"', terminal_host)
        self.assertIn('"-w"', terminal_host)
        self.assertIn('"0"', terminal_host)
        self.assertIn('"new-tab"', terminal_host)

    def test_direct_dynamic_recorder_entrypoint_still_uses_same_process_runner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        self.assertIn('if __name__ == "__main__":\n    run_recorder_process()', recorder)

    def test_recorder_and_coordinator_share_sound_snooze_runtime_core(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        coordinator=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        self.assertIn("from recorder_runtime import sound as runtime_sound", recorder)
        self.assertIn("from recorder_runtime import sound as runtime_sound", coordinator)

    def test_coordinator_config_and_acquisition_have_single_module_owners(self):
        entry=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        config=(ROOT/"recorder_coordinator"/"configuration.py").read_text(encoding="utf-8")
        acquisition=(ROOT/"recorder_coordinator"/"acquisition.py").read_text(encoding="utf-8")

        self.assertIn("class CoordinatorConfigState:", config)
        self.assertIn("def acquire_active_targets(", acquisition)
        self.assertNotIn("class CoordinatorConfigState:", entry)
        self.assertNotIn("def acquire_active_targets(", entry)
        self.assertIn("from recorder_coordinator.acquisition import acquire_active_targets", entry)
        self.assertIn("from recorder_coordinator.configuration import (", entry)

    def test_identity_registry_lives_in_shared_runtime_layer(self):
        self.assertTrue((ROOT/"recorder_runtime"/"registry.py").is_file())
        self.assertFalse((ROOT/"recorder_coordinator"/"registry.py").exists())
        for path in (
            ROOT/"recorder_event_coordinator.py",
            ROOT/"recorder_identity_worker.py",
            ROOT/"recorder_coordinator"/"worker.py",
        ):
            text=path.read_text(encoding="utf-8")
            self.assertIn("recorder_runtime.registry", text)
            self.assertNotIn("recorder_coordinator.registry", text)

    def test_shared_source_package_does_not_depend_on_entrypoint_files(self):
        for path in (ROOT/"recorder_source").glob("*.py"):
            text=path.read_text(encoding="utf-8")
            self.assertNotIn("import record_dynamic",text,path.name)
            self.assertNotIn("import recorder_event_coordinator",text,path.name)

    def test_shared_policy_is_single_source_for_coordinator_common_rules(self):
        text=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        self.assertIn("from recorder_source.policy import",text)
        for duplicate in ("GROUP_PROVIDER = {","GROUP_MATCH_MODE = {","GROUP_SOURCE_BUCKET = {","PROVIDER_SELECTION_POLICY = {","PLAYLIST_USER_AGENTS = {"):
            self.assertNotIn(duplicate,text)

    def test_generic_refactored_layers_do_not_hardcode_provider_brands(self):
        allowed = {
            ROOT/"recorder_source"/"policy.py",
            ROOT/"recorder_source"/"identity.py",
        }
        forbidden = ("FANCODE", "HOTSTAR", "SONYLIV", "SONY_TV", "JIO_STAR", "KHEL")
        paths = [
            *sorted((ROOT/"recorder_coordinator").glob("*.py")),
            *sorted((ROOT/"recorder_runtime").glob("*.py")),
            *sorted((ROOT/"recorder_source").glob("*.py")),
        ]
        for path in paths:
            if path in allowed:
                continue
            text = path.read_text(encoding="utf-8").upper()
            for brand in forbidden:
                self.assertNotIn(brand, text, f"{brand} leaked into generic module {path.name}")

    def test_refactored_modules_do_not_copy_substantial_function_bodies(self):
        roots = (
            ROOT/"recorder_coordinator",
            ROOT/"recorder_runtime",
            ROOT/"recorder_source",
        )
        seen = {}
        for folder in roots:
            for path in sorted(folder.glob("*.py")):
                tree = ast.parse(path.read_text(encoding="utf-8"))
                for node in ast.walk(tree):
                    if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
                        continue
                    span = int(getattr(node, "end_lineno", node.lineno)) - int(node.lineno) + 1
                    if span < 12:
                        continue
                    body = ast.dump(ast.Module(body=node.body, type_ignores=[]), include_attributes=False)
                    previous = seen.get(body)
                    if previous is not None:
                        self.fail(
                            "substantial duplicate function body: "
                            f"{previous[0]}:{previous[1]} and {path}:{node.lineno}"
                        )
                    seen[body] = (path, node.lineno)

    def test_entrypoint_file_names_follow_python_convention(self):
        for path in ROOT.glob("*.py"):
            self.assertEqual(path.name, path.name.lower(), path.name)
        for path in ROOT.glob("*.bat"):
            self.assertEqual(path.name, path.name.lower(), path.name)

    def test_stream_type_from_url_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        transport=(ROOT/"recorder_source"/"transport.py").read_text(encoding="utf-8")

        self.assertIn("def stream_type_from_url(", transport)
        self.assertIn(
            "return source_transport.stream_type_from_url(stream_url)",
            recorder,
        )
        self.assertIn(
            "source_transport.stream_type_from_url(stream_url)",
            discovery,
        )
        self.assertNotIn("def _stream_type_from_url(", discovery)

    def test_remaining_probe_primitives_have_shared_owners(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        transport=(ROOT/"recorder_source"/"transport.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")

        self.assertIn("def curl_get_text(", transport)
        curl_start = recorder.index("def _run_nm3u8dl_curl_get_text(")
        curl_end = recorder.index(
            "def _run_nm3u8dl_curl_status_request(",
            curl_start,
        )
        mature_curl = recorder[curl_start:curl_end]
        self.assertIn("source_transport.curl_get_text(", mature_curl)
        self.assertNotIn("__RECORDER_CURL_HTTP_STATUS__", mature_curl)
        self.assertNotIn("subprocess.run(", mature_curl)

        frame_start = recorder.index("def _parse_nm3u8dl_frame_rate(")
        frame_end = recorder.index(
            "def _normalize_nm3u8dl_video_scan_type(",
            frame_start,
        )
        mature_frame = recorder[frame_start:frame_end]
        self.assertIn("source_quality.parse_frame_rate(value)", mature_frame)
        self.assertNotIn('if "/" in text:', mature_frame)
        self.assertIn("def parse_frame_rate(", quality)

        bitrate_start = recorder.index(
            "def _sample_nm3u8dl_stream_video_bitrate("
        )
        bitrate_end = recorder.index(
            "def _parse_nm3u8dl_idet_scan_type(",
            bitrate_start,
        )
        mature_bitrate = recorder[bitrate_start:bitrate_end]
        self.assertIn(
            "source_quality.sample_stream_video_bitrate(",
            mature_bitrate,
        )
        self.assertNotIn(
            "source_quality.build_ffmpeg_bitrate_sample_command(",
            mature_bitrate,
        )
        self.assertNotIn(
            "source_quality.parse_ffmpeg_bitrate_progress(",
            mature_bitrate,
        )
        self.assertIn("def sample_stream_video_bitrate(", quality)

    def test_coordinator_scan_uses_shared_cooperative_cancellation_contract(self):
        coordinator=(ROOT/"recorder_event_coordinator.py").read_text(encoding="utf-8")
        acquisition=(ROOT/"recorder_coordinator"/"acquisition.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")

        self.assertIn('stop_event.set()', coordinator)
        self.assertIn('command_queue.put("__CTRL_C__")', coordinator)
        self.assertIn("stop_requested=stop_event.is_set", coordinator)
        self.assertIn("stop_requested=stop_requested", acquisition)
        self.assertIn("def fetch_playlist_documents(", discovery)
        self.assertIn("def probe_candidates(", discovery)
        self.assertIn("stop_requested=stop_requested", discovery)
        self.assertIn("def inspect_manifest_probe_evidence(", quality)
        self.assertIn("stop_requested=stop_requested", recorder)

    def test_lifecycle_scan_type_policy_has_one_shared_owner(self):
        policy=(ROOT/"recorder_source"/"policy.py").read_text(encoding="utf-8")
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")

        self.assertIn("def apply_lifecycle_scan_type_policy(", policy)
        self.assertIn("shared_apply_lifecycle_scan_type_policy(", recorder)
        self.assertIn("apply_lifecycle_scan_type_policy(", discovery)
        self.assertNotIn(
            'quality["video_scan_type_source"] = "event-policy"',
            recorder,
        )
        self.assertNotIn(
            'video_scan_type_source = "event-policy"',
            discovery,
        )

    def test_manifest_content_classification_has_one_shared_owner(self):
        manifest=(ROOT/"recorder_source"/"manifest.py").read_text(encoding="utf-8")
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        transport=(ROOT/"recorder_source"/"transport.py").read_text(encoding="utf-8")

        self.assertIn("def manifest_type_from_text(", manifest)
        self.assertIn("from .manifest import manifest_type_from_text", quality)
        self.assertIn(
            "manifest_type = manifest_type_from_text(manifest_text)",
            quality,
        )
        self.assertIn(
            "source_quality.inspect_manifest_probe_evidence(",
            recorder,
        )
        self.assertIn("inspect_manifest_probe_evidence(", discovery)
        self.assertIn("is_manifest_text(", transport)
        mpd_pattern = r'<(?:[A-Za-z_][\w.-]*:)?MPD\b'
        self.assertIn(mpd_pattern, manifest)
        self.assertNotIn(mpd_pattern, recorder)
        self.assertNotIn(mpd_pattern, discovery)
        self.assertNotIn(mpd_pattern, quality)
        self.assertNotIn(mpd_pattern, transport)

    def test_recorder_and_coordinator_share_hls_quality_parser(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        self.assertIn("def parse_hls_manifest_quality(",quality)
        self.assertIn("parse_hls_manifest_quality(",quality)
        self.assertIn("source_quality.inspect_manifest_probe_evidence(",recorder)
        self.assertIn("inspect_manifest_probe_evidence(",discovery)

    def test_recorder_and_coordinator_share_probe_transport_policy(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        self.assertIn("from recorder_source import transport as source_transport", recorder)
        self.assertIn("source_transport.fetch_stream_manifest_text(", recorder)
        self.assertIn("source_transport.fetch_stream_manifest_text(", discovery)

    def test_recorder_and_coordinator_share_hls_variant_failure_classification(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        self.assertIn(
            "source_transport.classify_hls_variant_probe_failure(",
            quality,
        )
        self.assertIn("source_quality.inspect_manifest_probe_evidence(", recorder)
        self.assertIn("inspect_manifest_probe_evidence(", discovery)

    def test_recorder_and_coordinator_share_dash_quality_parser(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        self.assertIn("def parse_dash_manifest_quality(", quality)
        self.assertIn("parse_dash_manifest_quality(", quality)
        self.assertIn("source_quality.inspect_manifest_probe_evidence(", recorder)
        self.assertIn("inspect_manifest_probe_evidence(", discovery)
        self.assertNotIn("def _parse_dash_quality(", discovery)

    def test_json_playlist_adaptation_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        shared=(ROOT/"recorder_source"/"json_playlist.py").read_text(encoding="utf-8")

        self.assertIn("def adapt_json_playlist_text(", shared)
        self.assertIn("MATURE_JSON_PLAYLIST_POLICY =", shared)
        self.assertIn("NORMALIZED_JSON_PLAYLIST_POLICY =", shared)

        mature_start = recorder.index("def adapt_nm3u8dl_json_playlist_text(")
        mature_end = recorder.index(
            "def fetch_nm3u8dl_playlist_text(",
            mature_start,
        )
        mature_adapter = recorder[mature_start:mature_end]
        self.assertIn(
            "source_json_playlist.adapt_json_playlist_text(",
            mature_adapter,
        )
        self.assertIn("MATURE_JSON_PLAYLIST_POLICY", mature_adapter)
        self.assertNotIn("json.loads(", mature_adapter)
        self.assertNotIn("_find_nm3u8dl_json_records", recorder)

        normalized_start = discovery.index("def adapt_json_playlist_text(")
        normalized_end = discovery.index(
            "def parse_extinf_metadata(",
            normalized_start,
        )
        normalized_adapter = discovery[normalized_start:normalized_end]
        self.assertIn(
            "source_json_playlist.adapt_json_playlist_text(",
            normalized_adapter,
        )
        self.assertIn("NORMALIZED_JSON_PLAYLIST_POLICY", normalized_adapter)
        self.assertNotIn("json.loads(", normalized_adapter)
        self.assertNotIn("def _normalize_json_field_name(", discovery)

    def test_playlist_header_parsing_algorithm_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        shared=(ROOT/"recorder_source"/"playlist_headers.py").read_text(encoding="utf-8")
        self.assertIn("def parse_stream_url_and_headers(", shared)
        self.assertIn("source_playlist_headers.parse_stream_url_and_headers(", recorder)
        self.assertIn("parse_stream_url_and_headers(", discovery)
        self.assertNotIn("metadata_text.split(\"&\")", recorder)
        self.assertNotIn('lower.startswith("#extvlcopt:', discovery)

    def test_quality_signature_has_one_shared_owner(self):
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        snapshot=(ROOT/"recorder_coordinator"/"snapshot.py").read_text(encoding="utf-8")
        terminal=(ROOT/"recorder_coordinator"/"terminal.py").read_text(encoding="utf-8")

        self.assertIn("def quality_signature(", quality)
        self.assertIn("return quality_signature(candidate)", snapshot)
        self.assertIn("return quality_signature(candidate)", terminal)
        self.assertNotIn("round(float(candidate.video_fps", snapshot)
        self.assertNotIn("round(float(candidate.video_fps", terminal)

    def test_quality_probe_identity_and_formatter_are_shared(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        snapshot=(ROOT/"recorder_coordinator"/"snapshot.py").read_text(encoding="utf-8")
        self.assertIn("source_quality.quality_probe_identity(",recorder)
        self.assertIn("quality_probe_identity(",discovery)
        self.assertIn("source_quality.format_candidate_quality(",recorder)
        self.assertIn("format_candidate_quality(",snapshot)

    def test_quality_probe_completion_and_concurrency_have_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")

        self.assertIn("source_quality.probe_stream_quality_ffprobe(", recorder)
        self.assertIn("probe_stream_quality_ffprobe(", discovery)
        self.assertIn("sample_missing_bitrate=(bitrate <= 0)", discovery)
        self.assertIn("timeout_sec=QUALITY_FFPROBE_TIMEOUT_SEC", discovery)
        self.assertNotIn("sample_stream_video_bitrate(", discovery)
        self.assertNotIn("source_quality.parse_ffprobe_quality_output(", recorder)
        self.assertIn(
            "NM3U8DL_QUALITY_PROBE_WORKERS = source_quality.QUALITY_PROBE_WORKERS",
            recorder,
        )
        self.assertIn(
            "NM3U8DL_QUALITY_FFPROBE_TIMEOUT_SEC = source_quality.QUALITY_FFPROBE_TIMEOUT_SEC",
            recorder,
        )
        self.assertIn("max_workers: int = QUALITY_PROBE_WORKERS", discovery)
        self.assertIn("QUALITY_PROBE_WORKERS = 6", quality)
        self.assertIn("QUALITY_FFPROBE_TIMEOUT_SEC = 20.0", quality)
        self.assertIn("def run_grouped_quality_probes(", quality)

        mature_batch_start = recorder.index(
            "def enrich_nm3u8dl_candidate_qualities("
        )
        mature_batch_end = recorder.index(
            "def format_nm3u8dl_access_block_warning(",
            mature_batch_start,
        )
        mature_batch = recorder[mature_batch_start:mature_batch_end]
        self.assertIn(
            "source_quality.run_grouped_quality_probes(",
            mature_batch,
        )
        self.assertNotIn("ThreadPoolExecutor(", mature_batch)
        self.assertNotIn("as_completed(", mature_batch)

        shared_batch_start = discovery.index("def probe_candidates(")
        shared_batch = discovery[shared_batch_start:]
        self.assertIn("run_grouped_quality_probes(", shared_batch)
        self.assertNotIn("ThreadPoolExecutor(", shared_batch)
        self.assertNotIn("as_completed(", shared_batch)

        general_probe_start = recorder.index(
            "for ffprobe_key in ffprobe_key_values:"
        )
        general_probe_end = recorder.index(
            "# If DASH bitrate is still absent",
            general_probe_start,
        )
        general_probe = recorder[general_probe_start:general_probe_end]
        self.assertIn(
            "source_quality.merge_ffprobe_quality_evidence(",
            general_probe,
        )
        self.assertNotIn("ffprobe_fps =", general_probe)
        self.assertIn("merge_ffprobe_quality_evidence(", discovery)
        self.assertNotIn("ffprobe_width =", discovery)

    def test_candidate_manifest_probe_orchestration_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")

        self.assertIn("def inspect_manifest_probe_evidence(", quality)

        mature_start = recorder.index("def _probe_nm3u8dl_candidate_quality(")
        mature_end = recorder.index(
            "def _get_nm3u8dl_candidate_probe_identity(",
            mature_start,
        )
        mature_probe = recorder[mature_start:mature_end]
        self.assertIn(
            "source_quality.inspect_manifest_probe_evidence(",
            mature_probe,
        )
        self.assertNotIn(
            "_parse_nm3u8dl_hls_manifest_quality(",
            mature_probe,
        )
        self.assertNotIn(
            "_parse_nm3u8dl_dash_manifest_quality(",
            mature_probe,
        )
        self.assertNotIn(
            "_inspect_nm3u8dl_hls_manifest_drm(",
            mature_probe,
        )
        self.assertNotIn(
            "classify_hls_variant_probe_failure(",
            mature_probe,
        )

        shared_start = discovery.index("def probe_candidate_hls(")
        shared_end = discovery.index("_PROBE_SOURCE_EXTRA_KEYS", shared_start)
        shared_probe = discovery[shared_start:shared_end]
        self.assertIn("inspect_manifest_probe_evidence(", shared_probe)
        self.assertNotIn("parse_hls_manifest_quality(", shared_probe)
        self.assertNotIn("parse_dash_manifest_quality(", shared_probe)
        self.assertNotIn("inspect_hls_manifest_drm(", shared_probe)
        self.assertNotIn(
            "classify_hls_variant_probe_failure(",
            shared_probe,
        )

    def test_playback_fingerprint_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        playback=(ROOT/"recorder_source"/"playback.py").read_text(encoding="utf-8")
        self.assertIn("def playback_fingerprint(", playback)
        self.assertIn("source_playback.playback_fingerprint(", recorder)
        self.assertIn("playback_fingerprint(", discovery)
        self.assertNotIn("def _playback_fingerprint(", discovery)

    def test_header_canonicalization_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        headers=(ROOT/"recorder_source"/"headers.py").read_text(encoding="utf-8")
        self.assertIn("def canonicalize_header_name(", headers)
        self.assertIn("source_headers.canonicalize_header_name(name)", recorder)
        self.assertIn("canonicalize_header_name(", discovery)
        self.assertNotIn("def _canonical_header_name(", discovery)

    def test_discovery_expiry_merge_delegates_to_shared_quality_rule(self):
        discovery=(ROOT/"recorder_source"/"discovery.py").read_text(encoding="utf-8")
        quality=(ROOT/"recorder_source"/"quality.py").read_text(encoding="utf-8")
        self.assertIn("def merge_auth_expiries(", quality)
        self.assertIn("merged = merge_auth_expiries(*values)", discovery)
        self.assertNotIn("return min(known) if known else None", discovery)

    def test_mature_recorder_consumes_shared_policy(self):
        text=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        self.assertIn("from recorder_source.policy import",text)
        self.assertIn("NM3U8DL_PLAYLIST_GROUP_PROFILES = dict(SHARED_PLAYLIST_GROUP_PROFILES)",text)
        self.assertIn("NM3U8DL_PLAYLIST_GROUP_MATCH_MODES = dict(SHARED_PLAYLIST_GROUP_MATCH_MODES)",text)
        self.assertIn("NM3U8DL_PLAYLIST_GROUP_LIFECYCLES = dict(SHARED_PLAYLIST_GROUP_LIFECYCLES)",text)
        self.assertIn("NM3U8DL_PLAYLIST_USER_AGENTS = dict(SHARED_PLAYLIST_USER_AGENTS)",text)
        self.assertIn("shared_selection_policy_for_provider(",text)
        self.assertNotIn("SHARED_PROVIDER_SELECTION_POLICIES",text)

    def test_selection_reasoning_has_one_shared_owner(self):
        recorder=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        terminal=(ROOT/"recorder_coordinator"/"terminal.py").read_text(encoding="utf-8")
        selection=(ROOT/"recorder_source"/"selection.py").read_text(encoding="utf-8")
        launch=(ROOT/"recorder_coordinator"/"launch.py").read_text(encoding="utf-8")
        snapshot=(ROOT/"recorder_coordinator"/"snapshot.py").read_text(encoding="utf-8")

        self.assertIn("def selection_nonselection_reason(",selection)
        self.assertIn("source_selection.selection_nonselection_reason(",recorder)
        self.assertNotIn("selection_nonselection_reason(",terminal)
        self.assertIn("runtime_status",terminal)
        self.assertNotIn('return "not selected: lower quality"',recorder)

        self.assertIn("selection_policy_for_provider(",launch)
        self.assertIn("selection_policy_for_provider(",snapshot)


if __name__ == "__main__":
    unittest.main(verbosity=2)
