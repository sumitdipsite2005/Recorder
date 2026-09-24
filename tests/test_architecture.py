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

    def test_entrypoint_file_names_follow_python_convention(self):
        for path in ROOT.glob("*.py"):
            self.assertEqual(path.name, path.name.lower(), path.name)
        for path in ROOT.glob("*.bat"):
            self.assertEqual(path.name, path.name.lower(), path.name)

    def test_mature_recorder_consumes_shared_policy(self):
        text=(ROOT/"record_dynamic.py").read_text(encoding="utf-8")
        self.assertIn("from recorder_source.policy import",text)
        self.assertIn("NM3U8DL_PLAYLIST_GROUP_PROFILES = dict(SHARED_PLAYLIST_GROUP_PROFILES)",text)
        self.assertIn("NM3U8DL_PLAYLIST_GROUP_MATCH_MODES = dict(SHARED_PLAYLIST_GROUP_MATCH_MODES)",text)
        self.assertIn("NM3U8DL_PLAYLIST_USER_AGENTS = dict(SHARED_PLAYLIST_USER_AGENTS)",text)


if __name__ == "__main__":
    unittest.main(verbosity=2)
