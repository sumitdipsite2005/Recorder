from __future__ import annotations

import unittest

from recorder_runtime.sound import (
    SoundSnoozeState,
    clear_sound_snooze,
    is_sound_snoozed,
    set_indefinite_sound_snooze,
    set_timed_sound_snooze,
    sound_snooze_remaining_seconds,
)


class SoundSnoozeTests(unittest.TestCase):
    def test_timed_snooze_expires_and_clears_state(self):
        state=SoundSnoozeState()
        set_timed_sound_snooze(state,duration_sec=900,now_ts=1000)
        self.assertTrue(
            is_sound_snoozed(state,now_ts=1100,indefinite_modes=("session",))
        )
        self.assertEqual(sound_snooze_remaining_seconds(state,now_ts=1100),800)
        self.assertFalse(
            is_sound_snoozed(state,now_ts=1900,indefinite_modes=("session",))
        )
        self.assertIsNone(state.sound_snooze_mode)

    def test_indefinite_scope_is_caller_defined_and_can_be_restored(self):
        state=SoundSnoozeState()
        set_indefinite_sound_snooze(state,"coordinator_run")
        self.assertTrue(
            is_sound_snoozed(
                state,
                now_ts=1000,
                indefinite_modes=("coordinator_run",),
            )
        )
        clear_sound_snooze(state)
        self.assertFalse(
            is_sound_snoozed(
                state,
                now_ts=1000,
                indefinite_modes=("coordinator_run",),
            )
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
