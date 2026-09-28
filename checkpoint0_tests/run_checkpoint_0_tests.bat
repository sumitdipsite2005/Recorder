@echo off
setlocal
cd /d "%~dp0"
python -m unittest -v test_recorder_safety_baseline.py
set TEST_EXIT=%ERRORLEVEL%
echo.
if %TEST_EXIT% EQU 0 (
    echo CHECKPOINT 0 TESTS: PASS
) else (
    echo CHECKPOINT 0 TESTS: FAIL
)
pause
exit /b %TEST_EXIT%
