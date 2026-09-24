@echo off
setlocal
cd /d "%~dp0"

python -m unittest -v checkpoint0_tests/test_recorder_safety_baseline.py
if errorlevel 1 goto :fail

python -m unittest discover -s tests -v
if errorlevel 1 goto :fail

echo.
echo ALL RECORDER TESTS: PASS ^(99 tests^)
exit /b 0

:fail
echo.
echo ALL RECORDER TESTS: FAIL
exit /b 1
