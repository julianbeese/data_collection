@echo off
REM Filtert kurze Reden (< 20 Wörter) aus der Datenbank

echo ======================================================================
echo Filtere kurze Reden aus debates_brexit_filtered.duckdb
echo ======================================================================
echo.

py scripts\filter_short_speeches.py

echo.
echo ======================================================================
echo Fertig! Druecke eine Taste zum Beenden...
pause

