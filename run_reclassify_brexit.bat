@echo off
REM ===================================================================
REM Brexit-Reklassifizierung mit Gemini 2.5-Flash
REM ===================================================================
REM
REM Dieses Script analysiert alle Reden in debates_brexit_filtered_min20words.duckdb
REM mit Gemini 2.5-Flash auf Brexit-Bezug und erstellt eine neue Datenbank
REM mit nur Brexit-relevanten Reden.
REM
REM Voraussetzungen:
REM - GEMINI_API_KEY Umgebungsvariable gesetzt
REM - debates_brexit_filtered_min20words.duckdb vorhanden
REM
REM ===================================================================

echo.
echo ====================================================================
echo Brexit-Reklassifizierung mit Gemini 2.5-Flash
echo ====================================================================
echo.

REM Prüfe ob API Key gesetzt ist
if "%GEMINI_API_KEY%"=="" (
    echo FEHLER: GEMINI_API_KEY Umgebungsvariable nicht gesetzt!
    echo.
    echo Setze den API Key mit:
    echo   $env:GEMINI_API_KEY = "your-api-key-here"
    echo.
    echo Oder erstelle eine .env Datei mit:
    echo   GEMINI_API_KEY=your-api-key-here
    echo.
    pause
    exit /b 1
)

echo [OK] GEMINI_API_KEY gefunden
echo.

REM Prüfe ob Quelldatenbank existiert
if not exist "debates_brexit_filtered_min20words.duckdb" (
    echo FEHLER: debates_brexit_filtered_min20words.duckdb nicht gefunden!
    echo Stelle sicher, dass die Datenbank im Hauptverzeichnis vorhanden ist.
    echo.
    pause
    exit /b 1
)

echo [OK] Quelldatenbank gefunden
echo.

REM Führe Python-Script aus
echo Starte Reklassifizierung...
echo.
python scripts\reclassify_brexit_gemini.py

REM Prüfe Exit-Code
if %ERRORLEVEL% NEQ 0 (
    echo.
    echo FEHLER: Script ist mit Fehler beendet!
    echo Exit Code: %ERRORLEVEL%
    echo.
    pause
    exit /b %ERRORLEVEL%
)

echo.
echo ====================================================================
echo Reklassifizierung abgeschlossen!
echo ====================================================================
echo.
echo Ergebnisse gespeichert in: debates_brexit_gemini_classified.duckdb
echo.

pause

