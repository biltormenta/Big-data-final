@echo off
rem doble clic para correr la entrega 2; los argumentos pasan al .ps1 (por ejemplo: ejecutar_entrega2.bat -SegPorMes 3)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ejecutar_entrega2.ps1" %*
pause
