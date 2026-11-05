@echo off
rem doble clic para correr todo; los argumentos pasan al .ps1 (por ejemplo: ejecutar_todo.bat -SinIngesta)
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0ejecutar_todo.ps1" %*
pause
