@echo off
setlocal
set "PYTHON64=C:\Program Files\FTNN\app\16.2.14708\PythonEnv\Python\python.exe"
set "PYTHON32=C:\Program Files (x86)\FTNN\PythonEnv\Python\python.exe"

if exist "%PYTHON64%" (
  "%PYTHON64%" %*
  exit /b %ERRORLEVEL%
)

if exist "%PYTHON32%" (
  "%PYTHON32%" %*
  exit /b %ERRORLEVEL%
)

echo No usable Python interpreter found in the known FTNN locations. 1>&2
exit /b 1
