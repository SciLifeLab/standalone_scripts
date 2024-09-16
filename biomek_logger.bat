@ECHO OFF
SETLOCAL
SET LOG_PATH="C:\Users\Public\Documents\Biomek5\Logs"
SET SCRIPT="%USERPROFILE%\Repos\standalone_scripts\upload_biomek_logs.py"
SET STB_CONF="%USERPROFILE%\Repos\conf\biomek_upload_conf.yaml"
SET STB_LOG="%USERPROFILE%\Repos\logs\statusdb_upload.log"
ECHO %* | C:\Users\Public\miniconda3\envs\biomek_log\python %SCRIPT% --conf %STB_CONF% --logfile %STB_LOG% --log_file_path %LOG_PATH%