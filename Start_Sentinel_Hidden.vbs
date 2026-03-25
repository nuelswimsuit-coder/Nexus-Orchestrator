' Launch Sentinel LAN auto-deployer in the background (no console).
' Requires pythonw.exe on PATH.

Option Explicit
Dim sh, fso, root, pyExe, scriptPy, cmd

Set sh = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
root = fso.GetParentFolderName(WScript.ScriptFullName)
pyExe = "pythonw.exe"
scriptPy = root & "\scripts\sentinel_service.py"
cmd = """" & pyExe & """ """ & scriptPy & """ autodeploy"

sh.CurrentDirectory = root
' 0 = hidden window; False = do not wait for exit
sh.Run cmd, 0, False

Set sh = Nothing
Set fso = Nothing
