# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for NexusSupreme.exe
# Build:  python -m PyInstaller NexusSupreme.spec
# Output: dist/NexusSupreme.exe

from PyInstaller.utils.hooks import collect_submodules, collect_data_files

block_cipher = None

# Collect all submodules for packages that use dynamic imports
hidden = []
hidden += collect_submodules("nexus_supreme")
hidden += collect_submodules("sqlalchemy")
hidden += collect_submodules("PyQt6")
hidden += ["requests", "httpx", "structlog", "dotenv"]

# Data files to bundle
datas = [
    ("nexus_supreme", "nexus_supreme"),
    (".env",          "."),
]
# Include icon if present
import os
if os.path.exists("nexus_icon.ico"):
    datas.append(("nexus_icon.ico", "."))

a = Analysis(
    ["Launch_NexusSupreme.py"],
    pathex       = ["."],
    binaries     = [],
    datas        = datas,
    hiddenimports= hidden,
    hookspath    = [],
    hooksconfig  = {},
    runtime_hooks= [],
    excludes     = [
        "matplotlib", "numpy", "pandas", "scipy",
        "tkinter",    "test",  "unittest",
    ],
    win_no_prefer_redirects = False,
    win_private_assemblies  = False,
    cipher      = block_cipher,
    noarchive   = False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name         = "NexusSupreme",
    debug        = False,
    bootloader_ignore_signals = False,
    strip        = False,
    upx          = True,
    upx_exclude  = [],
    runtime_tmpdir = None,
    console      = False,          # no console window
    disable_windowed_traceback = False,
    target_arch  = None,
    codesign_identity = None,
    entitlements_file = None,
    icon         = "nexus_icon.ico" if os.path.exists("nexus_icon.ico") else None,
    version      = "version_info.txt" if os.path.exists("version_info.txt") else None,
)
