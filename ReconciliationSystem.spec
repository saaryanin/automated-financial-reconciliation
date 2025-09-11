# -*- mode: python ; coding: utf-8 -*-

a = Analysis(
    ['main.py'],
    pathex=['.', 'src', 'frontend'],
    binaries=[],
    datas=[
        ('data', 'data'),
        ('frontend/calendar_icon.png', '.'),  # Keep this to copy to temp root
    ],
    hiddenimports=[
        'PyQt5.sip',
        'src',
        'src.config',
        'src.processor_renamer',
        'src.reports_creator',
        'src.deposits_matcher',
        'src.withdrawals_matcher',
        'src.preprocess',
        'src.utils',
        'src.shifts_handler',
        'src.output',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    noarchive=False,
    optimize=0,
)
pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='ReconciliationSystem',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Change to True for debugging (shows console with prints)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)