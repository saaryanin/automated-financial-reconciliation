# -*- mode: python ; coding: utf-8 -*-

block_cipher = None

a = Analysis(
    ['main.py'],
    pathex=['.', 'src', 'frontend'],
    binaries=[],
    datas=[
        ('data', 'data'),
        ('frontend/calendar_icon.png', '.'),
    ],
    hiddenimports=[
        'PyQt5.sip',
        'src',
        'src.config',
        'src.files_renamer',
        'src.preprocess_test',  # NEW: For reports_creator.py's process_files_in_parallel/combine_processed_files
        'src.preprocess',       # Keep for any legacy/aliases
        'src.withdrawals_matcher',
        'src.deposits_matcher',
        'src.reports_creator',
        'src.output',
        'src.utils',
        'src.shifts_handler',
        'src.processor_renamer',
        'tempfile',             # For config's gettempdir() in frozen mode
        'shutil',               # For cleanup in reports_creator/output
        'pathlib',              # Explicit for Path usage in output.py cleanup
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)
pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='ReconciliationSystem',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,  # Keep for debug logs (cleanup prints, reports_creator scans, etc.)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
)