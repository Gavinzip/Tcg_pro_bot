#!/usr/bin/env python3
"""Install only pinned, checksum-verified Linux/amd64 backup binaries."""
import bz2
import hashlib
import io
from pathlib import Path
import platform
import sys
import urllib.request
import zipfile

ARTIFACTS = {
 'restic': ('https://github.com/restic/restic/releases/download/v0.19.1/restic_0.19.1_linux_amd64.bz2',
            'f415415624dcc452f2a02b8c33641791a8c6d6d3b65bbb3543fcf9a25151585c'),
 'rclone': ('https://downloads.rclone.org/v1.74.2/rclone-v1.74.2-linux-amd64.zip',
            '72a806370072015ccbe4d81bcd348cc5eaf3beca6c65ba693fd43fb31fcca5b1'),
}

def main():
    if platform.system() != 'Linux' or platform.machine() not in {'x86_64', 'amd64'}:
        raise SystemExit('backup_tools_require_linux_amd64')
    root = Path(sys.argv[1] if len(sys.argv) > 1 else '/usr/local/bin')
    root.mkdir(parents=True, exist_ok=True)
    for name, (url, expected) in ARTIFACTS.items():
        request = urllib.request.Request(url, headers={'User-Agent': 'TCG-Pro-Backup/1.0'})
        with urllib.request.urlopen(request, timeout=120) as response:
            archive = response.read()
        if hashlib.sha256(archive).hexdigest() != expected:
            raise SystemExit('backup_tool_checksum_mismatch')
        if name == 'restic':
            executable = bz2.decompress(archive)
        else:
            with zipfile.ZipFile(io.BytesIO(archive)) as zipped:
                executable = zipped.read('rclone-v1.74.2-linux-amd64/rclone')
        path = root / name
        path.write_bytes(executable)
        path.chmod(0o755)
        print(name + '_checksum_verified')

if __name__ == '__main__':
    main()
