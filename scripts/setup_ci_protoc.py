#!/usr/bin/env python3
"""Provision official protoc release assets for clean GitHub-hosted runners."""
import io
import os
from pathlib import Path
import platform
import subprocess
import urllib.request
import zipfile

VERSION = "30.2"
ASSETS = {
    "Linux": "linux-x86_64",
    "Darwin": "osx-universal_binary",
    "Windows": "win64",
}


def main():
    target = ASSETS[platform.system()]
    root = Path(os.environ["RUNNER_TEMP"]) / f"kamish-protoc-{VERSION}"
    url = f"https://github.com/protocolbuffers/protobuf/releases/download/v{VERSION}/protoc-{VERSION}-{target}.zip"
    with urllib.request.urlopen(url, timeout=60) as response:
        archive_bytes = response.read()
    with zipfile.ZipFile(io.BytesIO(archive_bytes)) as archive:
        for member in archive.infolist():
            path = Path(member.filename)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Unsafe protobuf archive entry")
        archive.extractall(root)
    compiler = root / "bin" / ("protoc.exe" if platform.system() == "Windows" else "protoc")
    compiler.chmod(0o755)
    subprocess.run([str(compiler), "--version"], check=True)
    with open(os.environ["GITHUB_ENV"], "a", encoding="utf-8") as env:
        env.write(f"PROTOC={compiler}\nPROTOC_INCLUDE={root / 'include'}\n")


if __name__ == "__main__":
    main()
