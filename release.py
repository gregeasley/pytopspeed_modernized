#!/usr/bin/env python3
"""
Release script for pytopspeed-modernized package

This script automates the release process for publishing to PyPI.
Author: Greg Easley <greg@easley.dev>

Usage: python release.py [testpypi|pypi]
"""

import re
import subprocess
import sys
import tempfile
from pathlib import Path

PACKAGE_FILE_PREFIX = 'pytopspeed_modernized'

# Every place the version is declared; they must all agree before releasing
VERSION_FILES = {
    'setup.py': r'version\s*=\s*["\']([^"\']+)["\']',
    'setup.cfg': r'^version\s*=\s*(\S+)',
    'pyproject.toml': r'^version\s*=\s*["\']([^"\']+)["\']',
    'src/pytopspeed/__init__.py': r'__version__\s*=\s*["\']([^"\']+)["\']',
    'src/cli/__init__.py': r'__version__\s*=\s*["\']([^"\']+)["\']',
}


def get_versions():
    """Return {file: version} for every file that declares the version"""
    versions = {}
    for path, pattern in VERSION_FILES.items():
        file = Path(path)
        if file.exists():
            match = re.search(pattern, file.read_text(encoding='utf-8'), re.MULTILINE)
            versions[path] = match.group(1) if match else None
    return versions


def get_version():
    """Get current version from setup.py (falls back to pyproject.toml)"""
    versions = get_versions()
    return versions.get('setup.py') or versions.get('pyproject.toml') or "unknown"


def check_versions():
    """Make sure every version declaration matches"""
    print("Checking version declarations...")
    versions = get_versions()
    if len(set(versions.values())) != 1 or None in versions.values():
        print("ERROR: Version declarations disagree:")
        for path, version in versions.items():
            print(f"   {path}: {version}")
        return False
    print(f"SUCCESS: All {len(versions)} version declarations are {get_version()}")
    return True


def run_command(command, description, interactive=False, cwd=None):
    """
    Run a command (list of arguments) and handle errors.

    Interactive commands (e.g. twine prompting for credentials) are not captured,
    so their prompts and output reach the terminal.
    """
    print(f"Running: {description}...")
    try:
        if interactive:
            subprocess.run(command, check=True, cwd=cwd)
        else:
            result = subprocess.run(command, check=True, capture_output=True, text=True, cwd=cwd)
            if result.stdout:
                print(f"   Output: {result.stdout.strip()}")
        print(f"SUCCESS: {description} completed successfully")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {description} failed")
        if e.stderr:
            print(f"   Error: {e.stderr.strip()}")
        if e.stdout:
            print(f"   Output: {e.stdout.strip()}")
        return False
    except FileNotFoundError as e:
        print(f"ERROR: {description} failed: {e}")
        return False


def python(*args):
    """Command line that runs this same Python interpreter (python may not be on PATH)"""
    return [sys.executable, *args]


def check_tools():
    """Make sure the build and twine packages are installed"""
    print("Checking release tools...")
    # Look them up from outside the repo: its build/ folder would otherwise shadow the build package
    result = subprocess.run(python("-c", "import build, twine"), cwd=tempfile.gettempdir(),
                            capture_output=True, text=True)
    if result.returncode != 0:
        print("ERROR: The 'build' and 'twine' packages are required:")
        print(f"   {sys.executable} -m pip install build twine")
        return False
    print("SUCCESS: build and twine are installed")
    return True


def release_files():
    """The built distribution files for the current version"""
    return sorted(str(p) for p in Path('dist').glob(f'{PACKAGE_FILE_PREFIX}-{get_version()}[-.]*'))


def check_git_status():
    """Check git status"""
    print("Checking git status...")

    # Check if we're in a git repository
    if not run_command(["git", "status"], "Checking git repository"):
        return False

    # Check for uncommitted changes
    result = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True)
    if result.stdout.strip():
        print("WARNING: You have uncommitted changes:")
        print(result.stdout)
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return False

    return True


def run_tests():
    """Run all tests"""
    print("Running tests...")

    # Run unit tests
    if not run_command(python("-m", "pytest", "tests/unit/", "-q"), "Running unit tests"):
        print("ERROR: Unit tests failed!")
        return False

    # Run integration tests
    if not run_command(python("-m", "pytest", "tests/integration/", "-q"), "Running integration tests"):
        print("WARNING: Integration tests failed, but continuing...")

    return True


def build_package():
    """Build the sdist and wheel for the current version"""
    print("Building package...")

    # Remove any earlier build of this version so only fresh files are uploaded
    for old in release_files():
        Path(old).unlink()

    # Run from outside the repo so its build/ folder can't shadow the build package
    root = Path(__file__).resolve().parent
    command = python("-m", "build", str(root), "--outdir", str(root / "dist"))
    if not run_command(command, "Building package", cwd=tempfile.gettempdir()):
        return False

    files = release_files()
    if not files:
        print(f"ERROR: No files for version {get_version()} found in dist/")
        return False
    print("   Built: " + ", ".join(files))
    return True


def check_package():
    """Check the built package"""
    print("Checking package...")

    # Check with twine
    if not run_command(python("-m", "twine", "check", *release_files()), "Checking package with twine"):
        return False

    return True


def upload_to_testpypi():
    """Upload to TestPyPI"""
    print("Uploading to TestPyPI...")

    if not run_command(python("-m", "twine", "upload", "--repository", "testpypi", *release_files()),
                       "Uploading to TestPyPI", interactive=True):
        return False

    print("SUCCESS: Package uploaded to TestPyPI!")
    print("Test installation: pip install --index-url https://test.pypi.org/simple/ pytopspeed-modernized")
    return True


def upload_to_pypi():
    """Upload to PyPI"""
    print("Uploading to PyPI...")

    if not run_command(python("-m", "twine", "upload", *release_files()), "Uploading to PyPI", interactive=True):
        return False

    print("SUCCESS: Package uploaded to PyPI!")
    print("Installation: pip install pytopspeed-modernized")
    return True


def create_git_tag():
    """Create and push the git tag for this release"""
    version = get_version()
    tag = f"v{version}"
    print(f"Creating git tag {tag}...")

    if not run_command(["git", "tag", "-a", tag, "-m", f"Release version {version}"], f"Creating tag {tag}"):
        return False

    if not run_command(["git", "push", "origin", tag], f"Pushing tag {tag} to remote"):
        return False

    return True


def main():
    """Main release function"""
    version = get_version()
    print(f"Starting release process for version {version}")

    # Check arguments
    if len(sys.argv) > 1:
        target = sys.argv[1].lower()
    else:
        print("Usage: python release.py [testpypi|pypi]")
        print("  testpypi: Upload to TestPyPI for testing")
        print("  pypi: Upload to PyPI for production")
        sys.exit(1)

    if target not in ['testpypi', 'pypi']:
        print("ERROR: Invalid target. Use 'testpypi' or 'pypi'")
        sys.exit(1)

    # Pre-release checks
    if not check_tools() or not check_versions():
        sys.exit(1)

    if not check_git_status():
        print("ERROR: Git status check failed")
        sys.exit(1)

    if not run_tests():
        print("ERROR: Tests failed")
        sys.exit(1)

    if not build_package():
        print("ERROR: Package build failed")
        sys.exit(1)

    if not check_package():
        print("ERROR: Package check failed")
        sys.exit(1)

    # Upload to target
    if target == 'testpypi':
        if not upload_to_testpypi():
            print("ERROR: TestPyPI upload failed")
            sys.exit(1)
    else:  # pypi
        if not upload_to_pypi():
            print("ERROR: PyPI upload failed")
            sys.exit(1)

        # Create git tag for production releases
        if not create_git_tag():
            print("WARNING: Git tag creation failed, but release was successful")

    print(f"\nRelease {version} completed successfully!")
    print("Package available at: https://pypi.org/project/pytopspeed-modernized/")


if __name__ == "__main__":
    main()
