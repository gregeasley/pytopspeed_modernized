#!/usr/bin/env python3
"""
Release script for pytopspeed-modernized package

This script automates the release process for publishing to PyPI.
Author: Greg Easley <greg@easley.dev>
"""

import os
import sys
import subprocess
import re
from pathlib import Path


def get_version():
    """Get current version from setup.py"""
    setup_py = Path('setup.py')
    if setup_py.exists():
        with open(setup_py, 'r') as f:
            content = f.read()
            match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)
    
    # Fallback to pyproject.toml
    pyproject_toml = Path('pyproject.toml')
    if pyproject_toml.exists():
        with open(pyproject_toml, 'r') as f:
            content = f.read()
            match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
            if match:
                return match.group(1)
    
    return "unknown"


def run_command(command, description):
    """Run a command and handle errors"""
    print(f"Running: {description}...")
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"SUCCESS: {description} completed successfully")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {description} failed")
        print(f"   Error: {e.stderr.strip()}")
        return False


def check_git_status():
    """Check git status"""
    print("Checking git status...")
    
    # Check if we're in a git repository
    if not run_command("git status", "Checking git repository"):
        return False
    
    # Check for uncommitted changes
    result = subprocess.run("git status --porcelain", shell=True, capture_output=True, text=True)
    if result.stdout.strip():
        print("⚠️  Warning: You have uncommitted changes:")
        print(result.stdout)
        response = input("Continue anyway? (y/N): ")
        if response.lower() != 'y':
            return False
    
    return True


def run_tests():
    """Run all tests"""
    print("Running tests...")
    
    # Run unit tests
    if not run_command("python -m pytest tests/unit/ -v", "Running unit tests"):
        print("ERROR: Unit tests failed!")
        return False
    
    # Run integration tests
    if not run_command("python -m pytest tests/integration/ -v", "Running integration tests"):
        print("WARNING: Integration tests failed, but continuing...")
    
    return True


def build_package():
    """Build the package"""
    print("Building package...")
    
    # Clean previous builds
    if not run_command("python build.py package", "Building package"):
        return False
    
    return True


def check_package():
    """Check the built package"""
    print("Checking package...")
    
    # Check with twine
    if not run_command("python -m twine check dist/*", "Checking package with twine"):
        return False
    
    return True


def upload_to_testpypi():
    """Upload to TestPyPI"""
    print("Uploading to TestPyPI...")
    
    if not run_command("python -m twine upload --repository testpypi dist/*", "Uploading to TestPyPI"):
        return False
    
    print("SUCCESS: Package uploaded to TestPyPI!")
    print("Test installation: pip install --index-url https://test.pypi.org/simple/ pytopspeed-modernized")
    return True


def upload_to_pypi():
    """Upload to PyPI"""
    print("Uploading to PyPI...")
    
    if not run_command("python -m twine upload dist/*", "Uploading to PyPI"):
        return False
    
    print("SUCCESS: Package uploaded to PyPI!")
    print("Installation: pip install pytopspeed-modernized")
    return True


def create_git_tag():
    """Create git tag for release"""
    version = get_version()
    print(f"Creating git tag v{version}...")
    
    if not run_command(f"git tag -a v{version} -m 'Release version {version}'", f"Creating tag v{version}"):
        return False
    
    if not run_command("git push origin --tags", "Pushing tags to remote"):
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
    print(f"Package available at: https://pypi.org/project/pytopspeed-modernized/")


if __name__ == "__main__":
    main()
