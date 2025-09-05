#!/usr/bin/env python3
"""
Build script for pytopspeed-modernized package

This script automates the build process for creating distribution packages.
Author: Greg Easley <greg@easley.dev>
"""

import os
import sys
import subprocess
import shutil
from pathlib import Path


def run_command(command, description):
    """Run a command and handle errors"""
    print(f"Running: {description}...")
    try:
        # Use the same Python executable as the current script
        python_exe = sys.executable
        if command.startswith("python "):
            command = command.replace("python ", f"{python_exe} ", 1)
        elif command.startswith("pip "):
            command = f"{python_exe} -m pip " + command[4:]
        
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        print(f"SUCCESS: {description} completed successfully")
        if result.stdout:
            print(f"   Output: {result.stdout.strip()}")
        return True
    except subprocess.CalledProcessError as e:
        print(f"ERROR: {description} failed")
        print(f"   Error: {e.stderr.strip()}")
        return False


def clean_build_dirs():
    """Clean build directories"""
    dirs_to_clean = ['build', 'dist', '*.egg-info']
    for pattern in dirs_to_clean:
        for path in Path('.').glob(pattern):
            if path.is_dir():
                print(f"🧹 Removing {path}")
                shutil.rmtree(path)


def build_package():
    """Build the package"""
    print("Starting package build process...")
    
    # Clean previous builds
    clean_build_dirs()
    
    # Check if we're in the right directory
    if not Path('setup.py').exists():
        print("❌ setup.py not found. Please run this script from the project root.")
        return False
    
    # Install build dependencies
    if not run_command("python -m pip install --upgrade build wheel setuptools", "Installing build dependencies"):
        return False
    
    # Run tests first
    if not run_command("python -m pytest tests/unit/ -v --tb=short", "Running unit tests"):
        print("⚠️  Tests failed, but continuing with build...")
    
    # Build source distribution
    if not run_command("python -m build --sdist", "Building source distribution"):
        return False
    
    # Build wheel distribution
    if not run_command("python -m build --wheel", "Building wheel distribution"):
        return False
    
    # Check the built packages (using twine check instead)
    if not run_command("python -m twine check dist/*", "Checking built packages"):
        return False
    
    print("Package build completed successfully!")
    print("\nBuilt packages:")
    for file in Path('dist').glob('*'):
        print(f"   - {file}")
    
    return True




def main():
    """Main build function"""
    # Always build package (no other options needed)
    success = build_package()
    
    if success:
        print("\nBuild completed successfully!")
        print("\nNext steps:")
        print("1. Test the package: pip install dist/*.whl")
        print("2. Upload to PyPI: python -m twine upload dist/*")
    else:
        print("\nBuild failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
