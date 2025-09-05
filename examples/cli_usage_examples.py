#!/usr/bin/env python3
"""
CLI Usage Examples for pytopspeed modernized library

This script demonstrates various ways to use the command-line interface
for converting TopSpeed database files to SQLite and back.
"""

import subprocess
import sys
from pathlib import Path

def run_command(cmd, description):
    """Run a command and display the result"""
    print(f"\n{'='*60}")
    print(f"Example: {description}")
    print(f"Command: {' '.join(cmd)}")
    print(f"{'='*60}")
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
        print("STDOUT:")
        print(result.stdout)
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
        print(f"Exit code: {result.returncode}")
    except subprocess.TimeoutExpired:
        print("Command timed out after 30 seconds")
    except Exception as e:
        print(f"Error running command: {e}")

def main():
    """Demonstrate CLI usage examples"""
    print("Pytopspeed CLI Usage Examples")
    print("=" * 60)
    
    # Get the path to the CLI script
    cli_script = Path(__file__).parent.parent / "pytopspeed.py"
    
    if not cli_script.exists():
        print(f"Error: CLI script not found at {cli_script}")
        return 1
    
    # Example 1: Show help
    run_command([sys.executable, str(cli_script), "--help"], 
                "Show main help")
    
    # Example 2: Show convert command help
    run_command([sys.executable, str(cli_script), "convert", "--help"], 
                "Show convert command help")
    
    # Example 3: Show reverse command help
    run_command([sys.executable, str(cli_script), "reverse", "--help"], 
                "Show reverse command help")
    
    # Example 4: Show list command help
    run_command([sys.executable, str(cli_script), "list", "--help"], 
                "Show list command help")
    
    # Example 5: List PHZ file contents (if available)
    phz_file = Path(__file__).parent.parent / "assets" / "TxWells.phz"
    if phz_file.exists():
        run_command([sys.executable, str(cli_script), "list", str(phz_file)], 
                    "List contents of PHZ file")
    else:
        print(f"\nNote: PHZ file not found at {phz_file}, skipping PHZ example")
    
    print(f"\n{'='*60}")
    print("CLI Usage Examples Complete")
    print("=" * 60)
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
