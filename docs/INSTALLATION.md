# Installation Guide

This guide provides detailed installation instructions for the Pytopspeed Modernized library.

## 📋 Prerequisites

### System Requirements

- **Operating System**: Windows, macOS, or Linux
- **Python**: Version 3.8 to 3.12 (the versions tested in CI)
- **Memory**: The converter keeps the source file's records in memory while converting, so larger files need more RAM
- **Storage**: Room for the input files and the output database. The SQLite database is several times larger than the TopSpeed file, because it also keeps the original record bytes for reverse conversion.

### Required Software

- **Python 3.8+**: [Download from python.org](https://www.python.org/downloads/)
- **Conda** (optional): [Download Anaconda or Miniconda](https://docs.conda.io/en/latest/miniconda.html)

## 🚀 Installation Methods

### Method 1: Install from PyPI (Recommended)

This is the easiest and recommended installation method for most users.

```bash
# Install directly from PyPI
pip install pytopspeed-modernized
```

This installs the `pytopspeed` command.

### Method 2: Install from Source

This method is recommended for developers who want to contribute or modify the code.

#### Step 1: Clone the Repository

```bash
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
```

#### Step 2: Create Conda Environment (Optional)

```bash
# Create a new conda environment with Python 3.11
conda create -n pytopspeed_modernized python=3.11

# Activate the environment
conda activate pytopspeed_modernized
```

#### Step 3: Install in Development Mode

```bash
# Install in development mode
pip install -e .

# Or include the development tools (pytest, pytest-cov, black, flake8, mypy)
pip install -e .[dev]
```

#### Step 4: Verify Installation

```bash
# Test the CLI
pytopspeed --help
pytopspeed --version

# Run the tests (needs the [dev] extras or requirements.txt)
python -m pytest tests/unit tests/integration
```

### Method 3: Virtual Environment

If you prefer using Python's built-in virtual environment:

#### Step 1: Clone the Repository

```bash
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
```

#### Step 2: Create Virtual Environment

```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate
# On macOS/Linux:
source venv/bin/activate
```

#### Step 3: Install Dependencies

```bash
# Upgrade pip
python -m pip install --upgrade pip

# Install required packages
pip install -r requirements.txt
```

`requirements.txt` installs the dependencies only, not the package, so there is no `pytopspeed` command. Run `pip install -e .` as well if you want it.

#### Step 4: Verify Installation

```bash
# Test the CLI from the checkout
python src/cli/main.py --help
```

### Method 4: System-wide Installation

**Note**: Not recommended for development.

#### Step 1: Clone the Repository

```bash
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
```

#### Step 2: Install the Package

```bash
# Install normally
pip install .
```

## 📦 Dependencies

Installing the package installs these runtime dependencies:

| Package | Version | Purpose |
|---------|---------|---------|
| `construct` | >=2.10 | Binary data parsing |
| `click` | >=8.0.0 | CLI framework |
| `pandas` | >=1.5.0 | Data manipulation |
| `psutil` | >=5.9.0 | System monitoring |

`requirements.txt` also lists `pytest` and `pytest-cov` for running the tests. The `[dev]` extra adds `pytest`, `pytest-cov`, `black`, `flake8` and `mypy`.

### Optional Dependencies

These packages are not required but may be useful:

- **Jupyter Notebook**: For interactive development
- **matplotlib**: For data visualization
- **sqlite3**: Built into Python (no installation needed)

## 🔧 Configuration

There are no configuration files or environment variables. Settings are passed on the command line (for example `--batch-size`, `-v`) or as constructor arguments in the Python API (`SqliteConverter(batch_size=..., progress_callback=...)`).

## 🧪 Testing the Installation

### Run Unit Tests

```bash
# Run all unit tests
python -m pytest tests/unit/ -v

# Run specific test files
python -m pytest tests/unit/test_tps_parser.py -v
python -m pytest tests/unit/test_sqlite_converter.py -v
python -m pytest tests/unit/test_reverse_converter.py -v
```

### Run Integration Tests

```bash
# Run integration tests (no sample data needed)
python -m pytest tests/integration/ -v
```

Unit and integration tests together should report 376 passed. The tests generate their TopSpeed files synthetically.

### Test CLI Functionality

```bash
# Test help system
pytopspeed --help
pytopspeed convert --help
pytopspeed reverse --help
pytopspeed list --help

# Try it on one of your own files
pytopspeed list MyProject.phz
```

From a checkout without installing, replace `pytopspeed` with `python src/cli/main.py`.

## 🐛 Troubleshooting Installation

### Common Issues

**1. "conda: command not found"**
- Install Anaconda or Miniconda
- Add conda to your PATH environment variable
- Restart your terminal/command prompt

**2. "pip: command not found"**
- Ensure Python is properly installed
- Use `python -m pip` instead of `pip`
- Check that Python is in your PATH

**3. "pytopspeed: command not found"**
- The package isn't installed in the active environment (installing only `requirements.txt` doesn't create the command); run `pip install -e .` or use `python src/cli/main.py`
- Check that the environment's `Scripts` (Windows) or `bin` directory is on your PATH

**4. "Permission denied" errors**
- Use `--user` flag: `pip install --user -r requirements.txt`
- Use a virtual environment instead

**5. "Package not found" errors**
- Update pip: `python -m pip install --upgrade pip`
- Check internet connection
- Try using a different package index: `pip install -i https://pypi.org/simple/ -r requirements.txt`

**6. "ImportError" after installation**
- Verify the correct Python environment is activated
- Check that packages were installed in the right environment
- Try reinstalling: `pip uninstall <package> && pip install <package>`

### Platform-Specific Issues

#### Windows

**Issue**: "Microsoft Visual C++ 14.0 is required"
- pip is building a dependency from source because no wheel matches your Python version; use a supported Python version, or install Microsoft Visual C++ Build Tools

**Issue**: Long path names
- Enable long path support in Windows 10/11
- Or use shorter directory names

#### macOS

**Issue**: "xcrun: error: invalid active developer path"
- Install Xcode command line tools: `xcode-select --install`

**Issue**: Permission errors with pip
- Use `--user` flag or virtual environment
- Avoid using `sudo` with pip

#### Linux

**Issue**: Missing system dependencies
- Install build essentials: `sudo apt-get install build-essential`
- Install Python development headers: `sudo apt-get install python3-dev`

## 🔄 Updating

### Update Dependencies

```bash
# Update all packages to latest versions
pip install --upgrade -r requirements.txt

# Update specific package
pip install --upgrade construct
```

### Update the Library

```bash
# From PyPI
pip install --upgrade pytopspeed-modernized

# From a source checkout: pull latest changes and reinstall if needed
git pull origin master
pip install -e .
```

## 🗑️ Uninstallation

### Remove Conda Environment

```bash
# Deactivate environment
conda deactivate

# Remove environment
conda env remove -n pytopspeed_modernized
```

### Remove Virtual Environment

```bash
# Deactivate environment
deactivate

# Remove directory
rm -rf venv  # Linux/macOS
rmdir /s venv  # Windows
```

### Remove System Installation

```bash
# Uninstall package
pip uninstall pytopspeed-modernized

# Remove dependencies (be careful!)
pip uninstall construct pytest click pandas psutil
```

## 📞 Getting Help

If you encounter issues during installation:

1. **Check the logs**: Look for error messages in the terminal output
2. **Verify prerequisites**: Ensure Python (and conda, if you use it) is properly installed
3. **Try different methods**: If one installation method fails, try another
4. **Check compatibility**: Ensure your Python version is supported
5. **Search issues**: Look for similar problems in the project repository

For additional help, please open an issue in the project repository with:
- Your operating system and version
- Python version (`python --version`)
- Complete error message
- Steps you've already tried
