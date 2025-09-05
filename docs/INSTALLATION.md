# Installation Guide

This guide provides detailed installation instructions for the Pytopspeed Modernized library.

## 📋 Prerequisites

### System Requirements

- **Operating System**: Windows, macOS, or Linux
- **Python**: Version 3.8 or higher (3.11 recommended)
- **Memory**: At least 4GB RAM (8GB+ recommended for large databases)
- **Storage**: Sufficient disk space for input files and output databases

### Required Software

- **Python 3.8+**: [Download from python.org](https://www.python.org/downloads/)
- **Conda** (recommended): [Download Anaconda or Miniconda](https://docs.conda.io/en/latest/miniconda.html)

## 🚀 Installation Methods

### Method 1: Conda Environment (Recommended)

This is the recommended installation method as it provides better dependency management.

#### Step 1: Clone the Repository

```bash
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
```

#### Step 2: Create Conda Environment

```bash
# Create a new conda environment with Python 3.11
conda create -n pytopspeed_modernized python=3.11

# Activate the environment
conda activate pytopspeed_modernized
```

#### Step 3: Install Dependencies

```bash
# Install required packages
pip install -r requirements.txt
```

#### Step 4: Verify Installation

```bash
# Test the CLI
python pytopspeed.py --help

# Run unit tests
python -m pytest tests/unit/ -v
```

### Method 2: Virtual Environment

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

#### Step 4: Verify Installation

```bash
# Test the CLI
python pytopspeed.py --help
```

### Method 3: System-wide Installation

**Note**: Not recommended for development, but suitable for production use.

#### Step 1: Clone the Repository

```bash
git clone https://github.com/gregeasley/pytopspeed_modernized
cd pytopspeed_modernized
```

#### Step 2: Install Dependencies

```bash
# Install required packages
pip install -r requirements.txt
```

#### Step 3: Install the Package (Optional)

```bash
# Install in development mode
pip install -e .

# Or install normally
pip install .
```

## 📦 Dependencies

The following packages are automatically installed with the requirements:

| Package | Version | Purpose |
|---------|---------|---------|
| `construct` | >=2.10 | Binary data parsing |
| `pytest` | >=7.0.0 | Testing framework |
| `click` | >=8.0.0 | CLI framework |
| `pandas` | >=1.5.0 | Data manipulation |
| `psutil` | >=5.9.0 | System monitoring |

### Optional Dependencies

These packages are not required but may be useful:

- **Jupyter Notebook**: For interactive development
- **matplotlib**: For data visualization
- **sqlite3**: Built into Python (no installation needed)

## 🔧 Configuration

### Environment Variables

You can set these optional environment variables:

```bash
# Set default batch size for conversions
export PYTOPSPEED_BATCH_SIZE=1000

# Set default log level
export PYTOPSPEED_LOG_LEVEL=INFO

# Set temporary directory for PHZ extraction
export PYTOPSPEED_TEMP_DIR=/tmp
```

### Configuration File

Create a `config.ini` file in the project root for persistent settings:

```ini
[converter]
batch_size = 1000
progress_callback = true
verbose_logging = false

[paths]
temp_directory = /tmp
output_directory = ./output
```

## 🧪 Testing the Installation

### Run Unit Tests

```bash
# Run all unit tests
python -m pytest tests/unit/ -v

# Run specific test categories
python -m pytest tests/unit/test_tps_parser.py -v
python -m pytest tests/unit/test_sqlite_converter.py -v
```

### Run Integration Tests

```bash
# Run integration tests (requires sample data)
python -m pytest tests/integration/ -v
```

### Test CLI Functionality

```bash
# Test help system
python pytopspeed.py --help
python pytopspeed.py convert --help
python pytopspeed.py reverse --help
python pytopspeed.py list --help

# Test with sample data (if available)
python pytopspeed.py list assets/TxWells.phz
```

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

**3. "Permission denied" errors**
- Use `--user` flag: `pip install --user -r requirements.txt`
- Run as administrator (Windows) or use `sudo` (Linux/macOS)
- Use a virtual environment instead

**4. "Package not found" errors**
- Update pip: `python -m pip install --upgrade pip`
- Check internet connection
- Try using a different package index: `pip install -i https://pypi.org/simple/ -r requirements.txt`

**5. "ImportError" after installation**
- Verify the correct Python environment is activated
- Check that packages were installed in the right environment
- Try reinstalling: `pip uninstall <package> && pip install <package>`

### Platform-Specific Issues

#### Windows

**Issue**: "Microsoft Visual C++ 14.0 is required"
- Install Microsoft Visual C++ Build Tools
- Or install Visual Studio Community with C++ tools

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
# Pull latest changes
git pull origin main

# Reinstall if needed
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
2. **Verify prerequisites**: Ensure Python and conda are properly installed
3. **Try different methods**: If one installation method fails, try another
4. **Check compatibility**: Ensure your Python version is supported
5. **Search issues**: Look for similar problems in the project repository

For additional help, please open an issue in the project repository with:
- Your operating system and version
- Python version (`python --version`)
- Complete error message
- Steps you've already tried
