#!/usr/bin/env python3
"""
Setup script for pytopspeed modernized library

This script installs the pytopspeed modernized library and makes the CLI
accessible from the command line.
"""

from setuptools import setup, find_packages
import os

# Read the README file for long description
def read_readme():
    readme_path = os.path.join(os.path.dirname(__file__), 'README.md')
    if os.path.exists(readme_path):
        with open(readme_path, 'r', encoding='utf-8') as f:
            return f.read()
    return "Modernized pytopspeed library for converting TopSpeed database files to SQLite"

# Read requirements
def read_requirements():
    requirements_path = os.path.join(os.path.dirname(__file__), 'requirements.txt')
    if os.path.exists(requirements_path):
        with open(requirements_path, 'r', encoding='utf-8') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return [
        'construct>=2.10',
        'pytest>=7.0.0',
        'click>=8.0.0',
        'pandas>=1.5.0',
        'psutil>=5.9.0'
    ]

setup(
    name="pytopspeed-modernized",
    version="1.0.1",
    author="Greg Easley",
    author_email="greg@easley.dev",
    description="Modernized pytopspeed library for converting TopSpeed database files to SQLite",
    long_description=read_readme(),
    long_description_content_type="text/markdown",
    url="https://github.com/gregeasley/pytopspeed_modernized",
    project_urls={
        "Bug Reports": "https://github.com/gregeasley/pytopspeed_modernized/issues",
        "Source": "https://github.com/gregeasley/pytopspeed_modernized",
        "Documentation": "https://github.com/gregeasley/pytopspeed_modernized/blob/master/docs/README.md",
    },
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: System :: Archiving",
        "Topic :: Utilities",
    ],
    python_requires=">=3.8",
    install_requires=read_requirements(),
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=5.0.0",
            "mypy>=1.0.0",
        ],
        "docs": [
            "sphinx>=5.0.0",
            "sphinx-rtd-theme>=1.0.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "pytopspeed=cli.main:main",
        ],
    },
    include_package_data=True,
    package_data={
        "": ["*.md", "*.txt", "*.yml", "*.yaml"],
    },
    zip_safe=False,
    keywords="topspeed, clarion, database, sqlite, conversion, migration, legacy",
    license="MIT",
)
