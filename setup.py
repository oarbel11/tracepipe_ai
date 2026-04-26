"""Setup configuration for Tracepipe AI."""
from setuptools import setup, find_packages

setup(
    name="tracepipe_ai",
    version="0.1.0",
    description="Column-level lineage tracking for Spark",
    packages=find_packages(),
    python_requires=">=3.8",
    install_requires=[],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-cov>=4.0.0",
        ]
    },
)
