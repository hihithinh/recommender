from setuptools import setup, find_packages

setup(
    name="bookcrossing-recommender",
    version="0.1.0",
    description="Distributed Book Recommender System using Spark ALS and LightGBM",
    author="Your Name",
    author_email="your.email@example.com",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.5.0",
        "pandas>=2.1.0",
        "numpy>=1.26.0",
        "scikit-learn>=1.3.0",
        "lightgbm>=4.1.0",
        "pyarrow>=14.0.0",
        "fastparquet>=2023.10.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.0",
            "pytest-cov>=4.1.0",
            "black>=23.7.0",
            "flake8>=6.1.0",
            "mypy>=1.5.0",
        ],
        "notebook": [
            "jupyter>=1.0.0",
            "jupyterlab>=4.0.0",
            "matplotlib>=3.8.0",
            "seaborn>=0.13.0",
            "plotly>=5.18.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Education",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
)
