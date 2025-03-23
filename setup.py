from setuptools import setup, find_packages

setup(
    name="weather-service",
    version="0.1.0",
    packages=find_packages(),
    install_requires=[
        "fastapi",
        "uvicorn",
        "pygrib",
        "numpy",
        "matplotlib",
        "requests",
        "beautifulsoup4",
    ],
    python_requires=">=3.8",
) 