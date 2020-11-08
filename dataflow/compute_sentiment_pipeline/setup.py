# System packages
from setuptools import find_packages, setup

setup(
    name="etl",
    version="0.0.1",
    install_requires=[
        "emoji==0.6.0"
    ],
    packages=find_packages()
)
