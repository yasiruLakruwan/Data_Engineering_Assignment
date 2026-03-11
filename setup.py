from setuptools import setup,find_packages
import os

with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name="Data Engineering Assignment",
    version="0.0.1",
    author="Yasiru Lakruwan",
    packages=find_packages(),
    install_requires=requirements
)