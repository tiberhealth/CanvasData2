from setuptools import setup
from setuptools import setup, find_packages

setup(
    name='tibercanvasdata2',
    version='1.0',
    packages=find_packages(),
    url='https://github.com/tiberhealth/CanvasData2',
    license='',
    author='Tiber Health Innovations',
    author_email='tiber-tools@tiberhealth.com',
    description='Canvas Data 2 Pull Routine',
    long_description='Canvas Data 2 Pull Routine by Tiber Health Innovations. Script is used to pull data from the CanvasData2 portals and create sql and load files for MySQL version 8.x',
    install_requires=[
        "asyncio",
        "instructure-dap-client",
        "tqdm",
        "PyYaml",
        "typer"
    ]
)
