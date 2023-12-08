from setuptools import setup, find_packages

setup(
    name='cd2datamanager',
    version='1.0.1',
    packages=find_packages(),
    url='https://github.com/tiberhealth/CanvasData2',
    license='',
    author='Tiber Health Innovations',
    author_email='tiber-tools@tiberhealth.com',
    description='Canvas Data 2 Data Manager',
    long_description='Canvas Data 2 Data Manager by Tiber Health Innovations. Script is used to pull data from the CanvasData2 portals and create sql and load files for MySQL version 8.x',
    install_requires=[
        "asyncio",
        "instructure-dap-client",
        "tqdm",
        "PyYaml",
        "typer"
    ],
    entry_points={
        "console_scripts": [
            "cd2datamanager = cd2datamanager.main:__main__",
        ]
    }
)
