import setuptools

# read the contents of your README file
from os import path

this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setuptools.setup(
    name="pyfilespeed",
    version="0.0.7",
    author="Suvarchal K. Cheedela",
    author_email="suvarchal.kumar@gmail.com",
    description="test file transfer speed",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/suvarchal/pyfilespeed",
    packages=setuptools.find_packages(),
    python_requires='>=3.6',
    install_requires=[
        'Click', 'parse', 'tqdm', 'asyncssh',
        'parse', 'uvloop'
    ],
    entry_points='''
        [console_scripts]
        pyfilespeed=pyfilespeed.cli:runner
    ''',
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT",
        "Operating System :: Linux",
    ],
)

