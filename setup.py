import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="squaad",
    version="2.0",
    author='Iordanis Fostiropoulos',
    author_email='danny.fostiropoulos@gmail.com',
    description='Helper functions for running queries, ml pipeline, statistical analysis on SQUAAD framework',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="http://github.com/fostiropoulos/squaad",
    packages=setuptools.find_packages(),
    package_data={
   'squaad': ['sql/*']

   },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
        "Operating System :: OS Independent",
    ],
)
