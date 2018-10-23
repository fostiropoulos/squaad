from setuptools import setup

setup(name='squaad',
      version='1.0',
      description='Helper functions for running queries, ml pipeline, statistical analysis on SQUAAD dataset',
      url='http://github.com/fostiropoulos',
      author='Iordanis Fostiropoulos',
      author_email='fostirop@usc.edu',
      license='MIT',
      packages=['squaad'],
      install_requires=[
          'sklearn',
          'numpy',
          'seaborn',
          'matplotlib',
          'imblearn',
          'xlwt',
          'rpy2'
      ],
      zip_safe=False)
