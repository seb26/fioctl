#!/usr/bin/env python3

from setuptools import setup

with open("README.md", "r") as f:
  long_description = f.read()

setup(name='fioctl',
      version='1.2.0',
      description='Frame.io CLI (seb26 fork - Python 3+)',
      long_description=long_description,
      packages=['fioctl'],
      include_package_data=True,
      install_requires=[
        'bitmath',
        'cached-property',
        'click',
        # 'frameioclient==0.6.0',
        'furl',
        'pyyaml',
        'requests',
        'rich',
        'tabulate',
        'token-bucket',
        'treelib',
      ],
      entry_points={
        'console_scripts': 'fioctl=fioctl.fioctl:cli'
      },
      author='Frame.io, Inc.',
      author_email='platform@frame.io',
      license='MIT')