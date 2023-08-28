
# setup.py

from setuptools import setup, find_packages

setup(
    name='astroinject',
    version='0.1',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'astroinject=astroinject.main:main',
        ],
    },
)
