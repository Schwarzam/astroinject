
# setup.py

from setuptools import setup, find_packages

setup(
    name='astroinject',
    version='1.0',
    author="Gustavo Schwarz",
    author_email="gustavo.b.schwarz@gmail.com",
    packages=find_packages(),
    install_requires = ['numpy', 'pandas', 'psycopg2-binary', 'sqlalchemy', 'pyyaml', 'astropy', 'logpool'],
    entry_points={
        'console_scripts': [
            'astroinject=astroinject.main:main',
        ],
    },
)
