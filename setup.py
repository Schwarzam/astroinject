
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
            'astroinject=astroinject.main:injection',
            'map_table=astroinject.main:map_table_command',
            'create_index=astroinject.main:create_index_command',
        ],
    },
)
