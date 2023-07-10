# -*- coding: utf-8 -*-
"""
Created on Mon Jul 10 16:18:09 2023

@author: valentin.pasche1
"""

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


with open('README.md', encoding="utf-8") as f:
    long_description = f.read()


def setup_package():
    setup(name='gripit',
          version='0.0.1',
          description='Projet de recherche Ra&D HES-SO, distibution du code',
          long_description=long_description,
          long_description_content_type="text/markdown",
          packages=find_packages(exclude=[]),
          url='https://github.com/valheiafr/gripit',
          install_requires=['numpy',
                            'scipy',
                            'pandas',
                            'polars',
                            'pyarrow',
                            'geopandas',
                            'connectorx',
                            'psycopg2',
                            'sqlalchemy',
                            'contextily',
                            'fiona',
                            'folium',
                            'shapely',
                            'pyproj',
                            'networkx',
                            'osmnx',
                            ],
          license='Apache 2.0',
          author='Valentin Pasche',
          author_email='valentin.pasche@hefr.ch')


if __name__ == '__main__':
    setup_package()
    