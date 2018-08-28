from setuptools import setup, find_packages
import scihub

setup(
    name='scihub',
    version=scihub.__version__,
    long_description=scihub.__description__,
    url=scihub.__url__,
    packages=find_packages(),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        'qquery>=0.0.1',
        'shapely'
    ]
)
