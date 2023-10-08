from setuptools import setup, find_packages

setup(
    name='uploadoss',
    version='0.3',
    author='Binghe',
    description='Uploadoss is the general module of uploading database data and local files to the Alibaba Cloud OSS',
    packages=find_packages(),
    install_requires=[
        'oss2',
        'pandas',
        'pymysql',
        'configparser'
    ],
)

