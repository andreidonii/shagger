from setuptools import setup

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='shagger',
    version='1.0',
    description='A kafka based draft microservice framework',
    author='Andrei Donii',
    author_email='569460@gmail.com',
    install_requires=required
)
