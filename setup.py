# -*- coding: utf-8 -*-
from distutils.core import setup

setup(
    name='tq',
    version='0.1.0',
    author='Johan Stén',
    author_email='johan.sten@gmail.com',
    packages=['tq'],
    url='https://github.com/johansten/tq',
    license='LICENSE.txt',
    description='A python task queue.',
    long_description=open('README.md').read(),
    classifiers =[
        'Programming Language :: Python',
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries :: Python Modules'],
    install_requires=[
        "redis",
    ],
)
