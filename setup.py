import os

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup


HERE = os.path.dirname(__file__)


def read_requirements(filename):
    filename = os.path.join(HERE, filename)
    return [line.strip() for line in open(filename) if line.strip()]


setup(
    name='keystone_cadf_logger',
    version='0.1',
    description='log specific CADF events from keystone',
    packages=['keystone_cadf_logger'],
    entry_points={
        'console_scripts': [
            'keystone-cadf-logger = keystone_cadf_logger.cli:main'
        ]
    },
    author='Matt Fischer',
    author_email='matt.fischer@twcable.com',
    url='https://github.com/matthewfischer/keystone_logger',
    license='Apache Software License',
    install_requires=read_requirements('requirements.txt'),
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Natural Language :: English',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Software Development :: Logging',
    ]
)
