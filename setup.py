import sys
from setuptools import setup, find_packages

optional_packages = []

flags = [('--cassandra', 'cassandra'),
         ('-cassandra', 'cassandra')]

for flag, package in flags:
    if flag in sys.argv:
        optional_packages.append(package)
        sys.argv.remove(flag)

setup(name='tragedy',
      version='0.8.1',
      description='Object Abstraction for Cassandra',
      url='http://github.com/enki/tragedy/',
      download_url='http://github.com/enki/tragedy/',
      packages=['tragedy']+optional_packages,
      author='Paul Bohm',
      author_email='enki@bbq.io',
      platforms = 'any',
      license='MIT',
      keywords="cassandra oam database",
      install_requires=['thrift'],
      zip_safe=False,
)