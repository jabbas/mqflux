import os
from setuptools import setup, find_packages

try:
    from pip._internal.req import parse_requirements
except ImportError:
    from pip.req import parse_requirements

def read(filename):
    path = os.path.join(os.path.dirname(__file__), filename)
    return open(path).read()


install_reqs = parse_requirements('requirements.txt', session=False)


setup(
    name="mqflux",
    version='0.1.0',
    author='Grzegorz Dzięgielewski',
    author_email='jabbas@jabbas.eu',
    description=('Grabs sensors data from mqtt and puts them into influxdb'),
    long_description=read('README.md'),
    license='GPL3',
    keywords='mqtt iot influxdb',
    packages=find_packages(),
    install_requires=[str(ir.req) for ir in install_reqs],
    entry_points={'console_scripts': 'mqflux = mqflux.cmd:cmd'},
)
