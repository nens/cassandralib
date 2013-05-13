from setuptools import setup

version = '0.5.dev0'

long_description = '\n\n'.join([
    open('README.rst').read(),
    open('CREDITS.rst').read(),
    open('CHANGES.rst').read(),
    ])

install_requires = [
    'numpy',
    'pandas',
    'pycassa',
    'pytz',
    'setuptools',
    ],

tests_require = [
    ]

setup(name='cassandralib',
      version=version,
      description="Python library to talk to Cassandra",
      long_description=long_description,
      # Get strings from http://www.python.org/pypi?%3Aaction=list_classifiers
      classifiers=['Programming Language :: Python'],
      keywords=[],
      author='Berto Booijink',
      author_email='berto.booijink@nelen-schuurmans.nl',
      url='https://github.com/nens/cassandralib',
      license='MIT',
      packages=['cassandralib'],
      include_package_data=True,
      zip_safe=False,
      install_requires=install_requires,
      tests_require=tests_require,
      extras_require={'test': tests_require},
      entry_points={
          'console_scripts': [
          ]},
      )
