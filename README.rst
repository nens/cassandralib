cassandralib
============

Python library to communicate with `Cassandra <http://cassandra.apache.org/>`_.

Building cassandralib
---------------------

Since cassandralib makes use of `pandas <http://pandas.pydata.org/>`_, which is built on top of `Numpy <http://numpy.scipy.org/>`_, this transitive dependency needs to be installed first. On Ubuntu, this is most easily done via::

	sudo apt-get install python-numpy

Instructions for other platforms may be found `here <http://www.scipy.org/Installing_SciPy/>`_.

To build pandas, we'll need Python header files (installing it via your package manager will probably not give you an up-to-date version, because, at the time of writing, pandas is being actively developed, with new releases every month). On Ubuntu::

	sudo apt-get install python-dev

If all is well and `git <http://git-scm.com/>`_ is present, tslib should now build smoothly::

	git clone https://github.com/nens/tslib.git
	cd tslib
	python bootstrap.py
	bin/buildout