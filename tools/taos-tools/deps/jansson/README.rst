Jansson README
==============

.. image:: https://github.com/akheron/jansson/workflows/tests/badge.svg
  :target: https://github.com/akheron/jansson/actions

.. image:: https://ci.appveyor.com/api/projects/status/lmhkkc4q8cwc65ko
  :target: https://ci.appveyor.com/project/akheron/jansson

.. image:: https://coveralls.io/repos/akheron/jansson/badge.png?branch=master
  :target: https://coveralls.io/r/akheron/jansson?branch=master

Jansson_ is a C library for encoding, decoding and manipulating JSON
data. Its main features and design principles are:

- Simple and intuitive API and data model

- `Comprehensive documentation`_

- No dependencies on other libraries

- Full Unicode support (UTF-8)

- Extensive test suite

Jansson is licensed under the `MIT license`_; see LICENSE in the
source distribution for details.


Compilation and Installation
----------------------------

You can download and install Jansson using the `vcpkg <https://github.com/Microsoft/vcpkg/>`_ dependency manager:

.. code-block:: bash

    git clone https://github.com/Microsoft/vcpkg.git
    cd vcpkg
    ./bootstrap-vcpkg.sh
    ./vcpkg integrate install
    vcpkg install jansson

The Jansson port in vcpkg is kept up to date by Microsoft team members and community contributors. If the version is out of date, please `create an issue or pull request <https://github.com/Microsoft/vcpkg/>`_ on the vcpkg repository.

If you obtained a `source tarball`_ from the "Releases" section of the main
site just use the standard autotools commands::

   $ ./configure
   $ make
   $ make install

To run the test suite, invoke::

   $ make check

If the source has been checked out from a Git repository, the
./configure script has to be generated first. The easiest way is to
use autoreconf::

   $ autoreconf -i


Documentation
-------------

Documentation is available at http://jansson.readthedocs.io/en/latest/.

The documentation source is in the ``doc/`` subdirectory. To generate
HTML documentation, invoke::

   $ make html

Then, point your browser to ``doc/_build/html/index.html``. Sphinx_
1.0 or newer is required to generate the documentation.


.. _Jansson: http://www.digip.org/jansson/
.. _`Comprehensive documentation`: http://jansson.readthedocs.io/en/latest/
.. _`MIT license`: http://www.opensource.org/licenses/mit-license.php
.. _`source tarball`: http://www.digip.org/jansson#releases
.. _Sphinx: http://sphinx.pocoo.org/
