.. mumu-spark documentation master file, created by
   sphinx-quickstart on Tue Sep 25 09:21:33 2018.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

Welcome to mumu-spark's documentation!
======================================
mumu-spark是一个学习项目，主要通过这个项目来了解和学习spark的基本使用方式和工作原理。mumu-spark主要包括弹性数据集rdd、spark sql、机器学习语言
mllib、实时工作流streaming、图形数据库graphx。通过这些模块的学习，初步掌握spark的使用方式。

The code is open source, and `available on GitHub`_.

.. _Read the docs: http://readthedocs.org/
.. _Sphinx: http://sphinx.pocoo.org/
.. _reStructuredText: http://sphinx.pocoo.org/rest.html
.. _CommonMark: http://commonmark.org/
.. _Subversion: http://subversion.tigris.org/
.. _Bazaar: http://bazaar.canonical.com/
.. _Git: http://git-scm.com/
.. _Mercurial: https://www.mercurial-scm.org/
.. _available on GitHub: https://github.com/mumuhadoop/mumu-spark

* :ref:`spark-user-docs`
* :ref:`spark-core-docs`
* :ref:`spark-feature-docs`
* :ref:`spark-faq-docs`

.. _spark-user-docs:

.. toctree::
   :maxdepth: 2
   :caption:  spark入门

   user/desc
   user/versions

.. _spark-core-docs:

.. toctree::
   :maxdepth: 2
   :caption:  spark核心

   core/dag

.. _spark-feature-docs:

.. toctree::
   :maxdepth: 2
   :glob:
   :caption: spark框架

   feature/sql
   feature/streaming
   feature/mllib
   feature/graphx

.. _spark-faq-docs:

.. toctree::
   :maxdepth: 2
   :glob:
   :caption: spark常见问题

   faq/common