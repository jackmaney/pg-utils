TODO
====

* Implement more methods and attributes that mirror those found in the `Pandas DataFrame API <http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe>`_. Of particlar note:

    * ``as_matrix`` (taking number of rows and returning the corresponding NumPy array representation).

    * ``dtypes`` (giving the PostgreSQL data types of each column) **Done**, although there is no actual ``dtype`` object; types are represented as strings.

    * ``get_dtype_counts`` (ie number of ``text`` columns, number of ``int`` columns, etc). **Done** with the same caveat as above for ``dtypes``.

    * ``select_dtypes``

    * ``query`` (this one may be tricky...)

    * ``groupby``

    * ``corr`` and ``cov``

    * ``clip``, ``cliplower``, ``clipupper``

    * ``mean``, ``max``, ``min``, ``median``, ``quantile``, ``rank``, ``std``, ``var``, etc.

    * ``dropna`` and ``fillna``

    * ``pivot``

    * ``nlargest``, ``nsmallest``

    * ``plot`` as per the new plotting API (ie ``df.plot.line(...)`` instead of ``df.plot(kind="line",...)``

* Refactor to have a ``Column`` class more closely matching a Pandas Series class.

* For initializers, allow ``conn`` to be have a default of ``None`` so that connections will have a default value of ``pg_utils.connection.Connection()``. Likewise with ``schema`` and ``"public"``.