4. Operator chaining
--------------------

The real purpose of ``cdb_query`` is to perform all of the steps asynchronously.
The ``ask``, ``validate``, ``reduce_soft_links`` and ``download_opendap`` operations can be
chained and applied to each simulation.

CMIP5
^^^^^
In `CMIP5`, simulations are ``institute, model, ensemble`` triples. Chaining operators will first
determine the simulations triples and chain the operators for every triples. Advanced options allow
to bypass this default setting. This will be covered in later recipes. This is the internal mechanics
but for the user it is fairly transparent (except when there is an error message).

With operator chaining, recipe 1 could be written::

    $ OPENID="your openid
    $ cdb_query CMIP5 ask validate record_validate download_opendap reduce \
                      --ask_month=1,2,10,11,12 \
                      --ask_var=tas:day-atmos-day,orog:fx-atmos-fx \
                      --ask_experiment=amip:1979-2004 \
                      --Xdata_node=http://esgf2.dkrz.de \
                      --openid=$OPENID \
                      --year=1979 --month=1 \
                      --out_destination=./out/CMIP5/ \
                      --num_procs=10 \
                      '' \
                      tas_ONDJF_pointers.validate.197901.retrieved.converted.nc

It does:

#. Finds ONDJF ``tas`` and fixed variable ``orog`` for ``amip``.
#. Excludes (``--Xdata_node=http://esgf2.dkrz.de``) data node ``http://esgf2.dkrz.de`` because it is a tape archive and tends to be slow.
#. Retrieve credentials (``--openid=$OPENID``). It will prompt for your password.
#. Record the result (``record_validate``) of ``validate`` to ``tas_ONDJF_pointers.validate.197901.retrieved.converted.nc.validate``.
#. Does this using 10 processes ``--num_procs=10``.
#. Download only January 1979 (``--year=1979 --month=1``).
#. Converts (the empty script ``''`` passed to ``reduce``) the data to the CMIP5 DRS to directory ``./out/CMIP5/``.


CORDEX
^^^^^^
In `CORDEX`, simulations are ``domain,driving_model,institute,rcm_model,rcm_version,ensemble`` sextuples. Chaining operators will first
determine the simulations triples and chain the operators for every sextuple. Advanced options allow
to bypass this default setting. This will be covered in later recipes. This is the internal mechanics
but for the user it is fairly transparent (except when there is an error message).

With operator chaining, recipe 3 could be written::

    $ OPENID="your openid"
    $ cdb_query CORDEX ask validate record_validate reduce_soft_links download_opendap reduce \ 
                      --ask_experiment=historical:1979-2004 --ask_var=pr:day --ask_month=6,7,8,9 \
                      --openid=$OPENID \
                      --year=1979 --month=6 \
                      --domain=EUR-11 \
                      --out_destination=./out_France/CORDEX/ \
                      --Xdata_node=http://esgf2.dkrz.de \
                      --num_procs=10 \
                      --reduce_soft_links_script='nc4sl subset --lonlatbox -5.0 10.0 40.0 53.0' \
                      '' \
                      pr_JJAS_France_pointers.validate.France.retrieved.converted.nc

It does:

#. Finds JJAS ``pr`` for ``historical``.
#. Excludes (``--Xdata_node=http://esgf2.dkrz.de``) data node ``http://esgf2.dkrz.de`` because it is a tape archive and tends to be slow.
#. Retrieve certificates (``--openid=$OPENID``). It will prompt for your password.
#. Record the result (``record_validate``) of ``validate`` to ``pr_JJAS_France_pointers.validate.France.retrieved.converted.nc.validate``.
#. Does this using 10 processes ``--num_procs=10``.
#. Download only June 1979 (``--year=1979 --month=6``).
#. Converts (the empty script ``''`` passed to ``reduce``) the data to the CMIP5 DRS to directory ``./out_France/CORDEX/``.

.. note:: From now on, recipes will be presented as chained operators.
