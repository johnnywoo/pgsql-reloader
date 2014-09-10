pgsql-reloader
==============

This is a tool for fast reloading of database data (essentially dump/restore) between automatic tests.

It eliminates the need to clean up manually after tests: you just prepare a database state then load it before each test.
For a small database (tens of tables with few rows) it is substantially faster than pg_restore and the like.
Usually a database state can be shared over several tests, eliminating the need for calling test setup code repeatedly, which improves performance even further.

This code was simply ripped out of another project without testing, so it definitely needs some polish, 
but the core parts are tried-and-true.
