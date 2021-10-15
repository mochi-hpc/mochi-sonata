# What is Sonata?

Sonata is a remotely-accessibl JSON document store based on UnQLite and on
the Mochi suit of libraries. It enables managing collections of JSON records,
searching through them, and running Jx9 scripts on them.

# Got some examples?

A comprehensive set of examples is available in [this directory](examples).

# How do I install Sonata?

The easiest way to install Sonata is to use [spack](https://spack.readthedocs.io).
Once you have spack installed and setup on your machine, you need to added the
mochi namespace to it, as follows.

```
git clone https://github.com/mochi-hpc/mochi-spack-packages.git
spack repo add mochi-spack-packages
```

You can now install Sonata as follows.

```
spack install mochi-sonata
```

# And then?

Sonata comes in three libraries: sonata-server, sonata-client, and sonata-admin.
The server library contains the `sonata::Provider` class, which allows to start 
a Sonata service on a server program. The admin library contains the
`sonata::Admin` class, which enables creating and destroying database on a
running provider. The `sonata::Client` class is contained in the client library.
This class provides the main interface to open a database, and manipulat
collections.
