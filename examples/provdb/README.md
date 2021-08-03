This directory contains a benchmark meant to reproduce
the behavior of Chimbuko's provenance database.

### Installing

You will need spack installed and available on the
command line. Load the gcc module as follows:

```
$ module swap xl gcc/9.1.0
```

Navigate in the `examples/provdb` directory and run:

```
$ spack env create sonata-env summit.yaml
$ spack env activate sonata-env
$ spack install
```

Then from the root of the source directory:

```
$ mkdir build
$ cmake .. -DCMAKE_CXX_COMPILER=mpic++ -DCMAKE_C_COMPILER=mpicc -DENABLE_EXAMPLES=ON -DENABLE_BEDROCK=OFF
$ make
```

### Running

Using the `build` directory as current working directory, edit `../examples/provdb/summit.lfs`
to change line 6 (`#BSUB -nnodes 11`), specifying the number of nodes requested. One node will
be used by the server (unless specified otherwise), the rest will be used by clients.

Then, submit the job as follows:

```
bsub ../examples/provdb/summit.lfs
```

You can also change the number of server nodes, the number of servers per node, the number
of databases per server, and the number of clients per node, using:

```
bsub ../examples/provdb/summit.lfs <num-server-nodes> <num-servers-per-node> <num-db-per-server> <num-clients-per-node>
```

By default, the following values are used:

- `1` server node;
- `1` server per node;
- `40/N` database per server, where `N` is the number of servers per node;
- `42` clients per node;

The client is setup to run 500 iterations. For each of these iterations, each client sends 2 records
to 2 distinct collections living in a designated database (databases are attributed in a round-robin
manner to clients), using asynchronous requests, which are stored into a vector. It send waits 1 second,
before checking all the requests for completion and removing the completed requests from the vector.
At the end of each iteration, each client prints out a log line as follows:

```
[2021-08-03 10:08:47.927] [info] [X] iteration 499, 0 pending requests
```
where `X` is the client's rank.

### Output

All the output will be produced in a directory named `exp-<jobid>`, apart from the unqlite files, which
will be produced in the current directory (and deleted by the client application at the end of the run,
if everything goes normally).

The `exp-<jobid>` directory also contains copies of the configuration files passed to the server
and clients, as well as the ERF files for the job scheduler.

The `client.out` file is the file of interest, which contains the log lines as explained above.
