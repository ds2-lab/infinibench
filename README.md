# InfiniBench

InfiniBench is the benchmark toolkit and workload replayer for [InfiniStore](https://github.com/ds2-lab/infinistore) project.

## Benchmark

To run the benchmark, execute:

~~~
go get
make build
bin/infinibench [options]
~~~

Useful options:

~~~
-n [NUMBER]: Number of requests.
-c [NUMBER]: Number of concurrent clients.
-keymin [NUMBER]: Start postfix of generated keys. Generated key will be in the form of key_[keymin] ~ key_[keymax].
-keymax [NUMBER]: End postfix of generated keys.
-sz [NUMBER]: Object size in bytes.
-op [0 or 1]: Operation flag: 0 - SET (load the data store); 1 - GET.
-cli: Client library, support "infinistore"(default), "redis", "s3", "elasticache", "fsx", and "efs".
-addrlist [ADDR:PORT,...]: Server addresses.
-d [NUMBER]: Number of data shards for RS erasure coding. Ignore if cli is not "infinistore".
-p [NUMBER]: Number of parity shards for RS erasure coding. Ignore if cli is not "infinistore".
-bucket: S3 bucket name, Ignore if cle is not "s3".
-h: Print out help info.
-i [NUMBER]: Interval for every request (ms)
~~~

Example: command below will set 10 objects of size 1 MB from key_1 to key_10 using one concurrent client.

~~~
bin/infinibench -n 10 -c 1 -keymin 1 -keymax 10 -sz 1048576 -d 10 -p 2 -op 0
~~~

## Simulation

A sample of IBM docker registry trace is included. To run the simulation using sample trace:

~~~
go get
make simulate
~~~

## Replay

To replay trace, run following command:

~~~
go get
make build
bin/playback [trace file]
~~~

## License
InfiniBench source code is available under the MIT [License](/LICENSE).