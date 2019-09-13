#!/usr/bin/env python3

import logging
import sys
from benchmarks import causal_test
from benchmarks import causal_bench
from benchmarks import causal_bench_parallel_dag
import client as flclient

logging.basicConfig(stream=sys.stdout, level=logging.INFO)

if len(sys.argv) < 4:
    print('Usage: ./run_benchmark.py benchmark_name function_elb mode'
          + '{ip}')
    sys.exit(1)

f_elb = sys.argv[2]
mode = sys.argv[3]

print("before flconn")

if len(sys.argv) == 5:
    ip = sys.argv[4]
    flconn = flclient.FluentConnection(f_elb, ip)
else:
    flconn = flclient.FluentConnection(f_elb)

print("after flconn")

kvs = flconn.kvs_client

bname = sys.argv[1]

if bname == 'causal_test':
    causal_test.run(flconn, kvs, mode, None)
elif bname == 'causal_bench':
	causal_bench.run(flconn, kvs, mode, None)
elif bname == 'causal_bench_parallel_dag':
	causal_bench_parallel_dag.run(flconn, kvs, mode, None)
else:
    print('Unknown benchmark type: %s!' % (bname))