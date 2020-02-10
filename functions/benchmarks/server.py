#!/usr/bin/env python3
import cloudpickle as cp
import logging
import sys
import zmq

from . import causal_bench_1M
from . import utils

BENCHMARK_START_PORT = 3000


def benchmark(flconn, tid):
    logging.basicConfig(filename='log_benchmark.txt', level=logging.INFO,
                        format='%(asctime)s %(message)s')
    logging.info('tid is %d' % tid)
    logging.info('scheduler address is %s' % flconn.service_addr)

    logging.info('reading trace')
    trace = cp.load(open('/fluent/functions/benchmarks/workload_trace_1.0', 'rb'))
    logging.info('finish reading trace')

    ctx = zmq.Context(1)

    benchmark_start_socket = ctx.socket(zmq.PULL)
    benchmark_start_socket.bind('tcp://*:' + str(BENCHMARK_START_PORT + tid))
    kvs = flconn.kvs_client

    zipf = 0
    base = 0
    sum_probs = {}
    sum_probs[0] = 0.0

    zipf_write = 0
    base_write = 0
    sum_probs_write = {}
    sum_probs_write[0] = 0.0

    params = [zipf, base, sum_probs, zipf_write, base_write, sum_probs_write]

    while True:
        msg = benchmark_start_socket.recv_string()
        splits = msg.split(':')

        resp_addr = splits[0]
        bname = splits[1]
        mode = splits[2]
        segment = None
        loop = None
        if len(splits) == 4:
            segment = int(splits[3])
        elif len(splits) == 5:
            segment = int(splits[3])
            loop = int(splits[4])

        sckt = ctx.socket(zmq.PUSH)
        sckt.connect('tcp://' + resp_addr + ':3000')
        run_bench(bname, mode, segment, flconn, kvs, sckt, params, loop, trace, tid)


def run_bench(bname, mode, segment, flconn, kvs, sckt, params, loop, trace, tid):
    logging.info('Running benchmark %s with mode %s.' % (bname, mode))

    if bname == 'causal_bench_1M':
        latency = causal_bench_1M.run(flconn, kvs, mode, segment, params, loop, trace, tid)
    elif bname == 'causal_bench_1M_parallel':
        latency = causal_bench_1M_parallel.run(flconn, kvs, mode, segment, params, loop)
    else:
        logging.info('Unknown benchmark type: %s!' % (bname))
        sckt.send(b'END')
        return

    # some benchmark modes return no results
    sckt.send(cp.dumps(latency))
    logging.info('*** Benchmark %s finished. ***' % (bname))