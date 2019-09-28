import cloudpickle as cp
import logging
import sys
import time
import zmq

from benchmarks import utils

logging.basicConfig(filename='log_trigger.txt', level=logging.INFO,
                    format='%(asctime)s %(message)s')

NUM_THREADS = 6

ips = []
with open('bench_ips.txt', 'r') as f:
    l = f.readline()
    while l:
        ips.append(l.strip())
        l = f.readline()

for ip in ips:
	print(ip)

msg = sys.argv[1]
ctx = zmq.Context(1)

recv_socket = ctx.socket(zmq.PULL)
recv_socket.bind('tcp://*:3000')

sent_msgs = 0

if 'create' in msg:
    sckt = ctx.socket(zmq.PUSH)
    sckt.connect('tcp://' + ips[0] + ':3000')

    sckt.send_string(msg)
    sent_msgs += 1
elif 'zipf' in msg:
	index = 0
	for ip in ips:
		for tid in range(NUM_THREADS):
			sckt = ctx.socket(zmq.PUSH)
			sckt.connect('tcp://' + ip + ':' + str(3000 + tid))
			sckt.send_string(msg)
			sent_msgs += 1
			index += 1
elif 'warmup' in msg:
	index = 0
	for ip in ips:
		for tid in range(NUM_THREADS):
			sckt = ctx.socket(zmq.PUSH)
			sckt.connect('tcp://' + ip + ':' + str(3000 + tid))
			sckt.send_string(msg + ':' + str(index))
			sent_msgs += 1
			index += 1
elif 'run' in msg:
	end_recv = 0

	latency = []

	total_inconsistency = 0

	for loop in range(300):
		print('loop is %d' % loop)
		index = 0
		for ip in ips:
			for tid in range(NUM_THREADS):
				sckt = ctx.socket(zmq.PUSH)
				sckt.connect('tcp://' + ip + ':' + str(3000 + tid))
				sckt.send_string(msg + ':' + str(index))
				sent_msgs += 1
				index += 1

		while end_recv < sent_msgs:
			payload = recv_socket.recv()
			logging.info("received response")
			end_recv += 1
			result = cp.loads(payload)
			latency += result[0]
			total_inconsistency += result[1]

		sent_msgs = 0
		end_recv = 0
		utils.print_latency_stats(latency, 'Causal')
		time.sleep(0.5)
	logging.info("benchmark done")
	utils.print_latency_stats(latency, 'Causal', True)
	utils.print_latency_stats(latency, 'Causal')
	print('total inconsistency is %d' % total_inconsistency)
	sys.exit(0)

end_recv = 0

latency = []

while end_recv < sent_msgs:
	payload = recv_socket.recv()
	logging.info("received response")
	end_recv += 1

logging.info("benchmark done")