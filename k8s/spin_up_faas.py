from add_nodes import add_nodes
import util

c = util.init_k8s()

add_nodes(c, '../conf/kvs-base.yml', ['scheduler'], [2], ['100.96.1.6'], route_addr='ac5807c0e647411e98d680abbbf9888c-386205992.us-east-1.elb.amazonaws.com')
add_nodes(c, '../conf/kvs-base.yml', ['function'], [3], ['100.96.1.6'], route_addr='ac5807c0e647411e98d680abbbf9888c-386205992.us-east-1.elb.amazonaws.com', scheduler_ips=['172.20.61.82', '172.20.48.47'])