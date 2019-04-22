from add_nodes import add_nodes
import util

c = util.init_k8s()

add_nodes(c, '../conf/kvs-base.yml', ['benchmark'], [2], ['100.96.1.6'], route_addr='ac5807c0e647411e98d680abbbf9888c-386205992.us-east-1.elb.amazonaws.com', function_addr='a281861e7647611e98d680abbbf9888c-1595742194.us-east-1.elb.amazonaws.com')
