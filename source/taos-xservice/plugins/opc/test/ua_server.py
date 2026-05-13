import threading
import random
import time
from opcua import ua, Server

def update_variables(node_int32, node_bool, node_float64):
    index = 0
    while True:
        index += 1
        node_int32.set_value(index, ua.VariantType.Int32)
        node_bool.set_value([True, False][index%2], ua.VariantType.Boolean)
        node_float64.set_value(index, ua.VariantType.Double)
        time.sleep(1)

if __name__ == "__main__":
    server = Server()
    server.set_endpoint("opc.tcp://127.0.0.1:4840/")

    ns = server.register_namespace("http://taosdata.com/")
    main = server.nodes.objects.add_object(ua.NodeId("main", ns), "main")
    node_int32 = main.add_variable(ua.NodeId(1001, ns), "int32", 32, ua.VariantType.Int32, ua.ObjectIds.Int32)
    node_bool = main.add_variable(ua.NodeId(1002, ns), "bool", True, ua.VariantType.Boolean, ua.ObjectIds.Boolean)
    node_float64 = main.add_variable(ua.NodeId(1003, ns), "float64", 4.5, ua.VariantType.Double, ua.ObjectIds.Double)

    main.add_reference(node_int32, ua.ObjectIds.Organizes)
    main.add_reference(node_bool, ua.ObjectIds.Organizes)
    main.add_reference(node_float64, ua.ObjectIds.Organizes)

    ns3 = server.register_namespace("http://taosdata.com/2")
    main = server.nodes.objects.add_object(ua.NodeId("main", ns3), "main")
    node_int32_ns3 = main.add_variable(ua.NodeId(1001, ns3), "int32", 32, ua.VariantType.Int32, ua.ObjectIds.Double)

    # 创建并启动更新变量的线程
    thread = threading.Thread(target=update_variables, args=(node_int32, node_bool, node_float64))
    thread.start()

    server.start()