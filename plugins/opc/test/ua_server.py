from opcua import ua, Server

if __name__ == "__main__":
    server = Server()
    server.set_endpoint("opc.tcp://0.0.0.0:4840/")

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
    node_int32 = main.add_variable(ua.NodeId(1001, ns3), "int32", 32, ua.VariantType.Int32, ua.ObjectIds.Int32)

    server.start()