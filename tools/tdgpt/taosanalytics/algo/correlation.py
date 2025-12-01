from taosanalytics.servicemgmt import loader


def do_dtw(input_list1: list, input_list2: list, params):
    s = loader.get_service("dtw")

    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)

    s.set_params(params)
    dist, path = s.execute()
    return dist, path

def do_tlcc(input_list1: list, input_list2: list, params):
    s = loader.get_service("tlcc")

    s.set_input_list(input_list1, None)
    s.set_second_input_data(input_list2)

    s.set_params(params)
    lags, ccf_vals = s.execute()
    return lags, ccf_vals
