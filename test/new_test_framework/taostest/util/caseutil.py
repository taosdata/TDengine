def parse_param(case_param):
    """
    parse case param in form: 'k1=v1;k2=v2' to dict
    """
    kvs = [kv.split('=') for kv in case_param.split(';')]
    return {k: v for k, v in kvs}
