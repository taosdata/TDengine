from iso8601 import parse_date


def test_parse_time():
    s1 = "2018-10-03T14:38:15+08:00"
    t1 = parse_date(s1)
    print("\n", repr(t1))
    assert t1.isoformat() == "2018-10-03T14:38:15+08:00"
    s2 = "2018-10-03T14:38:16.8+08:00"
    t2 = parse_date(s2)
    assert t2.isoformat() == "2018-10-03T14:38:16.800000+08:00"
    s3 = "2018-10-03T14:38:16.65+08:00"
    t3 = parse_date(s3)
    assert t3.isoformat() == "2018-10-03T14:38:16.650000+08:00"
