from util.log import tdLog


def calc_availability_percent(total_window_ms, unavailable_window_ms):
    if total_window_ms <= 0:
        return 0.0
    healthy_ms = max(total_window_ms - unavailable_window_ms, 0)
    return healthy_ms * 100.0 / total_window_ms


def assert_availability_threshold(total_window_ms, unavailable_window_ms, threshold=99.0):
    availability = calc_availability_percent(total_window_ms, unavailable_window_ms)
    tdLog.info(
        f"assigned-stepdown-availability total={total_window_ms} unavailable={unavailable_window_ms} "
        f"availability={availability:.3f}% threshold={threshold:.3f}%"
    )
    if availability < threshold:
        tdLog.exit(
            f"assigned-stepdown guard failed availability threshold: {availability:.3f}% < {threshold:.3f}%"
        )
    return availability
