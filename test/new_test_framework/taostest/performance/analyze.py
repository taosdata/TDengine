# coding: utf-8


from abc import ABCMeta, abstractmethod

from .monitor import Monitor


class Analyze(metaclass=ABCMeta):
    '''
    分析指标数据
    '''

    cpu_engine_msg = Monitor.cpu_engine_info

    @abstractmethod
    def get_max_info(self, info):
        '''
        description: get the max-value from list of info without NULL value
        param info:  list
        return: float/int
        '''
        new_info = list(filter(None, info))
        max_info = max(new_info)
        return max_info

    @abstractmethod
    def get_avg_info(self, info):
        '''
        description: get the avg-value from list of info without NULL value
        param info: list
        return: float/int
        '''
        new_info = list(filter(None, info))
        sum_info = 0
        for i in new_info:
            sum_info += i
        len_info = len(new_info)
        avg_info = sum_info / len_info
        return avg_info

    @abstractmethod
    def get_min_info(self, info):
        '''
        description: get the min-value from list of info without NULL value
        param {info}: list
        return: {float/int}
        '''
        new_info = list(filter(None, info))
        min_info = min(new_info)
        return min_info

    @abstractmethod
    def get_percentile_info(self, percentile, info):
        '''
        description: get the percentile from sorted list of info without NULL value
        param {*}
        return {*}
        '''
        new_info = list(filter(None, info))
        overload_num = 0
        len_info = len(new_info)
        for i in new_info:
            if i >= percentile:
                overload_num += 1

        percentile_info = overload_num / len_info
        return percentile_info

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        pass


class cpu_engine_Analyze(Analyze):
    # self.cpu_engine_msg = monitor.cpu_engine_info(checktime=None)

    def get_max_info(self, cpu_engine_msg):
        return super().get_max_info(cpu_engine_msg)

    def get_avg_info(self, cpu_engine_msg):
        return super().get_avg_info(cpu_engine_msg)

    def get_min_info(self, cpu_engine_msg):
        return super().get_min_info(cpu_engine_msg)

    def get_percentile_info(self, percentile, cpu_engine_msg):
        return super().get_percentile_info(percentile, cpu_engine_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class cpu_system_Analyze(Analyze):
    # cpu_system_msg = monitor.cpu_system_info()

    def get_max_info(self, cpu_system_msg):
        return super().get_max_info(cpu_system_msg)

    def get_avg_info(self, cpu_system_msg):
        return super().get_avg_info(cpu_system_msg)

    def get_min_info(self, cpu_system_msg):
        return super().get_min_info(cpu_system_msg)

    def get_percentile_info(self, percentile, cpu_system_msg):
        return super().get_percentile_info(percentile, cpu_system_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class mem_engine_Analyze(Analyze):
    # mem_engine_msg = monitor.mem_engine_info()

    def get_max_info(self, mem_engine_msg):
        return super().get_max_info(mem_engine_msg)

    def get_avg_info(self, mem_engine_msg):
        return super().get_avg_info(mem_engine_msg)

    def get_min_info(self, mem_engine_msg):
        return super().get_min_info(mem_engine_msg)

    def get_percentile_info(self, percentile, mem_engine_msg):
        return super().get_percentile_info(percentile, mem_engine_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class mem_system_Analyze(Analyze):
    # mem_system_msg = monitor.mem_system_info()

    def get_max_info(self, mem_system_msg):
        return super().get_max_info(mem_system_msg)

    def get_avg_info(self, mem_system_msg):
        return super().get_avg_info(mem_system_msg)

    def get_min_info(self, mem_system_msg):
        return super().get_min_info(mem_system_msg)

    def get_percentile_info(self, percentile, mem_system_msg):
        return super().get_percentile_info(percentile, mem_system_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class io_read_disk_Analyze(Analyze):
    # io_read_disk_msg = monitor.io_read_disk_info()

    def get_max_info(self, io_read_disk_msg):
        return super().get_max_info(io_read_disk_msg)

    def get_avg_info(self, io_read_disk_msg):
        return super().get_avg_info(io_read_disk_msg)

    def get_min_info(self, io_read_disk_msg):
        return super().get_min_info(io_read_disk_msg)

    def get_percentile_info(self, percentile, io_read_disk_msg):
        return super().get_percentile_info(percentile, io_read_disk_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class io_write_disk_Analyze(Analyze):
    # io_write_disk_msg = monitor.io_write_disk_info()

    def get_max_info(self, io_write_disk_msg):
        return super().get_max_info(io_write_disk_msg)

    def get_avg_info(self, io_write_disk_msg):
        return super().get_avg_info(io_write_disk_msg)

    def get_min_info(self, io_write_disk_msg):
        return super().get_min_info(io_write_disk_msg)

    def get_percentile_info(self, percentile, io_write_disk_msg):
        return super().get_percentile_info(percentile, io_write_disk_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class disk_engine_used_Analyze(Analyze):
    # disk_engine_used_msg = monitor.disk_engine_info()

    def get_max_info(self, disk_engine_used_msg):
        return super().get_max_info(disk_engine_used_msg)

    def get_avg_info(self, disk_engine_used_msg):
        return super().get_avg_info(disk_engine_used_msg)

    def get_min_info(self, disk_engine_used_msg):
        return super().get_min_info(disk_engine_used_msg)

    def get_percentile_info(self, percentile, disk_engine_used_msg):
        return super().get_percentile_info(percentile, disk_engine_used_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class net_in_Analyze(Analyze):
    # net_in_msg = monitor.net_in_info()

    def get_max_info(self, net_in_msg):
        return super().get_max_info(net_in_msg)

    def get_avg_info(self, net_in_msg):
        return super().get_avg_info(net_in_msg)

    def get_min_info(self, net_in_msg):
        return super().get_min_info(net_in_msg)

    def get_percentile_info(self, percentile, net_in_msg):
        return super().get_percentile_info(percentile, net_in_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)


class net_out_Analyze(Analyze):
    # net_out_msg = monitor.net_out_info()

    def get_max_info(self, net_out_msg):
        return super().get_max_info(net_out_msg)

    def get_avg_info(self, net_out_msg):
        return super().get_avg_info(net_out_msg)

    def get_min_info(self, net_out_msg):
        return super().get_min_info(net_out_msg)

    def get_percentile_info(self, percentile, net_out_msg):
        return super().get_percentile_info(percentile, net_out_msg)

    def check_diff_version_perf(self, this_test_info, benchmark_info):
        return super().check_diff_version_perf(this_test_info, benchmark_info)
