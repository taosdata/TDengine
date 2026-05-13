import queue
import threading
import contextlib

# null object to stop thread
StopEvent = threading.Event()

def db_write_data_thread(thread_name, cmd_opr, json_file_path, output_file_path, logger):
    """Start the thread for db write data
    :thread_name: the thread name
    :param cmd_opr: the local or remote command operation instance
    :param json_file_path: the db write data json file path for taosBenchmark
    :param output_file_path: the db write data output file path for taosBenchmark
    :logger: log instance
    """
    try:
        cmd_opr.run_local_command("taosBenchmark -f {} -o {}".format(json_file_path, output_file_path), timeout=3600)
        logger.info("Start the db write data thread {}".format(thread_name))
    except Exception as ex:
        raise Exception("Write db data thread is failed: {}".format(ex))

def db_query_data_thread(thread_name, cmd_opr, json_file_path, output_file_path, logger):
    """Start the thread for db query data
    :thread_name: the thread name
    :param cmd_opr: the local or remote command operation instance
    :param json_file_path: the db query data json file path for taosBenchmark
    :param output_file_path: the db write data output file path for taosBenchmark
    :logger: log instance
    """
    try:
        cmd_opr.run_local_command("taosBenchmark -f {} -o {}".format(json_file_path, output_file_path), timeout=3600)
        logger.info("Start the db query thread {}".format(thread_name))
    except Exception as ex:
        raise Exception("Query db data thread is failed: {}".format(ex))

def db_topic_subscribe_thread(thread_name, cmd_opr, json_file_path, output_file_path, logger):
    """Start the thread for data topic subscription
    :thread_name: the thread name
    :param cmd_opr: the local or remote command operation instance
    :param json_file_path: the db data topic subscription json file path for taosBenchmark
    :param output_file_path: the db data topic subscription output file path for taosBenchmark
    :logger: log instance
    """
    try:
        cmd_opr.run_local_command("taosBenchmark -f {} -o {}".format(json_file_path, output_file_path), timeout=3600)
        logger.info("Start the data topic subscription thread {}".format(thread_name))
    except Exception as ex:
        raise Exception("Data topic subscription thread is failed: {}".format(ex))

class ThreadPool:
    """Thread pool to execute the thread tasks
    """
    def __init__(self, max_num, logger, max_task_num=None):
        self.log = logger
        # thread task queue number
        if max_task_num:
            self.task_queue = queue.Queue(max_task_num)
        else:
            self.task_queue = queue.Queue()
        self.max_num = max_num
        # cancel task flag
        self.cancel = False
        # terminal task flag
        self.terminal = False
        # thread instance list
        self.generate_list = []
        # available thread instance list
        self.free_list = []

    def put(self, func, args, callback=None):
        """Put a task into the thread pool
        :param func: the task function
        :param args: the task function params
        :param callback: the task function callback function
        :return: True as the thread pool is terminated, otherwise as None
        """
        # check cancel flag
        if self.cancel:
            return
        # create a new thread when there is no available thread and current total thread number is less than max thread number
        if len(self.free_list) == 0 and len(self.generate_list) < self.max_num:
            self.generate_thread()
        worker = (func, args, callback,)
        # put the task into the queue
        self.task_queue.put(worker)

    def generate_thread(self):
        """
        Create a thread
        """
        t = threading.Thread(target=self.call)
        t.start()
 
    def call(self):
        """
        Get and execute the task func
        """
        # get the thread name
        current_thread = threading.currentThread
        self.generate_list.append(current_thread)
        # get the task
        event = self.task_queue.get()
        while event != StopEvent:
            # get the thread function, params and callback function
            func, arguments, callback = event
            try:
                # execute the thread function
                print(*arguments)
                result = func(current_thread, *arguments)
                status = True
            except Exception as e:
                status = False
                result = None
                if self.terminal:
                    self.log.info("The thread {} is terminated as expected".format(current_thread))
                else:
                    self.log.error("Execute task failed: {}".format(e.args))
            # callbackk function is available
            if callback is not None:
                try:
                    callback(status, result)
                except Exception as e:
                    self.log.error("Execute callback function failed: {}".format(e.args))
            # 
            with self.work_state(current_thread):
                # set close thread flag
                if self.terminal:
                    event = StopEvent
                else:
                    # get the next task
                    event = self.task_queue.get()
        else:
            # remove the thread from thread instance list
            self.generate_list.remove(current_thread)

    def close(self):
        """
        Close all the threads when all the tasks are finished
        """
        # set cancel flag
        self.cancel = True
        full_size = len(self.generate_list)
        while full_size:
            self.task_queue.put(StopEvent)
            full_size -= 1

    def terminate(self):
        """Terminate the thread when tasks are not finished
        """
        # set terminal flag
        self.terminal = True
        while self.generate_list:
            self.task_queue.put(StopEvent)

    @contextlib.contextmanager
    def work_state(self, worker_thread):
        """Record the available thread or handle the task with available thread
        """
        self.free_list.append(worker_thread)
        try:
            yield
        finally:
            self.free_list.remove(worker_thread)
    