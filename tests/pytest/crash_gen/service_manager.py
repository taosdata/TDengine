import os
import io
import sys
import threading
import signal
import logging
import time
import subprocess

from typing import IO, List

try:
    import psutil
except:
    print("Psutil module needed, please install: sudo pip3 install psutil")
    sys.exit(-1)

from queue import Queue, Empty

from .misc import Logging, Status, CrashGenError, Dice, Helper, Progress
from .db import DbConn, DbTarget

class TdeInstance():
    """
    A class to capture the *static* information of a TDengine instance,
    including the location of the various files/directories, and basica
    configuration.
    """

    @classmethod
    def _getBuildPath(cls):
        selfPath = os.path.dirname(os.path.realpath(__file__))
        if ("community" in selfPath):
            projPath = selfPath[:selfPath.find("communit")]
        else:
            projPath = selfPath[:selfPath.find("tests")]

        buildPath = None
        for root, dirs, files in os.walk(projPath):
            if ("taosd" in files):
                rootRealPath = os.path.dirname(os.path.realpath(root))
                if ("packaging" not in rootRealPath):
                    buildPath = root[:len(root) - len("/build/bin")]
                    break
        if buildPath == None:
            raise RuntimeError("Failed to determine buildPath, selfPath={}, projPath={}"
                .format(selfPath, projPath))
        return buildPath

    @classmethod
    def prepareGcovEnv(cls, env):
        # Ref: https://gcc.gnu.org/onlinedocs/gcc/Cross-profiling.html
        bPath = cls._getBuildPath() # build PATH
        numSegments = len(bPath.split('/')) - 1 # "/x/TDengine/build" should yield 3
        numSegments = numSegments - 1 # DEBUG only
        env['GCOV_PREFIX'] = bPath + '/svc_gcov'
        env['GCOV_PREFIX_STRIP'] = str(numSegments) # Strip every element, plus, ENV needs strings
        Logging.info("Preparing GCOV environement to strip {} elements and use path: {}".format(
            numSegments, env['GCOV_PREFIX'] ))

    def __init__(self, subdir='test', tInstNum=0, port=6030, fepPort=6030):
        self._buildDir  = self._getBuildPath()
        self._subdir    = '/' + subdir # TODO: tolerate "/"
        self._port      = port # TODO: support different IP address too
        self._fepPort   = fepPort

        self._tInstNum    = tInstNum
        self._smThread    = ServiceManagerThread()

    def getDbTarget(self):
        return DbTarget(self.getCfgDir(), self.getHostAddr(), self._port)

    def getPort(self):
        return self._port

    def __repr__(self):
        return "[TdeInstance: {}, subdir={}]".format(
            self._buildDir, Helper.getFriendlyPath(self._subdir))
    
    def generateCfgFile(self):       
        # print("Logger = {}".format(logger))
        # buildPath = self.getBuildPath()
        # taosdPath = self._buildPath + "/build/bin/taosd"

        cfgDir  = self.getCfgDir()
        cfgFile = cfgDir + "/taos.cfg" # TODO: inquire if this is fixed
        if os.path.exists(cfgFile):
            if os.path.isfile(cfgFile):
                Logging.warning("Config file exists already, skip creation: {}".format(cfgFile))
                return # cfg file already exists, nothing to do
            else:
                raise CrashGenError("Invalid config file: {}".format(cfgFile))
        # Now that the cfg file doesn't exist
        if os.path.exists(cfgDir):
            if not os.path.isdir(cfgDir):
                raise CrashGenError("Invalid config dir: {}".format(cfgDir))
            # else: good path
        else: 
            os.makedirs(cfgDir, exist_ok=True) # like "mkdir -p"
        # Now we have a good cfg dir
        cfgValues = {
            'runDir':   self.getRunDir(),
            'ip':       '127.0.0.1', # TODO: change to a network addressable ip
            'port':     self._port,
            'fepPort':  self._fepPort,
        }
        cfgTemplate = """
dataDir {runDir}/data
logDir  {runDir}/log

charset UTF-8

firstEp {ip}:{fepPort}
fqdn {ip}
serverPort {port}

# was all 135 below
dDebugFlag 135
cDebugFlag 135
rpcDebugFlag 135
qDebugFlag 135
# httpDebugFlag 143
# asyncLog 0
# tables 10
maxtablesPerVnode 10
rpcMaxTime 101
# cache 2
keep 36500
# walLevel 2
walLevel 1
#
# maxConnections 100
"""
        cfgContent = cfgTemplate.format_map(cfgValues)
        f = open(cfgFile, "w")
        f.write(cfgContent)
        f.close()

    def rotateLogs(self):
        logPath = self.getLogDir()
        # ref: https://stackoverflow.com/questions/1995373/deleting-all-files-in-a-directory-with-python/1995397
        if os.path.exists(logPath):
            logPathSaved = logPath + "_" + time.strftime('%Y-%m-%d-%H-%M-%S')
            Logging.info("Saving old log files to: {}".format(logPathSaved))
            os.rename(logPath, logPathSaved)
        # os.mkdir(logPath) # recreate, no need actually, TDengine will auto-create with proper perms


    def getExecFile(self): # .../taosd
        return self._buildDir + "/build/bin/taosd"

    def getRunDir(self): # TODO: rename to "root dir" ?!
        return self._buildDir + self._subdir

    def getCfgDir(self): # path, not file
        return self.getRunDir() + "/cfg"

    def getLogDir(self):
        return self.getRunDir() + "/log"

    def getHostAddr(self):
        return "127.0.0.1"

    def getServiceCmdLine(self): # to start the instance
        return [self.getExecFile(), '-c', self.getCfgDir()] # used in subproce.Popen()
    
    def _getDnodes(self, dbc):
        dbc.query("show dnodes")
        cols = dbc.getQueryResult() #  id,end_point,vnodes,cores,status,role,create_time,offline reason
        return {c[1]:c[4] for c in cols} # {'xxx:6030':'ready', 'xxx:6130':'ready'}

    def createDnode(self, dbt: DbTarget):
        """
        With a connection to the "first" EP, let's create a dnode for someone else who
        wants to join.
        """
        dbc = DbConn.createNative(self.getDbTarget())
        dbc.open()

        if dbt.getEp() in self._getDnodes(dbc):
            Logging.info("Skipping DNode creation for: {}".format(dbt))
            dbc.close()
            return

        sql = "CREATE DNODE \"{}\"".format(dbt.getEp())
        dbc.execute(sql)
        dbc.close()

    def getStatus(self):
        return self._smThread.getStatus()

    def getSmThread(self):
        return self._smThread

    def start(self):
        if not self.getStatus().isStopped():
            raise CrashGenError("Cannot start instance from status: {}".format(self.getStatus()))

        Logging.info("Starting TDengine instance: {}".format(self))
        self.generateCfgFile() # service side generates config file, client does not
        self.rotateLogs()

        self._smThread.start(self.getServiceCmdLine())

    def stop(self):
        self._smThread.stop()

    def isFirst(self):
        return self._tInstNum == 0


class TdeSubProcess:
    """
    A class to to represent the actual sub process that is the run-time
    of a TDengine instance. 

    It takes a TdeInstance object as its parameter, with the rationale being
    "a sub process runs an instance".
    """

    # RET_ALREADY_STOPPED = -1
    # RET_TIME_OUT = -3
    # RET_SUCCESS = -4

    def __init__(self):
        self.subProcess = None
        # if tInst is None:
        #     raise CrashGenError("Empty instance not allowed in TdeSubProcess")
        # self._tInst = tInst # Default create at ServiceManagerThread

    def __repr__(self):
        if self.subProcess is None:
            return '[TdeSubProc: Empty]'
        return '[TdeSubProc: pid = {}]'.format(self.getPid())

    def getStdOut(self):
        return self.subProcess.stdout

    def getStdErr(self):
        return self.subProcess.stderr

    def isRunning(self):
        return self.subProcess is not None

    def getPid(self):
        return self.subProcess.pid

    def start(self, cmdLine):
        ON_POSIX = 'posix' in sys.builtin_module_names

        # Sanity check
        if self.subProcess:  # already there
            raise RuntimeError("Corrupt process state")

        # Prepare environment variables for coverage information
        # Ref: https://stackoverflow.com/questions/2231227/python-subprocess-popen-with-a-modified-environment
        myEnv = os.environ.copy()
        TdeInstance.prepareGcovEnv(myEnv)

        # print(myEnv)
        # print(myEnv.items())
        # print("Starting TDengine via Shell: {}".format(cmdLineStr))

        useShell = True    
        self.subProcess = subprocess.Popen(
            ' '.join(cmdLine) if useShell else cmdLine,
            shell=useShell,
            # svcCmdSingle, shell=True, # capture core dump?
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            # bufsize=1, # not supported in binary mode
            close_fds=ON_POSIX,
            env=myEnv
            )  # had text=True, which interferred with reading EOF

    STOP_SIGNAL = signal.SIGKILL # What signal to use (in kill) to stop a taosd process?

    def stop(self):
        """
        Stop a sub process, and try to return a meaningful return code.

        Common POSIX signal values (from man -7 signal):
        SIGHUP           1
        SIGINT           2 
        SIGQUIT          3 
        SIGILL           4
        SIGTRAP          5
        SIGABRT          6 
        SIGIOT           6 
        SIGBUS           7 
        SIGEMT           - 
        SIGFPE           8  
        SIGKILL          9  
        SIGUSR1         10 
        SIGSEGV         11
        SIGUSR2         12
        """
        if not self.subProcess:
            Logging.error("Sub process already stopped")
            return  # -1

        retCode = self.subProcess.poll() # ret -N means killed with signal N, otherwise it's from exit(N)
        if retCode:  # valid return code, process ended
            retCode = -retCode # only if valid
            Logging.warning("TSP.stop(): process ended itself")
            self.subProcess = None
            return retCode

        # process still alive, let's interrupt it
        Logging.info("Terminate running process, send SIG_{} and wait...".format(self.STOP_SIGNAL))
        # sub process should end, then IPC queue should end, causing IO thread to end        
        topSubProc = psutil.Process(self.subProcess.pid)
        for child in topSubProc.children(recursive=True):  # or parent.children() for recursive=False
            child.send_signal(self.STOP_SIGNAL)
            time.sleep(0.2) # 200 ms
        # topSubProc.send_signal(sig) # now kill the main sub process (likely the Shell)

        self.subProcess.send_signal(self.STOP_SIGNAL) # main sub process (likely the Shell)
        self.subProcess.wait(20)
        retCode = self.subProcess.returncode # should always be there
        # May throw subprocess.TimeoutExpired exception above, therefore
        # The process is guranteed to have ended by now
        self.subProcess = None        
        if retCode != 0: # != (- signal.SIGINT):
            Logging.error("TSP.stop(): Failed to stop sub proc properly w/ SIG {}, retCode={}".format(
                self.STOP_SIGNAL, retCode))
        else:
            Logging.info("TSP.stop(): sub proc successfully terminated with SIG {}".format(self.STOP_SIGNAL))
        return - retCode

class ServiceManager:
    PAUSE_BETWEEN_IPC_CHECK = 1.2  # seconds between checks on STDOUT of sub process

    def __init__(self, numDnodes): # >1 when we run a cluster
        Logging.info("TDengine Service Manager (TSM) created")
        self._numDnodes = numDnodes # >1 means we have a cluster
        self._lock = threading.Lock()
        # signal.signal(signal.SIGTERM, self.sigIntHandler) # Moved to MainExec
        # signal.signal(signal.SIGINT, self.sigIntHandler)
        # signal.signal(signal.SIGUSR1, self.sigUsrHandler)  # different handler!

        self.inSigHandler = False
        # self._status = MainExec.STATUS_RUNNING # set inside
        # _startTaosService()
        self._runCluster = (numDnodes > 1)
        self._tInsts : List[TdeInstance] = []
        for i in range(0, numDnodes):
            ti = self._createTdeInstance(i) # construct tInst
            self._tInsts.append(ti)

        # self.svcMgrThreads : List[ServiceManagerThread] = []
        # for i in range(0, numDnodes):
        #     thread = self._createThread(i) # construct tInst
        #     self.svcMgrThreads.append(thread)

    def _createTdeInstance(self, dnIndex):
        if not self._runCluster: # single instance 
            subdir = 'test'
        else:        # Create all threads in a cluster
            subdir = 'cluster_dnode_{}'.format(dnIndex)
        fepPort= 6030 # firstEP Port
        port   = fepPort + dnIndex * 100
        return TdeInstance(subdir, dnIndex, port, fepPort)
        # return ServiceManagerThread(dnIndex, ti)

    def _doMenu(self):
        choice = ""
        while True:
            print("\nInterrupting Service Program, Choose an Action: ")
            print("1: Resume")
            print("2: Terminate")
            print("3: Restart")
            # Remember to update the if range below
            # print("Enter Choice: ", end="", flush=True)
            while choice == "":
                choice = input("Enter Choice: ")
                if choice != "":
                    break  # done with reading repeated input
            if choice in ["1", "2", "3"]:
                break  # we are done with whole method
            print("Invalid choice, please try again.")
            choice = ""  # reset
        return choice

    def sigUsrHandler(self, signalNumber, frame):
        print("Interrupting main thread execution upon SIGUSR1")
        if self.inSigHandler:  # already
            print("Ignoring repeated SIG...")
            return  # do nothing if it's already not running
        self.inSigHandler = True

        choice = self._doMenu()
        if choice == "1":            
            self.sigHandlerResume() # TODO: can the sub-process be blocked due to us not reading from queue?
        elif choice == "2":
            self.stopTaosServices()
        elif choice == "3": # Restart
            self.restart()
        else:
            raise RuntimeError("Invalid menu choice: {}".format(choice))

        self.inSigHandler = False

    def sigIntHandler(self, signalNumber, frame):
        print("ServiceManager: INT Signal Handler starting...")
        if self.inSigHandler:
            print("Ignoring repeated SIG_INT...")
            return
        self.inSigHandler = True

        self.stopTaosServices()
        print("ServiceManager: INT Signal Handler returning...")
        self.inSigHandler = False

    def sigHandlerResume(self):
        print("Resuming TDengine service manager (main thread)...\n\n")

    # def _updateThreadStatus(self):
    #     if self.svcMgrThread:  # valid svc mgr thread
    #         if self.svcMgrThread.isStopped():  # done?
    #             self.svcMgrThread.procIpcBatch()  # one last time. TODO: appropriate?
    #             self.svcMgrThread = None  # no more

    def isActive(self):
        """
        Determine if the service/cluster is active at all, i.e. at least
        one thread is not "stopped".
        """
        for ti in self._tInsts:
            if not ti.getStatus().isStopped():
                return True
        return False

    def isRunning(self):
        for ti in self._tInsts:
            if not ti.getStatus().isRunning():
                return False
        return True


    # def isRestarting(self):
    #     """
    #     Determine if the service/cluster is being "restarted", i.e., at least
    #     one thread is in "restarting" status
    #     """
    #     for thread in self.svcMgrThreads:
    #         if thread.isRestarting():
    #             return True
    #     return False

    def isStable(self):
        """
        Determine if the service/cluster is "stable", i.e. all of the
        threads are in "stable" status.
        """
        for ti in self._tInsts:
            if not ti.getStatus().isStable():
                return False
        return True

    def _procIpcAll(self):
        while self.isActive():
            Progress.emit(Progress.SERVICE_HEART_BEAT)
            for ti in self._tInsts: # all thread objects should always be valid
            # while self.isRunning() or self.isRestarting() :  # for as long as the svc mgr thread is still here
                status = ti.getStatus()
                if  status.isRunning():
                    th = ti.getSmThread()
                    th.procIpcBatch()  # regular processing,
                    if  status.isStopped():
                        th.procIpcBatch() # one last time?
                    # self._updateThreadStatus()
                    
            time.sleep(self.PAUSE_BETWEEN_IPC_CHECK)  # pause, before next round
        # raise CrashGenError("dummy")
        Logging.info("Service Manager Thread (with subprocess) ended, main thread exiting...")

    def _getFirstInstance(self):
        return self._tInsts[0]

    def startTaosServices(self):
        with self._lock:
            if self.isActive():
                raise RuntimeError("Cannot start TAOS service(s) when one/some may already be running")

            # Find if there's already a taosd service, and then kill it
            for proc in psutil.process_iter():
                if proc.name() == 'taosd':
                    Logging.info("Killing an existing TAOSD process in 2 seconds... press CTRL-C to interrupt")
                    time.sleep(2.0)
                    proc.kill()
                # print("Process: {}".format(proc.name()))
            
            # self.svcMgrThread = ServiceManagerThread()  # create the object
            
            for ti in self._tInsts:
                ti.start()  
                if not ti.isFirst():                                    
                    tFirst = self._getFirstInstance()
                    tFirst.createDnode(ti.getDbTarget())
                ti.getSmThread().procIpcBatch(trimToTarget=10, forceOutput=True)  # for printing 10 lines                                     

    def stopTaosServices(self):
        with self._lock:
            if not self.isActive():
                Logging.warning("Cannot stop TAOS service(s), already not active")
                return

            for ti in self._tInsts:
                ti.stop()
                
    def run(self):
        self.startTaosServices()
        self._procIpcAll()  # pump/process all the messages, may encounter SIG + restart
        if  self.isActive():  # if sig handler hasn't destroyed it by now
            self.stopTaosServices()  # should have started already

    def restart(self):
        if not self.isStable():
            Logging.warning("Cannot restart service/cluster, when not stable")
            return

        # self._isRestarting = True
        if  self.isActive():
            self.stopTaosServices()
        else:
            Logging.warning("Service not active when restart requested")

        self.startTaosServices()
        # self._isRestarting = False

    # def isRunning(self):
    #     return self.svcMgrThread != None

    # def isRestarting(self):
    #     return self._isRestarting

class ServiceManagerThread:
    """
    A class representing a dedicated thread which manages the "sub process"
    of the TDengine service, interacting with its STDOUT/ERR.

    It takes a TdeInstance parameter at creation time, or create a default    
    """
    MAX_QUEUE_SIZE = 10000

    def __init__(self):
        # Set the sub process
        self._tdeSubProcess = None # type: TdeSubProcess

        # Arrange the TDengine instance
        # self._tInstNum = tInstNum # instance serial number in cluster, ZERO based
        # self._tInst    = tInst or TdeInstance() # Need an instance

        self._thread = None # The actual thread, # type: threading.Thread
        self._status = Status(Status.STATUS_STOPPED) # The status of the underlying service, actually.

    def __repr__(self):
        return "[SvcMgrThread: status={}, subProc={}]".format(
            self.getStatus(), self._tdeSubProcess)

    def getStatus(self):
        return self._status

    # Start the thread (with sub process), and wait for the sub service
    # to become fully operational
    def start(self, cmdLine):
        if self._thread:
            raise RuntimeError("Unexpected _thread")
        if self._tdeSubProcess:
            raise RuntimeError("TDengine sub process already created/running")

        Logging.info("Attempting to start TAOS service: {}".format(self))

        self._status.set(Status.STATUS_STARTING)
        self._tdeSubProcess = TdeSubProcess()
        self._tdeSubProcess.start(cmdLine)

        self._ipcQueue = Queue()
        self._thread = threading.Thread( # First thread captures server OUTPUT
            target=self.svcOutputReader,
            args=(self._tdeSubProcess.getStdOut(), self._ipcQueue))
        self._thread.daemon = True  # thread dies with the program
        self._thread.start()

        self._thread2 = threading.Thread( # 2nd thread captures server ERRORs
            target=self.svcErrorReader,
            args=(self._tdeSubProcess.getStdErr(), self._ipcQueue))
        self._thread2.daemon = True  # thread dies with the program
        self._thread2.start()

        # wait for service to start
        for i in range(0, 100):
            time.sleep(1.0)
            # self.procIpcBatch() # don't pump message during start up
            Progress.emit(Progress.SERVICE_START_NAP)
            # print("_zz_", end="", flush=True)
            if self._status.isRunning():
                Logging.info("[] TDengine service READY to process requests")
                Logging.info("[] TAOS service started: {}".format(self))
                # self._verifyDnode(self._tInst) # query and ensure dnode is ready
                # Logging.debug("[] TAOS Dnode verified: {}".format(self))
                return  # now we've started
        # TODO: handle failure-to-start  better?
        self.procIpcBatch(100, True) # display output before cronking out, trim to last 20 msgs, force output
        raise RuntimeError("TDengine service did not start successfully: {}".format(self))

    def _verifyDnode(self, tInst: TdeInstance):
        dbc = DbConn.createNative(tInst.getDbTarget())
        dbc.open()
        dbc.query("show dnodes")
        # dbc.query("DESCRIBE {}.{}".format(dbName, self._stName))
        cols = dbc.getQueryResult() #  id,end_point,vnodes,cores,status,role,create_time,offline reason
        # ret = {row[0]:row[1] for row in stCols if row[3]=='TAG'} # name:type
        isValid = False
        for col in cols:
            # print("col = {}".format(col))
            ep = col[1].split(':') # 10.1.30.2:6030
            print("Found ep={}".format(ep))
            if tInst.getPort() == int(ep[1]): # That's us
                # print("Valid Dnode matched!")
                isValid = True # now we are valid
                break
        if not isValid:
            print("Failed to start dnode, sleep for a while")
            time.sleep(600)
            raise RuntimeError("Failed to start Dnode, expected port not found: {}".
                format(tInst.getPort()))
        dbc.close()

    def stop(self):
        # can be called from both main thread or signal handler
        Logging.info("Terminating TDengine service running as the sub process...")
        if self.getStatus().isStopped():
            Logging.info("Service already stopped")
            return
        if self.getStatus().isStopping():
            Logging.info("Service is already being stopped")
            return
        # Linux will send Control-C generated SIGINT to the TDengine process
        # already, ref:
        # https://unix.stackexchange.com/questions/176235/fork-and-how-signals-are-delivered-to-processes
        if not self._tdeSubProcess:
            raise RuntimeError("sub process object missing")

        self._status.set(Status.STATUS_STOPPING)
        # retCode = self._tdeSubProcess.stop()
        try:
            retCode = self._tdeSubProcess.stop()
            # print("Attempted to stop sub process, got return code: {}".format(retCode))
            if retCode == signal.SIGSEGV : # SGV
                Logging.error("[[--ERROR--]]: TDengine service SEGV fault (check core file!)")
        except subprocess.TimeoutExpired as err:
            Logging.info("Time out waiting for TDengine service process to exit")
        else:    
            if self._tdeSubProcess.isRunning():  # still running, should now never happen
                Logging.error("FAILED to stop sub process, it is still running... pid = {}".format(
                    self._tdeSubProcess.getPid()))
            else:
                self._tdeSubProcess = None  # not running any more
                self.join()  # stop the thread, change the status, etc.

        # Check if it's really stopped
        outputLines = 10 # for last output
        if  self.getStatus().isStopped():
            self.procIpcBatch(outputLines)  # one last time
            Logging.debug("End of TDengine Service Output: {}".format(self))
            Logging.info("----- TDengine Service (managed by SMT) is now terminated -----\n")
        else:
            print("WARNING: SMT did not terminate as expected: {}".format(self))

    def join(self):
        # TODO: sanity check
        if not self.getStatus().isStopping():
            raise RuntimeError(
                "SMT.Join(): Unexpected status: {}".format(self._status))

        if self._thread:
            self._thread.join()
            self._thread = None
            self._status.set(Status.STATUS_STOPPED)
            # STD ERR thread
            self._thread2.join()
            self._thread2 = None
        else:
            print("Joining empty thread, doing nothing")

    def _trimQueue(self, targetSize):
        if targetSize <= 0:
            return  # do nothing
        q = self._ipcQueue
        if (q.qsize() <= targetSize):  # no need to trim
            return

        Logging.debug("Triming IPC queue to target size: {}".format(targetSize))
        itemsToTrim = q.qsize() - targetSize
        for i in range(0, itemsToTrim):
            try:
                q.get_nowait()
            except Empty:
                break  # break out of for loop, no more trimming

    TD_READY_MSG = "TDengine is initialized successfully"

    def procIpcBatch(self, trimToTarget=0, forceOutput=False):
        self._trimQueue(trimToTarget)  # trim if necessary
        # Process all the output generated by the underlying sub process,
        # managed by IO thread
        print("<", end="", flush=True)
        while True:
            try:
                line = self._ipcQueue.get_nowait()  # getting output at fast speed
                self._printProgress("_o")
            except Empty:
                # time.sleep(2.3) # wait only if there's no output
                # no more output
                print(".>", end="", flush=True)
                return  # we are done with THIS BATCH
            else:  # got line, printing out
                if forceOutput:
                    Logging.info('[TAOSD] ' + line)
                else:
                    Logging.debug('[TAOSD] ' + line)
        print(">", end="", flush=True)

    _ProgressBars = ["--", "//", "||", "\\\\"]

    def _printProgress(self, msg):  # TODO: assuming 2 chars
        print(msg, end="", flush=True)
        pBar = self._ProgressBars[Dice.throw(4)]
        print(pBar, end="", flush=True)
        print('\b\b\b\b', end="", flush=True)

    def svcOutputReader(self, out: IO, queue):
        # Important Reference: https://stackoverflow.com/questions/375427/non-blocking-read-on-a-subprocess-pipe-in-python
        # print("This is the svcOutput Reader...")
        # for line in out :
        for line in iter(out.readline, b''):
            # print("Finished reading a line: {}".format(line))
            # print("Adding item to queue...")
            try:
                line = line.decode("utf-8").rstrip()
            except UnicodeError:
                print("\nNon-UTF8 server output: {}\n".format(line))

            # This might block, and then causing "out" buffer to block
            queue.put(line)
            self._printProgress("_i")

            if self._status.isStarting():  # we are starting, let's see if we have started
                if line.find(self.TD_READY_MSG) != -1:  # found
                    Logging.info("Waiting for the service to become FULLY READY")
                    time.sleep(1.0) # wait for the server to truly start. TODO: remove this
                    Logging.info("Service is now FULLY READY") # TODO: more ID info here?
                    self._status.set(Status.STATUS_RUNNING)

            # Trim the queue if necessary: TODO: try this 1 out of 10 times
            self._trimQueue(self.MAX_QUEUE_SIZE * 9 // 10)  # trim to 90% size

            if self._status.isStopping():  # TODO: use thread status instead
                # WAITING for stopping sub process to finish its outptu
                print("_w", end="", flush=True)

            # queue.put(line)
        # meaning sub process must have died
        Logging.info("EOF for TDengine STDOUT: {}".format(self))
        out.close()

    def svcErrorReader(self, err: IO, queue):
        for line in iter(err.readline, b''):
            Logging.info("TDengine STDERR: {}".format(line))
        Logging.info("EOF for TDengine STDERR: {}".format(self))
        err.close()