# This file defines model class for DB table TestInputData which capture test details for view, update, delete test
# details in the DB. This file also implement other class which provide basic routines for updating test details in DB

#!/usr/bin/env python
# DB tables to refer
# TestInputData,
#  testid
#  frameworkid
#  start_time
#  end_time
#  end_status
#  exechostname
#  stathostname
#  timeout

# TestArgs,
#  exec script args

# ApplicationFrameworkMetadata
#  execution_script_location

from logger import LOG
import base64
import cPickle


class TestInputData():
    testid = None
    frameworkid = None
    frameworkname = None
    start_time = None
    end_time = None
    end_status = None
    exechostname = ""
    stathostname = ""
    timeout = None
    exec_results_path = None
    exec_path = None
    exec_log_path = None
    stats_results_path = None
    execScriptArgs = []
    execution_script_location = None
    email = None
    title = None
    purpose = None
    creation_time = None
    strace = None
    strace_process = None
    strace_delay = None
    strace_duration = None
    perf_process = None
    perf_delay = None
    perf_duration = None
    timeout_flag = False


class testObj():
    def __init__(self):
        self.TestInputData = TestInputData()

    def SerializeToString(self):
        """
        Serialize TestInputData object into string

        """
        pickled_string = cPickle.dumps(self.TestInputData)
        ret = base64.b64encode(pickled_string)
        return ret

    def ParseFromString(self, h):
        """
        De-serialize pickled string into TestInputData object

        """
        pickled_string = base64.b64decode(h)
        self.TestInputData = cPickle.loads(pickled_string)
        return


class testDefn():
    def __init__(self):
        self.db = None  # dbaccess.DBAccess(LOG.getLogger("dblog", "DH"))
        self.testobj = testObj()

    def __del__(self):
        if self.db is not None:
            self.db.close()

    def tostr():
        return str()

    def deserialize(self, h):
        self.testobj.ParseFromString(h)

    def serialize(self):
        return self.testobj.SerializeToString()

    def construct(self, tid):
        """
        Contruct TestInputData object by query DB details for fetching test information for a given testid tid

        """
        import dbaccess
        import config

        lctx = LOG.getLogger("dblog", "DH")
        cfg = config.CFG("DaytonaHost", lctx)
        cfg.readCFG("config.ini")
        self.db = dbaccess.DBAccess(cfg, LOG.getLogger("dblog", "DH"))
        self.testobj.TestInputData.testid = tid

        # Query TestInputData table for test details
        query_result = self.db.query("""select testid, frameworkid, start_time,
                                      end_time, end_status,
                                      timeout, cc_list, title, purpose, creation_time
                                      from TestInputData where testid = %s""", (self.testobj.TestInputData.testid,),
                                     False, False);

        (self.testobj.TestInputData.testid, self.testobj.TestInputData.frameworkid,
         self.testobj.TestInputData.start_time, self.testobj.TestInputData.end_time,
         self.testobj.TestInputData.end_status,
         self.testobj.TestInputData.timeout, self.testobj.TestInputData.email, self.testobj.TestInputData.title,
         self.testobj.TestInputData.purpose, self.testobj.TestInputData.creation_time) = query_result
        lctx.debug(query_result)

        # Query test host details from HostAssociation table
        query_result = self.db.query("""select ha.hostname, hat.name, hat.shared, hat.execution, hat.statistics
                                              from HostAssociation ha
                                              join  HostAssociationType hat
                                              on ha.hostassociationtypeid = hat.hostassociationtypeid
                                              where testid = %s and hat.frameworkid = %s""",
                                     (self.testobj.TestInputData.testid, self.testobj.TestInputData.frameworkid), True,
                                     False);

        for r in query_result:
            lctx.debug(r)
            if r[1] == 'statistics' and r[4] == 1:
                self.testobj.TestInputData.stathostname = r[0] + "," + self.testobj.TestInputData.stathostname
            elif r[1] == 'execution' and r[3] == 1:
                self.testobj.TestInputData.exechostname = r[0] + "," + self.testobj.TestInputData.exechostname

        self.testobj.TestInputData.stathostname = self.testobj.TestInputData.stathostname[:-1]
        self.testobj.TestInputData.exechostname = self.testobj.TestInputData.exechostname[:-1]

        lctx.debug(self.testobj.TestInputData.exechostname)
        lctx.debug(self.testobj.TestInputData.stathostname)

        # Query test argument values from TestArgs table
        query_result = self.db.query("""select * from TestArgs where testid = %s""",
                                     (self.testobj.TestInputData.testid,), True, False);
        self.testobj.TestInputData.execScriptArgs = query_result
        lctx.debug(query_result)

        # Query execution script location and framework name from ApplicationFrameworkMetadata
        query_result = self.db.query(
            """select execution_script_location, frameworkname from ApplicationFrameworkMetadata where frameworkid = %s""",
            (self.testobj.TestInputData.frameworkid,), False, False);
        (self.testobj.TestInputData.execution_script_location, self.testobj.TestInputData.frameworkname) = query_result
        lctx.debug(query_result)

        # Query STRACE profiler information for this test, if any
        query_result = self.db.query(
            """select processname, delay, duration from ProfilerFramework where testid = %s and profiler = %s""",
            (self.testobj.TestInputData.testid, 'STRACE'), False, False);

        if query_result:
            self.testobj.TestInputData.strace = True
            (self.testobj.TestInputData.strace_process, self.testobj.TestInputData.strace_delay,
             self.testobj.TestInputData.strace_duration) = query_result

        # Query PERF profiler information for this test, if any
        query_result = self.db.query(
            """select processname, delay, duration from ProfilerFramework where testid = %s and profiler = %s""",
            (self.testobj.TestInputData.testid, 'PERF'), False, False);

        if query_result:
            (self.testobj.TestInputData.perf_process, self.testobj.TestInputData.perf_delay,
             self.testobj.TestInputData.perf_duration) = query_result

    def updateStatus(self, curStatus, newStatus):
        """
        Update test status from curStatus to newStatus in database for a given test.

        """
        lctx = LOG.getLogger("dblog", "DH")
        lctx.debug("setting status from %s to %s" % (curStatus, newStatus))
        if self.testobj.TestInputData.exec_results_path is not None:
            test_logger = LOG.gettestlogger(self, "EXEC")
            test_logger.info("Setting test status from %s to %s" % (curStatus, newStatus))
        update_res = self.db.query("""update TestInputData SET end_status = %s where testid=%s""",
                                   (newStatus, self.testobj.TestInputData.testid), False, True);
        self.testobj.TestInputData.end_status = newStatus
        update_res = self.db.query(
            """update CommonFrameworkSchedulerQueue SET state = %s, message = %s, state_detail = %s where testid = %s""",
            (newStatus, newStatus, newStatus, self.testobj.TestInputData.testid), False, True)

        if newStatus == "finished clean" or newStatus == "failed" or newStatus == "abort" or newStatus == "kill" or newStatus == "timeout clean":
            update_res = self.db.query("""delete from CommonFrameworkSchedulerQueue where testid=%s""",
                                       (self.testobj.TestInputData.testid,), False, True);
            lctx.debug("Deleted entry from CommonFrameworkSchedulerQueue because of failure for : " + str(
                self.testobj.TestInputData.testid))

        return

    def updateStartTime(self, timestr):
        """
        Update test execution start time in database

        """
        lctx = LOG.getLogger("dblog", "DH")
        lctx.debug("setting start time  to %s" % (timestr))
        update_res = self.db.query("""update TestInputData SET start_time = %s where testid=%s""",
                                   (timestr, self.testobj.TestInputData.testid), False, True);
        self.testobj.TestInputData.start_time = timestr
        return

    def updateEndTime(self, timestr):
        """
        Update test execution end time in database

        """
        lctx = LOG.getLogger("dblog", "DH")
        lctx.debug("setting end time  to %s" % (timestr))
        update_res = self.db.query("""update TestInputData SET end_time = %s where testid=%s""",
                                   (timestr, self.testobj.TestInputData.testid), False, True);
        self.testobj.TestInputData.start_time = timestr
        return
