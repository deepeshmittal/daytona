# This file provide routines for scheduler to interact with database. It also implement class which defines DBmon
# thread which continuously query DB for new test in waiting queue

#!/usr/bin/env python
import mysql.connector
from mysql.connector import Error as Dberror
import testobj
import time
import os
import threading
import common
import shutil
from collections import defaultdict

import common


class DBAccess():
    """
    This class implements DB interaction routines
    """
    cfg=None

    def __init__(self, cfg, lctx):
        """
        Contructor : Initializes DB connection with config parameters provided in config file

        """
        self._db_connection = None
        self._db_cur = None
        self.cfg = cfg
        self.lctx = lctx
        try:
            if (self._db_connection and self._db_cur) and (self._db_cur is not None) and (self._db_connection is not None):
                self._db_cur.close()
                self._db_connection.close()

            self._db_connection = mysql.connector.connect(host=cfg.mysql_host, database=cfg.msql_db,
                                                          user=cfg.mysql_user, password=cfg.mysql_password)
            self._db_cur = self._db_connection.cursor()

            self.lctx.debug('Connected to MySQL database')
        except Dberror as e:
            self.lctx.error(e)

    def close(self):
        """
        Close DB connection

        """
        if (self._db_connection and self._db_cur) and (self._db_cur is not None) and (self._db_connection is not None):
            self._db_cur.close()
            self._db_connection.close()

    def query(self, query, params, n, commit, lastid=False, rowcount=False):
        """
        Execute database query and returns the result based on input flag

        """
        try:
            self._db_cur.execute(query, params)
            if commit:
                self._db_connection.commit()
            if lastid:
                return self._db_cur.lastrowid
            if rowcount:
                return self._db_cur.rowcount
            if not n:
                return self._db_cur.fetchone()
            else:
                return self._db_cur.fetchall()
        except Dberror as err:
            self.lctx.debug(err)

    def commit(self):
        """
        Commiting DB changes in current transaction

        """
        self._db_connection.commit()

    def rollback(self):
        """
        Rollback DB changes in current transaction

        """
        self._db_connection.rollback()

    def __del__(self):
        """
        Destructor: Destroys db connection

        """
        self._db_connection.close()


class DaytonaDBmon():
    """
    DBmon class implement routine that continuously query database for new tests in waiting queue. As soon as new test
    comes in the queue, this routine picks up this test creates directory in daytona file system for this test and put
    this test in DBmon queue. Later dispatch thread pick up this test for further execution

    """
    mon_thread = []
    lock = threading.Lock()
    tests_to_run = defaultdict(list)
    cfg = None

    def __init__(self, cfg, lctx):
        self.lctx = lctx
        self.db = DBAccess(cfg, lctx)
        self.cfg = cfg
        self.startMon()
        print "CFG in init :"
        print self.cfg.mysql_host

        common.createdir(cfg.daytona_dh_root, self.lctx)

        time.sleep(5)  # wait for 5 secs for all recs to be loaded into map

    def __del__(self):
        self.db.close()

    def mon(self, *args):
      #query all waiting state tests and load into to_schedule, this is for restart case (similarly for running)
      restarted = True
      print "CFG in mon :"
      print self.cfg.mysql_host
      while True:
        self.db = DBAccess(self.cfg, self.lctx)
	query_result = self.db.query("""select testid from CommonFrameworkSchedulerQueue where state = %s or state = %s""", ("waiting", "scheduled"), True, False)
        test_list = [item[0] for item in query_result]
        d = "DBMON [W] : |"
        for k in self.tests_to_run:
          l = self.tests_to_run[k]
          for t in l :
            if t.testobj.TestInputData.testid in test_list:
              d = d + str(t.testobj.TestInputData.testid) + "|"
            else:
              self.lock.acquire()
              self.tests_to_run[k].remove(t)
              self.lock.release()
        self.lctx.info(d)
        d = ""

        query_result = None

        if restarted == True :
          query_result = self.db.query("""select testid from CommonFrameworkSchedulerQueue where state = %s or state = %s or state = %s or state = %s or state = %s or state = %s""", ("scheduled","waiting","setup","running","completed","collating"), True, False);
          #query_result = self.db.query("""select testid from TestInputData where end_status = %s or end_status = %s or end_status = %s or end_status = %s or end_status = %s or end_status = %s""", ("scheduled","waiting","setup","running","completed","collating"), True, False);
          restarted = False
        else:
          status = "scheduled"
          query_result = self.db.query("""select testid from CommonFrameworkSchedulerQueue where state = %s""", (status,), True, False);
          #query_result = self.db.query("""select testid from TestInputData where end_status = %s""", (status,), True, False);

        #reset all states to scheduled, mostly required in a restart case
        #all items that make to the DBMON [Q] will be in scheduled state
        for testid in query_result:
          to = testobj.testDefn()
          to.testobj.TestInputData.testid = testid[0]
          res = to.construct(testid[0])
          res = to.updateStatus("*", "scheduled")

        d = "DBMON [Q] : "
        d = d + str(query_result)
        self.lctx.info(d)

        for testid in query_result:
          found = False
          for k in self.tests_to_run: #search across all FW
            l = self.tests_to_run[k] #list of tests ready to be run
            for t in l:
              self.lctx.debug(t.testobj.TestInputData.testid)
              self.lctx.debug(testid[0])
              self.lctx.debug(k)
              if t.testobj.TestInputData.testid == testid[0]:
                self.lctx.debug("Test already present in runQ")
                found = True

          if(found == False):
            to = testobj.testDefn()
            to.testobj.TestInputData.testid = testid[0]

            res = to.construct(testid[0])

            #create required dirs and setup server side env
            prefix=self.cfg.daytona_dh_root +"/"+ to.testobj.TestInputData.frameworkname + "/" + str(to.testobj.TestInputData.testid) + "/" + "results" + "/"
            shutil.rmtree(prefix, ignore_errors=True)

            to.testobj.TestInputData.exec_results_path = prefix+to.testobj.TestInputData.exechostname + "/"
            to.testobj.TestInputData.exec_path = prefix+to.testobj.TestInputData.exechostname
            to.testobj.TestInputData.exec_log_path = prefix+to.testobj.TestInputData.exechostname+"/application"

            to.testobj.TestInputData.stats_results_path = defaultdict()
            for s in to.testobj.TestInputData.stathostname.split(','):
              to.testobj.TestInputData.stats_results_path[s.strip()] = prefix+s.strip()+"/"

            to.testobj.TestInputData.stats_results_path[to.testobj.TestInputData.exechostname] = prefix+to.testobj.TestInputData.exechostname+"/"

            common.createdir(to.testobj.TestInputData.exec_results_path, self.lctx)
            common.createdir(to.testobj.TestInputData.exec_path, self.lctx)
            common.createdir(to.testobj.TestInputData.exec_log_path, self.lctx)
            for s in to.testobj.TestInputData.stats_results_path:
              common.createdir(to.testobj.TestInputData.stats_results_path[s], self.lctx)

            res = to.updateStatus("scheduled", "waiting")

            sz = to.serialize()
            t2 = testobj.testDefn()
            t2.deserialize(sz)

            if t2.testobj.TestInputData.testid != to.testobj.TestInputData.testid :
              self.lctx.error("error in ser / dser")
              break

            #use a lock here
            self.lock.acquire()
            self.tests_to_run[to.testobj.TestInputData.frameworkid].append(to)
            self.lctx.debug("Adding : " + str(to.testobj.TestInputData.testid))
            self.lock.release()

        self.db.close()

        if self.mon_thread[0].check() == False :
          return
        time.sleep(5)


    def startMon(self):
        mthread = common.FuncThread(self.mon, True)
        self.mon_thread.append(mthread)
        mthread.start()
