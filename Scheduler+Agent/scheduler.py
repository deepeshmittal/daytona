# This is main file for scheduler process which does all the setup for scheduler process. It start 4 threads for
# performing test execution and SAR data collection. It communicate with agent process of each host
# (execution or statistic host) mentioned in test details and send daytona commands to agent for executing test
# and stat collection. Thread details are as follows:
# 1. Database Monitor Thread
# 2. Dispatch Thread
# 3. Testmon Thread
# 4. Server Thread for listening agent messages


#!/usr/bin/env python
import threading
import socket
import time
from collections import defaultdict
import traceback
from dbcli_action import dbCliHandle
import pickle
import dbaccess
import server
import config
import client
import envelope
import common
import testobj
from logger import LOG
import csv
import process_files


class Scheduler:
    """
    Main scheduler class which implement all scheduler related procedures. Each function is explained later

    """
    scheduler_thread = None
    testmon_thread = None
    lock = threading.Lock()
    dispatchQ__lock = threading.Lock()
    running_tests = defaultdict()
    dispatch_queue = defaultdict()

    def __init__(self, db, cfg, lctx):
        """
        Scheduler class constructor which initialize class variables and other threads

        """
        self.dbinstance = db
        self.cfg = cfg
        self.testmap = db.tests_to_run
        self.cl = client.TCPClient(LOG.getLogger("clientlog", "DH"))
        self.ev = envelope.DaytonaEnvelope()
        self.HOST = common.get_local_ip()
        self.PORT = cfg.DHPORT
        self.CPORT = cfg.CPORT

        self.scheduler_thread = common.FuncThread(self.dispatch, True)
        self.testmon_thread = common.FuncThread(self.testmon, True)
        self.lctx = lctx

    def process_results(self, *args):
        """
        This procedure is called by testmon as seperate thread when test execution ends or test timeout occur
        on agent.

        """
        t = args[1]
        status = args[2]

        serialize_str = t.serialize();
        t2 = testobj.testDefn()
        t2.deserialize(serialize_str)

	# Setting up test logger for capturing test life cycle on scheduler
	test_logger = LOG.gettestlogger(t2, "EXEC")
        test_logger.info("Test execution completes, preocessing test results")

        try:
            if t.testobj.TestInputData.testid != t2.testobj.TestInputData.testid:
                lctx.error("testobj not same")
                raise Exception("Test objects do not match : ", t2.testobj.TestInputData.testid)

	    # set test status to collating
            if t.testobj.TestInputData.timeout_flag:
                t.updateStatus("timeout", "collating")
            else:
                t.updateStatus("completed", "collating")

            ip = t.testobj.TestInputData.exechostname
            lctx.debug(status)
            if status in ["completed", "timeout"]:

		# Initiate instance of ProcessOutputFiles for docker and top outout file processing
                ptop = process_files.ProcessOutputFiles(LOG.getLogger("processTop", "DH"))

                lctx.info("SENDING results.tgz download to : " + ip + ":" + str(self.CPORT))
		# send file download command to exec host (no need to send stop test as this procedure is invoked due
                # to test end on exec host)
                retsend = self.cl.send(ip, self.CPORT,
                                       self.ev.construct("DAYTONA_FILE_DOWNLOAD", str(t2.testobj.TestInputData.testid)))
                lctx.debug(retsend)
                if retsend.split(",")[1] != "SUCCESS":
                    lctx.error("Error downloading LOGS from " + ip + " : " + retsend)
                    test_logger.error("Error downloading LOGS from " + ip + " : " + retsend)
                else:
                    test_logger.info("Logs download successfull from exec host " + ip)

		# copy results files from exec to daytona file system and untar results
                try:
                    lctx.debug(
                        "Untar file : " + t2.testobj.TestInputData.exec_results_path + "results.tgz to location : " + 
			t2.testobj.TestInputData.exec_results_path + "/../")
                    common.untarfile(t2.testobj.TestInputData.exec_results_path + "/results.tgz",
                                     t2.testobj.TestInputData.exec_results_path + "/../")
                except Exception as e:
                    lctx.error("Error in untar EXEC host results")
                    test_logger.error("Error in untar EXEC host results")
                    lctx.error(e)

		# process top and docker stat files downloaded from exec host
		ptop_ret = ptop.process_output_files(t2.testobj.TestInputData.stats_results_path[ip] + "sar/")
                lctx.debug(ptop_ret + " : " + t2.testobj.TestInputData.stats_results_path[ip])
                test_logger.info("Exec host logs extracted and processed succesfully")

		# send DAYTONA_FINISH_TEST to exec host for finishing and test cleanup
                retsend = self.cl.send(ip, self.CPORT,
                                       self.ev.construct("DAYTONA_FINISH_TEST", str(t2.testobj.TestInputData.testid)))
                lctx.debug(retsend)
                test_logger.info("Test END successfull on exec host " + ip)

                for s in t.testobj.TestInputData.stathostname.split(','):
                    if len(s.strip()) == 0:
                        break

                    # stop stats monitors on req hosts
                    # any host that blocks stop monitor blocks the scheduling for the FW

                    p = self.CPORT
                    try:
			# Send DAYTONA_STOP_TEST on all agent hosts to stop SAR data collection after test finish on
                        # exec host. This message is required to tell stat hosts that test execution is finished on
                        # exec host. Upon receiving this message on stat host, agent will change test state to TESTEND
                        # and then other SAR data collection thread will stop writing log files for this test.
			lctx.info("Stopping test on stat host : " + s)
                        retsend = self.cl.send(s.strip(), p, self.ev.construct("DAYTONA_STOP_TEST",
                                                                               str(t2.testobj.TestInputData.testid)))
                        lctx.debug(retsend)
                        if retsend.split(",")[1] != "SUCCESS":
                            lctx.error("Failed to stop test on stat host " + s + " : " + retsend)
                            test_logger.error("Failed to stop test on stat host " + s + " : " + retsend)
                        else:
                            test_logger.info("Test stopped on stat host " + s)

			# send file download command to stat host
                        lctx.info("Sending results.tgz download to :" + s.strip() + ":" + str(p))
                        retsend = self.cl.send(s.strip(), p, self.ev.construct("DAYTONA_FILE_DOWNLOAD",
                                                                               str(t2.testobj.TestInputData.testid)))
                        lctx.debug(retsend)
                        if retsend.split(",")[1] != "SUCCESS":
                            lctx.error("Error downloading STATS from " + s.strip() + ":" + retsend)
                            test_logger.error("Error downloading STATS from " + s.strip() + ":" + retsend)
                        else:
                            test_logger.info("Logs downloaded from stat host " + s)

			# copy results files from stat host to daytona file system and untar results
                        lctx.debug("Untar file : " + t2.testobj.TestInputData.stats_results_path[
                            s] + "results.tgz to location : " + t2.testobj.TestInputData.stats_results_path[s] + "/../")
                        common.untarfile(t2.testobj.TestInputData.stats_results_path[s] + "/results.tgz",
                                         t2.testobj.TestInputData.stats_results_path[s] + "/../")

			# process top and docker stat files downloaded from stat host
			ptop_ret = ptop.process_output_files(t2.testobj.TestInputData.stats_results_path[s] + "sar/")
                        lctx.debug(ptop_ret + " : " + t2.testobj.TestInputData.stats_results_path[s])
                        test_logger.info("Stat host " + s + " logs extracted and processed succesfully")

			# send DAYTONA_FINISH_TEST to exec host for finishing and test cleanup
                        retsend = self.cl.send(s.strip(), p, self.ev.construct("DAYTONA_FINISH_TEST",
                                                                               str(t2.testobj.TestInputData.testid)))
                        lctx.debug(retsend)
                        test_logger.info("Test END successfull on stat host " + s)
                    except Exception as e:
			# Just continue with other stat hosts if any exception occurs while working on any particular
                        # host (Continue only when something goes wrong with stat host, because we still want to
                        # download logs from other stat hosts)
                        lctx.error(e)
                        test_logger.error(e)
                        continue

        except Exception as e:
	    # Throw an error if anything goes wrong with finishing test on exec host and set test state to failed
            lctx.error("Error in processing results")
            lctx.error(e)
            test_logger.error("Error in processing results")
            test_logger.error(e)
            t.updateStatus("collating", "failed")

        # updating test state to timeout clean if test terminated due to timeout else setting it to finished clean
        if t.testobj.TestInputData.timeout_flag:
            t.updateStatus("collating", "timeout clean")
        else:
            t.updateStatus("collating", "finished clean")

        now = time.time()
        tstr = str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)))
        # update test end time in database
        t.updateEndTime(tstr)
        f = None

        # Formatting email with results.csv details to send it to CC list if user has mentioned in test details
        # (admin need to smtp server for this functionality to work, smtp server details need to be
        # mentioned in config.sh)
        try:
            f = open(t2.testobj.TestInputData.exec_results_path + "/results.csv")
        except IOError as e:
            lctx.debug("File results.csv not found")
            pass

        to = t.testobj.TestInputData.email
        htmlfile = '<table>'
        if f:
            reader = csv.reader(f)
            rownum = 0
            for row in reader:
                if rownum == 0:
                    htmlfile += '<tr>'
                    for column in row:
                        htmlfile += '<th style="text-align: left;" width="70%">' + column + '</th>'
                    htmlfile += '</tr>'
                else:
                    htmlfile += '<tr>'
                    for column in row:
                        htmlfile += '<td style="text-align: left;" width="70%">' + column + '</td>'
                    htmlfile += '</tr>'
                rownum += 1
            f.close()

        htmlfile += '</table>'
        host_ip = "http://" + common.get_local_ip() + "/test_info.php?testid=" + str(t.testobj.TestInputData.testid)

        subject = "Test {} completed successfully".format(t.testobj.TestInputData.testid)

        mail_content = "<BR> Test id : {} <BR> Framework : {} <BR> Title : {} <BR>".format(
            t.testobj.TestInputData.testid, t.testobj.TestInputData.frameworkname, t.testobj.TestInputData.title)

        mail_content = mail_content + "<BR>==========================================================<BR>"
        mail_content = mail_content + "<BR>Purpose : {} <BR><BR> Creation time : {} <BR>Start time : {} <BR>End " \
                                      "time : {} <BR>".format(t.testobj.TestInputData.purpose, 
                                                              t.testobj.TestInputData.creation_time, 
                                                              t.testobj.TestInputData.start_time, 
                                                              t.testobj.TestInputData.end_time)
        mail_content = mail_content + "<BR>Your test executed successfully. <BR>Results (Contents of results.csv)<BR>"
        mail_content = mail_content + "<BR>==========================================================<BR>"
        mail_content = mail_content + "<BR>" + htmlfile + "<BR>"
        mail_content = mail_content + "<BR>==========================================================<BR>"
        mail_content = mail_content + "Link:"
        mail_content = mail_content + '<BR><a href="' + host_ip + '">' + host_ip + '</a>'

        try:
            common.send_email(subject, to, mail_content, "", lctx, cfg.email_user, cfg.email_server, cfg.smtp_server,
                              cfg.smtp_port)
        except Exception as e:
            lctx.error("Mail send error")

	LOG.removeLogger(t)
        return "SUCCESS"


    def trigger(self, *args):
	"""
        trigger starts in a thread, keep track of all triggers and they should complete in a specified time, otherwise
        signal a close. triggers are used for test setup, then starting test and then taking test into running
        state on agent.

        """
        t = args[1]

        serialize_str = t.serialize();
        t2 = testobj.testDefn()
        t2.deserialize(serialize_str)
        time.sleep(6)

	# Setting up test logger for capturing test life cycle on scheduler
	test_logger = LOG.gettestlogger(t2, "EXEC")
	test_logger.info("Test setup started")

        try:
            if t.testobj.TestInputData.testid != t2.testobj.TestInputData.testid:
                lctx.error("testobj not same")
                raise Exception("test trigger error", t2.testobj.TestInputData.testid)

	    # Sending DAYTONA_SETUP_TEST command on execution host, on receiving this command agent will perform basic
	    # test setup by creating test directories for saving log files and it will copy execution script in test
            # directory for starting execution
            ip = t.testobj.TestInputData.exechostname
            retsend = self.cl.send(ip, self.CPORT, self.ev.construct("DAYTONA_SETUP_TEST", serialize_str + ",EXEC"))
            lctx.debug(retsend)

            if retsend and len(retsend.split(",")) > 1:
                if retsend.split(",")[1] != "SUCCESS":
                    lctx.error(retsend)
                    raise Exception("test trigger error", t2.testobj.TestInputData.testid)
            else:
                raise Exception("Test Setup Failure : Test -  ", t2.testobj.TestInputData.testid)
	    
	    test_logger.info("Test setup complete on exec host " + ip)

	    # Triggering test setup on all stat hosts
            for s in t.testobj.TestInputData.stathostname.split(','):
                if len(s.strip()) == 0:
                    break
		test_logger.info("Starting test setup on stat host " + s)
                lctx.debug(s.strip())
                p = self.CPORT

		# Sending hearbeat message to check whether stat host is up or not
                retsend = self.cl.send(s, p, self.ev.construct("DAYTONA_HEARTBEAT", ""))

                if retsend and len(retsend.split(",")) > 1:
                    if retsend.split(",")[1] != "ALIVE":
                        raise Exception("Remove host not avaliable - No Heartbeat ", t2.testobj.TestInputData.testid)
                    else:
			test_logger.info("Hearbeat received from stat host " + s)
			# Trigger DAYTONA_HANDSHAKE to verify that both agent and scheduler are able to communicate with
                        # each other on custom daytona ports
                        retsend = self.cl.send(s, p,
                                           self.ev.construct("DAYTONA_HANDSHAKE", "handshake1," + self.HOST + "," + str(
                                               self.PORT) + "," + str(t2.testobj.TestInputData.testid) + "," + s))
                        lctx.debug(retsend)
                        if retsend == "SUCCESS":
                            alive = True
                            server.serv.registered_hosts[s] = s
                            addr = socket.gethostbyname(s)
                            server.serv.registered_hosts[addr] = addr
			    test_logger.info("Handshake successfull with agent on stat host " + s)	
                        else:
                            raise Exception("Unable to handshake with agent on stats host:" + s,
                                            t2.testobj.TestInputData.testid)
		else:
                    raise Exception("Stat host " + s +  " not avaliable - No Heartbeat")

		# Trigger test setup on stat hosts, this will create test directory for saving log files
		retsend = self.cl.send(s.strip(), p, self.ev.construct("DAYTONA_SETUP_TEST", serialize_str + ",STAT"))
                lctx.debug(retsend)

                if retsend and len(retsend.split(",")) > 1:
                    if retsend.split(",")[1] != "SUCCESS":
                        lctx.error(retsend)
                        raise Exception("test trigger error", t2.testobj.TestInputData.testid)
                else:
                    raise Exception("Test Setup Failure : Test -  ", t2.testobj.TestInputData.testid)
		
		test_logger.info("Test setup complete on stat host " + s)

            # Trigger the start of test on exec host
            retsend = self.cl.send(ip, self.CPORT,
                                   self.ev.construct("DAYTONA_START_TEST", str(t2.testobj.TestInputData.testid) + ",EXEC"))
            lctx.debug(retsend)
            if retsend and len(retsend.split(",")) > 1:
                if retsend.split(",")[1] != "SUCCESS":
                    lctx.error(retsend)
                    raise Exception("test trigger error", t2.testobj.TestInputData.testid)
	    else:
                raise Exception("Failed to start Test : ", t2.testobj.TestInputData.testid)
	    
	    test_logger.info("Test started on exec host " + ip)

            # Trigger the start of test on STAT hosts (This is for initiating system metric data collection)
            for s in t.testobj.TestInputData.stathostname.split(','):
                if len(s.strip()) == 0:
                    break
                p = self.CPORT
                s = s.strip()
                retsend = self.cl.send(s, p,
                                       self.ev.construct("DAYTONA_START_TEST",
                                                         str(t2.testobj.TestInputData.testid) + ",STAT"))
                lctx.debug(retsend)
                if retsend and len(retsend.split(",")) > 1:
                    if retsend.split(",")[1] != "SUCCESS":
                        lctx.error(retsend)
                        raise Exception("Failed to start test on STAT host : ", s)
                else:
                    raise Exception("Failed to start test on STAT host : ", s)

		test_logger.info("Test started on stat host " + s)

            # Get status from exec host
            retsend = self.cl.send(ip, self.CPORT,
                                   self.ev.construct("DAYTONA_GET_STATUS", str(t2.testobj.TestInputData.testid)))

            if retsend and len(retsend.split(",")) > 1:
                if "RUNNING" == retsend.split(",")[1] or "MONITOR_ON" == retsend.split(",")[1]:
                    # update from setup to running
                    lctx.debug("Updating test status to running in DB")
                    t.updateStatus("setup", "running")
                    now = time.time()
                    tstr = str(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(now)))
                    t.updateStartTime(tstr)
		    self.dispatchQ__lock.acquire()
                    del self.dispatch_queue[t.testobj.TestInputData.frameworkid]
                    self.dispatchQ__lock.release()
		    self.lock.acquire()
                    self.running_tests[t.testobj.TestInputData.frameworkid] = t
                    self.lock.release()
                else:
                    lctx.error("Unable to determine status, testmon will garbage collect this testid")
                    # Garbage collect testid in runnning state that fails to give status
                    # Killing of threads on client host and remove from list with status=fail is done in testmon

        except Exception as e:
	    # If any trigger fails, then abort test startup with error
	    lctx.error(e)
	    test_logger.error(e)
            lctx.error("ERROR : Unknown trigger error : " + str(t.testobj.TestInputData.testid))
            t.updateStatus("setup", "failed")
            lctx.debug(traceback.print_exc())
            lctx.error("Removing Test " + str(t.testobj.TestInputData.testid) + " from dispatch Q")
            self.dispatchQ__lock.acquire()
            del self.dispatch_queue[t.testobj.TestInputData.frameworkid]
            self.dispatchQ__lock.release()

	    # This will abort test and perform cleanup on exec host with trigger was successful on exec host
            retsend = self.cl.send(ip, self.CPORT,
                                   self.ev.construct("DAYTONA_ABORT_TEST", str(t2.testobj.TestInputData.testid)))
            lctx.debug(retsend)
	    test_logger.info("Test aborted on exec host " + ip)

	    # On all other stat hosts we send cleanup in case trigger was successful on any stat host
            for s in t.testobj.TestInputData.stathostname.split(','):
                if len(s.strip()) == 0:
                    break
		retsend = self.cl.send(s.strip(), self.CPORT,
                                       self.ev.construct("DAYTONA_CLEANUP_TEST", str(t2.testobj.TestInputData.testid)))
                lctx.debug(retsend)
		test_logger.info("Test abort on stat host " + s)

	    LOG.removeLogger(t2)
            return "FAILED"

        return "SUCCESS"

    def __del__(self):
        self.testmon_thread.stop()
        self.scheduler_thread.stop()

    def dispatch(self, *args):
        """
        This is dispatch queue of scheduler where test from different framework wait in the waiting queue for scheduler to
        bind it with trigger thread. This procedure continuously iterate over testmap populated by DBMon with tests
        started by user from UI or CLI. This keep track of all running tests and it allows one test per framework. Once
        this procedure find an open test spot for a test from particular framework, this procedure will pop it from testmap,
        put it in dispatch queue and assign trigger thread for this test to start test setup and then execution.

        """
        dispatch_threads = defaultdict()
        while True:
	    # Continuously iterate on testmap for initiating any test execution
            for k in self.testmap:
		# iterating for all frameworkid k in testmap which contains list of waiting tests for a particular framework
                found = False
		
		# If test for a particular framework is already in running or dispatch queue then this new test need to
		# wait until previous test gets finish, hence we do nothing and just continue
		if k in self.dispatch_queue or k in self.running_tests:
                    found = True
                else:
                    lctx.debug("Found spot for test")

                if found:
                    continue

		# Proceed if spot is available for executing test for this framework
                try:
                    tmp_t = self.testmap[k][0]
                except Exception as e:
                    lctx.debug("No test object found in map")
                    continue

                if tmp_t is None:
                    continue

                alive = False

                h = tmp_t.testobj.TestInputData.exechostname

		# Initiating test logger for capturing test life cycle on scheduler, all logs are logged in file <testid>.log
		test_logger = LOG.init_testlogger(tmp_t, "EXEC")
		test_logger.info("Test execution starts")
                try:
		    # Sending heartbeat on exec host to check if it agent is up on exec host
                    retsend = self.cl.send(h, self.CPORT, self.ev.construct("DAYTONA_HEARTBEAT", ""))

                    if retsend and len(retsend.split(",")) > 2:
                        status = retsend.split(",")[1]
                    else:
                        raise Exception("Execution host not avaliable - No Heartbeat ", tmp_t.testobj.TestInputData.testid)

                    if "ALIVE" == status:
			test_logger.info("HeartBeat received from execution host " + h)
			# Sending DAYTONA_HANDSHAKE for verifying connectivity between scheduler and agent on exec host
			# using custom daytona ports
			ret = self.cl.send(h, self.CPORT,
                                           self.ev.construct("DAYTONA_HANDSHAKE",
                                                             "handshake1," + self.HOST + "," + str(self.PORT) + "," + str(
                                                                 tmp_t.testobj.TestInputData.testid) + "," + h))
                        if ret == "SUCCESS":
                            alive = True
			    test_logger.info("Handshake successful with execution host " + h)
                            lctx.debug("Handshake successful in scheduler, adding ip/hostname to reg hosts")
                            server.serv.registered_hosts[h] = h
                            addr = socket.gethostbyname(h)
                            lctx.debug(addr)
                            server.serv.registered_hosts[addr] = addr
                        else:
                            raise Exception("Unable to handshake with agent on executuion host " + h)

		except Exception as e:
                    lctx.error(e)
		    test_logger.error(e)
                    # pause the dbmon here as we dont want the same test to be picked again after we pop
                    self.dbinstance.mon_thread[0].pause()
                    self.dbinstance.lock.acquire()
                    t = self.testmap[k].pop(0)
                    t.updateStatus("waiting", "failed")
                    self.dbinstance.lock.release()
                    lctx.debug("Removed test from map : " + str(t.testobj.TestInputData.testid))
                    self.dbinstance.mon_thread[0].resume()
		    LOG.removeLogger(tmp_t)
                    continue

                if alive == True and found == False:
                    # for each framework pick one and move it to running, iff running has an empty slot.
                    lctx.debug("-------Found empty slot in dispatch and running Q-------")

                    # pause the dbmon here as we dont want the same test to be picked again after we pop
                    self.dbinstance.mon_thread[0].pause()
                    self.dbinstance.lock.acquire()
                    t = self.testmap[k].pop(0)
                    self.dbinstance.lock.release()

                    lctx.info("< %s" % t.testobj.TestInputData.testid)

		    # put the test in dispatch queue
		    self.dispatchQ__lock.acquire()
                    self.dispatch_queue[k] = t
                    self.dispatchQ__lock.release()

                    t.updateStatus("waiting", "setup")
                    self.dbinstance.mon_thread[0].resume()

                    try:
			# Bind a seperate trigger thread for this test to start test execution
                        trigger_thread = common.FuncThread(self.trigger, True, t)
                        dispatch_threads[t.testobj.TestInputData.testid] = (trigger_thread, t)
                        trigger_thread.start()
                    except Exception as e:
                        lctx.error("Trigger error : " + str(t.testobj.TestInputData.testid))
			test_logger.error("Test setup failed " + str(t.testobj.TestInputData.testid))
			LOG.removeLogger(tmp_t)
			self.dispatchQ__lock.acquire()
                        del self.dispatch_queue[k]
                        self.dispatchQ__lock.release()
                        lctx.debug(e)

	    try:
		# Log list of test currently present in dispatch queue in scheduler debug file
                d = "DISPATCH [S/R] : "
                for k in self.dispatch_queue:
                    d = d + " |" + str(self.dispatch_queue[k].testobj.TestInputData.testid)
            except:
                lctx.debug("ERROR : Dispatch Q empty")

            lctx.debug(d)
            d = ""

            time.sleep(2)


    def testmon(self, *mon):
	"""
	Testmon continuously monitors all the running test. It keeps on checking test status on a exec host where execution
	script is running. If anything goes wrong with the test execution, this thread trigger termination actions for
	this test. It also trigger graceful test termination and logs collection when test finishes on exec host

	"""
        process_results_threads = defaultdict()
        while True:
            d = "TSMON [R] : |"
            remove = False
            error = False

	    # Continuously iterate over running test list for checking test status
            for k in self.running_tests:
                if self.running_tests[k] is not None:
                    t = self.running_tests[k]

                    serialize_str = t.serialize()
                    t2 = testobj.testDefn()
                    t2.deserialize(serialize_str)

		    # Initiating test logger for capturing test life cycle on scheduler, all logs are logged in
                    # file <testid>.log
		    test_logger = LOG.gettestlogger(t2, "EXEC")
                    if t.testobj.TestInputData.testid != t2.testobj.TestInputData.testid:
                        lctx.error("testobj not same")
                        t.updateStatus("running", "failed")
                        remove = True
                        break  # out of for loop

                    try:
			# Send DAYTONA_GET_STATUS message on exec host mentioned in test for checking test status
                        ret = self.cl.send(t.testobj.TestInputData.exechostname, self.CPORT,
                                           self.ev.construct("DAYTONA_GET_STATUS",
                                                             str(t2.testobj.TestInputData.testid)))
                        status = ret.split(",")[1]
                        lctx.debug(status)
			test_logger.info("Test status : " + status)
                    except Exception as e:
                        lctx.debug(e)
                        t.updateStatus("running", "failed")
                        error = True
                        break  # out of for loop

                    if status == "RUNNING":
			# If the test is in running state, then we need to verify that user hasn't terminated this
                        # test from UI. If user has terminated then testmon will stop test execution on all exec host
                        # and stat host
                        found = checkTestRunning(t.testobj.TestInputData.testid)
                        if not found:
                            error = True
                            break
                        d = d + str(self.running_tests[k].testobj.TestInputData.testid) + "|"
                    elif status in ["TESTEND", "TIMEOUT"]:
			# If test ends on exec host or if test timout occurs then trigger graceful shutdown of this test
                        # Testmon invoke a new thread for this test for logs download and test cleanup from all hosts
                        d = d + "*" + str(self.running_tests[k].testobj.TestInputData.testid) + "*|"
                        if t.testobj.TestInputData.end_status == "running":
                            lctx.debug(t.testobj.TestInputData.end_status)
                            if status == "TIMEOUT":
                                t.testobj.TestInputData.timeout_flag = True
                                t.updateStatus("running", "timeout")
                            else:
                                t.updateStatus("running", "completed")

			    # process_results download log files and perform cleanup on all other hosts
                            pt = common.FuncThread(self.process_results, True, t,
                                                   t.testobj.TestInputData.end_status)
                            process_results_threads[t.testobj.TestInputData.testid] = (pt, t)
                            pt.start()
                            remove = True
                            break
                        elif t.testobj.TestInputData.end_status == "collating" or t.testobj.TestInputData.end_status == "completed" or t.testobj.TestInputData.end_status == "finished clean":
                            d = d + "*" + str(self.running_tests[k].testobj.TestInputData.testid) + "*|"
                        else:
                            remove = True
                            t.updateStatus("running", "failed")
                            lctx.error(
                                "ERROR : Unknown test status for : " + str(t.testobj.TestInputData.testid) + ":" + str(
                                    status))
                            break  # out of for loop
                            
                    elif status.strip() in ["FAILED", "TESTNA"]:
			# Test termination if test fails or test is not even running on the host
                        if status.strip() == "FAILED":
                            error = True
                        elif status.strip() in ["ABORT", "TESTNA"]:
                            remove = True
                        t.updateStatus("", "failed")
                        lctx.error("TEST " + status.strip() + " : Cleaning test from running queue")
                        break  # out of for loop
                    else:
			# Test termination on receiving any unknown test state
                        remove = True
                        t.updateStatus("running", "failed")
                        lctx.error(
                            "ERROR : Unknown test status for : " + str(t.testobj.TestInputData.testid) + ":" + str(
                                status))
                        break  # out of for loop

                lctx.info(d)
                d = ""

	    # Two modes of test termination:
            if error:
		# If error is set then testmon will perform below steps:
                # 1. Send test ABORT on exec host if is alive, this will stop execution script, perform logs cleanup and
                #    test termination on the host
                # 2. Send test cleanup on all other stat host for performing logs cleanup and test termination on the host
                # 3. Remove test from the scheduler running queue

                retsend = None
		test_logger.error("Bad test status " + status + " - Terminating test")
                ip = t.testobj.TestInputData.exechostname
                try:
                    retsend = self.cl.send(ip, self.CPORT, self.ev.construct("DAYTONA_HEARTBEAT", ""))
                except:
                    pass

                if retsend and retsend.split(",")[1] == "ALIVE":
                    retsend = self.cl.send(ip, self.CPORT,
                                           self.ev.construct("DAYTONA_ABORT_TEST", str(t.testobj.TestInputData.testid)))
		    
		    test_logger.error("Test Aborted on exec host " + ip)
                
		for s in t.testobj.TestInputData.stathostname.split(','):
                    if len(s.strip()) == 0:
                        break
                    try:
                        retsend = self.cl.send(s.strip(), self.CPORT, self.ev.construct("DAYTONA_HEARTBEAT", ""))
                    except:
                        pass
                    if retsend and retsend.split(",")[1] == "ALIVE":
                        retsend = self.cl.send(s.strip(), self.CPORT,
                                               self.ev.construct("DAYTONA_CLEANUP_TEST",
                                                                 str(t.testobj.TestInputData.testid)))
			test_logger.error("Test Aborted on stat host " + s)

                self.lock.acquire()
                for k in self.running_tests:
                    if self.running_tests[k].testobj.TestInputData.testid == t.testobj.TestInputData.testid:
                        lctx.debug("removing entry for this test")
                        rt = self.running_tests.pop(k)
                        break
                if k in self.running_tests:
                    del self.running_tests[k]
                self.lock.release()

            if remove:
		# If remove flag is set, then testmon will only delete this test from the running queue of scheduler
                self.lock.acquire()
                for k in self.running_tests:
                    if self.running_tests[k].testobj.TestInputData.testid == t.testobj.TestInputData.testid:
                        lctx.debug("removing entry for this test")
                        rt = self.running_tests.pop(k)
                        break
                if k in self.running_tests:
                    del self.running_tests[k]
                self.lock.release()

            time.sleep(2)


def daytonaCli(self, *args):
    """
    This is daytona CLI command handler when scheduler receives any request from Daytona cli script to perform
    any task. Daytona cli provide ability to get framework definition, add test for particular framework, run test,
    update test details and fetch test results. User need to provide daytona credentials in order to execute commands
    using daytona CLI scripts

    """
    (obj, command, params, actionID, sync) = (args[0], args[1], args[2], args[3], args[4])
    lctx = LOG.getLogger("scheduler-clilog", "DH")

    cli_param_map = pickle.loads(params)
    if len(cli_param_map) != 3:
        return "Error|Not enough arguments"

    # Retriveing username and password from CLI command for authentication
    user = cli_param_map['user']
    password = cli_param_map['password']

    # Retriveing actual daytona command and associated parameter from CLI command
    cli_command = cli_param_map['param'].split("|")[0]
    cli_param = cli_param_map['param'].split("|")[1]

    lctx.debug("Host received CLI command : " + cli_command)

    db = dbCliHandle()

    # Calling authenticate_user webservice for verifying username and password combination
    auth = db.authenticate_user(user, password)
    if auth != "SUCCESS":
        return auth

    # Invoke function based on CLI action mentioned on CLI command
    if cli_command == "get_frameworkid_arg":
        arglist = db.getFrameworkIdArgs(cli_param)
        return arglist
    elif cli_command == "get_framework_arg":
        arglist = db.getFrameworkArgs(cli_param)
        return arglist
    elif cli_command == "add_test":
        res = db.addTest(cli_param, user)
        return res
    elif cli_command == "add_run_test":
        res = db.addTest(cli_param, user, 'scheduled')
        if res:
            if res.split("|")[0] == 'Error':
                return res
            else:
                testid = res.split("|")[1]
        else:
            return "Error|Test add failed"

        res = db.runTest(testid, user)
        if res.split("|")[0] == 'SUCCESS':
            return "SUCCESS|" + testid
        else:
            return res
    elif cli_command == "run_test":
        res = db.runTest(cli_param, user)
        return res
    elif cli_command == "get_result":
        res = db.getResult(cli_param, user)
        return res
    elif cli_command == "get_test_by_id":
        res = db.getTestByID(cli_param)
        return res
    elif cli_command == "update_test":
        res = db.updateTest(cli_param, user)
        return res
    elif cli_command == "update_run_test":
        res = db.updateTest(cli_param, user, 'scheduled')
        if res:
            if res.split("|")[0] == 'Error':
                return res
            else:
                testid = res.split("|")[1]
        else:
            return "Error|Test update failed"
        res = db.runTest(testid, user)
        if res.split("|")[0] == 'SUCCESS':
            return "SUCCESS|" + testid
        else:
            return res
    else:
        return "Error|Unknown command received on server"


def checkTestRunning(testid):
    """
    This function checks whether user has not initiated test termination from UI. CommonFrameworkSchedulerQueue table
    keep the list of all running test initiated by user from UI or CLI. When user terminate any running test from UI,
    daytona removes the entry of this test from CommonFrameworkSchedulerQueue table. This functions polls database to
    check if test is still present in the CommonFrameworkSchedulerQueue table

    """
    lctx = LOG.getLogger("dblog", "DH")
    cfg = config.CFG("DaytonaHost", lctx)
    cfg.readCFG("config.ini")
    db = dbaccess.DBAccess(cfg, LOG.getLogger("dblog", "DH"))
    check = db.query("""SELECT COUNT(*) FROM CommonFrameworkSchedulerQueue where testid=%s""", (testid,), False, False)
    db.close()
    if check[0] == 0:
        return False
    else:
        return True


if __name__ == "__main__":
    # Port 0 means to select an arbitrary unused port

    lctx = LOG.getLogger("schedulerlog", "DH")
    cfg = config.CFG("DaytonaHost", lctx)
    cfg.readCFG("config.ini")

    common.logger.ROLE = "DHOST"
    # Start DBmon thread for polling DB to check test initiated by user
    db = dbaccess.DaytonaDBmon(cfg, LOG.getLogger("dblog", "DH"))

    # Instantiating Scheduler object which intiate variables used by scheduler process
    sch = Scheduler(db, cfg, LOG.getLogger("schedulerlog", "DH"))

    # TCP server setup for listening daytona messages from agent on scheduler port
    server.serv.role = "DH"
    ase_serv = server.serv()
    server.serv.lctx = LOG.getLogger("listenerlog", "DH")
    ser = server.serv.ThreadedTCPServer((common.get_local_ip(), sch.PORT), server.serv.ThreadedTCPRequestHandler)
    server.serv.serverInstance = ser
    ip, port = ser.server_address
    server.lctx = LOG.getLogger("listenerlog", "DH")

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=ser.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()
    lctx.info("Server loop running in thread:" + server_thread.name)
    lctx.info("Server started @ %s:%s" % (ip, port))

    # Starting dispatch thread which serialize test execution
    sch.scheduler_thread.start()
    time.sleep(5)  # wait for 5 secs to dispatch DS to be loaded
    # Starting test mon which keep track of all running tests
    sch.testmon_thread.start()

    server_thread.join()
    lctx.info("Server thread ended")
    lctx.info("DB Mon thread ended")
    lctx.info("Schedule thread ended")
