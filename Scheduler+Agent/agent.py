# This is main file for agent process, this file start agent process on the host. Agent process communicate with
# scheduler for test execution and SAR data collection

#!/usr/bin/env python
import threading
import SocketServer
import time
from collections import defaultdict


import server
import config
import common
import testobj
import system_metrics_gather
from logger import LOG

# Main function
if __name__ == "__main__":

    lctx = LOG.getLogger("agentlog", "Agent")
    cfg = config.CFG("Agent", lctx)
    cfg.readCFG("config.ini")

    HOST = common.get_local_ip()
    PORT = cfg.CPORT

    # Creating daytona agent dir for saving execution script and SAR log files
    common.createdir(cfg.daytona_agent_root, lctx)

    server.serv.role = "Agent"
    base_serv = server.serv()

    # Initiating TCP server for listening daytona message from scheduler
    ser = base_serv.ThreadedTCPServer((HOST, PORT), server.serv.ThreadedTCPRequestHandler)
    server.serv.serverInstance = ser
    ip, port = ser.server_address

    # Start a thread with the server -- that thread will then start one
    # more thread for each request
    server_thread = threading.Thread(target=ser.serve_forever)

    # Exit the server thread when the main thread terminates
    server_thread.daemon = True
    server_thread.start()

    # Start system metrics gather threads, this will start multiple threads for various SAR data collection
    system_metrics_gather.init_sar_iostat_top()

    # Setup test log directory
    common.createdir(cfg.daytona_agent_root, lctx)
    log_dir = cfg.daytona_agent_root + "test_logs/"
    common.createdir(log_dir, lctx)

    lctx.info("Server loop running in thread:" + server_thread.name)
    lctx.info("Server started @ %s:%s" % (ip, port))

    # list of current exec ASYNC jobs
    # print actc.async_actions

    while True:
        if server.serv.actc is not None:
            d = "ASYNC Jobs ["
            lctx.debug(server.serv.actc.async_actions)
            for pending in server.serv.actc.async_actions:
                (t1, actionID, tst, ts) = pending
                diff = time.time() - ts
                lctx.debug(str(tst.testobj.TestInputData.timeout))
                d = d + (str(tst.testobj.TestInputData.testid)+":"+str(diff)) + ","
            d = d + "]"
            lctx.info(d)
        time.sleep(2)

    server_thread.join()
