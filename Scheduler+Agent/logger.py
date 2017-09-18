# Logger file implement routines that provide logger object to agent/scheduler for writing logs in debug files or
# test specific log files

#!/usr/bin/env python
import logging
import logging.handlers
import config
import os


class Singleton(type):
    _instances = {}

    def __call__(self, cls, *args, **kwargs):
        if cls not in cls._instances.keys():
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class LOG(object):
    __metaclass__ = Singleton

    _loggers = {}

    def __init__(self, *args, **kwargs):
        pass

    @staticmethod
    def getLogger(name=None, role=""):
        """
        Create or return logger object to a rotating debug file for both scheduler and agent

        """
        name = name + role
        if name not in LOG._loggers.keys():
            log_filename = role + '_logging_rotatingfile.log'
            logging.basicConfig(level=logging.DEBUG,
                                format='%(asctime)s %(threadName)-12s %(name)-16s %(levelname)-6s {%(filename)-12s '
                                       '%(lineno)-4d}  %(message)-100s',
                                datefmt='%m-%d %H:%M',
                                filename=log_filename,
                                filemode='w')

            # Add the log message handler to the logger
            handler = logging.handlers.RotatingFileHandler(
                log_filename, maxBytes=1073741824, backupCount=2)

            console = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s %(threadName)-14s %(name)-19s %(message)s')
            console.setLevel(logging.INFO)
            console.setFormatter(formatter)

            formatter = logging.Formatter('%(asctime)s %(levelname)-s : %(message)-100s', '%m-%d %H:%M')
            LOG._loggers[name] = logging.getLogger(str(name))
            LOG._loggers[name].setLevel(logging.DEBUG)
            LOG._loggers[name].addHandler(handler)
            handler.setFormatter(formatter)
            LOG._loggers[name].addHandler(console)

        return LOG._loggers[name]

    @staticmethod
    def removeLogger(test):
        """
        Remove logger from the map "LOG" which maintain logger object for each test

        """
        if test.testobj.TestInputData.testid in LOG._loggers.keys():
            del LOG._loggers[test.testobj.TestInputData.testid]

    @staticmethod
    def init_testlogger(test, hosttype):
        """
        Initialize new test logger for particular test, used by both agent and scheduler for writing test life cycle logs

        """
        if hosttype == "EXEC":
            if test.testobj.TestInputData.testid in LOG._loggers.keys():
                del LOG._loggers[test.testobj.TestInputData.testid]

            test_logger = "test_logger" + str(test.testobj.TestInputData.testid)
            log_file = test.testobj.TestInputData.exec_results_path + str(test.testobj.TestInputData.testid) + ".log"
        else:
            if test.testid in LOG._loggers.keys():
                del LOG._loggers[test.testid]

            cfg = config.CFG("DaytonaHost", None)
            cfg.readCFG("config.ini")

            test_logger = "test_logger" + str(test.testid)
            log_file = cfg.agent_test_logs + test.stathostip + "_" + str(test.testid) + ".log"

        try:
            os.remove(log_file)
        except OSError:
            pass

        logger = logging.getLogger(test_logger)
        if logger.handlers:
            logger.handlers.pop()

        fh = logging.FileHandler(log_file)
        fh.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s %(levelname)-6s %(message)-100s',
                                      '%Y-%m-%d %H:%M')
        fh.setFormatter(formatter)
        logger.addHandler(fh)
        logger.propagate = False
        if hosttype == "EXEC":
            LOG._loggers[test.testobj.TestInputData.testid] = logger
        else:
            LOG._loggers[test.testid] = logger
        return logger

    @staticmethod
    def gettestlogger(test, hosttype):
        """
        Return logger object if present in LOG map else it will create new logger object

        """
        if hosttype == "EXEC":
            if test.testobj.TestInputData.testid in LOG._loggers.keys():
                return LOG._loggers[test.testobj.TestInputData.testid]
            else:
                logger = LOG.init_testlogger(test, hosttype)
                return logger
        else:
            if test.testid in LOG._loggers.keys():
                return LOG._loggers[test.testid]
            else:
                logger = LOG.init_testlogger(test, hosttype)
                return logger
