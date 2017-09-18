# This file implement some basic routines

#!/usr/bin/env python
import threading
import os
import sys
import logger
import tarfile
import socket

import smtplib
import email.mime.multipart
import email.mime.base
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


def get_local_ip():
    """
    This procedure find out local ip address of the host and return

    """
    ip = [l for l in ([ip for ip in socket.gethostbyname_ex(socket.gethostname())[2] if not ip.startswith("127.")][:1],
                      [[(s.connect(('8.8.8.8', 53)), s.getsockname()[0], s.close()) for s in
                        [socket.socket(socket.AF_INET, socket.SOCK_DGRAM)]][0][1]]) if l][0][0]
    return ip


def send_email(subject, receiver, body, lctx, user, mailing_host, smtp_server, smtp_port):
    """
    This procedure send mail to receiver using SMTP library.
    """
    msg_html = ("<html><head></head><body> <p></p>" \
    "<br><p>" + body + "</p><br>" \
    "<p>Daytona</p> </body></html>")

    msg = MIMEMultipart('alternative')
    msg.attach(MIMEText(msg_html, 'html'))

    msg['Subject'] = subject
    msg['From'] = 'Results@Daytona'
    msg['To'] = receiver

    sender = user + '@' + mailing_host
    lctx.debug(sender)

    try:
        smtpobj = smtplib.SMTP(smtp_server, smtp_port)
        smtpobj.sendmail(sender, [receiver], msg.as_string())
        smtpobj.quit()
    except Exception as e:
        lctx.error("Mail sending error")
        lctx.error(e)


def make_tarfile(output_filename, source_dir):
    """
    This function makes a tar file of all the files present in source_dir and save it with the name output_filename

    """
    with tarfile.open(output_filename, "w:gz") as tar:
        tar.add(source_dir, arcname=os.path.basename(source_dir))


def untarfile(input_filename, dest_dir):
    """
    This procedure extract the content of TAR file input_filename and save all the file in location dest_dir

    """
    with tarfile.open(input_filename, "r") as tar:
        tar.extractall(dest_dir)


def createdir(dirent, lctx):
    """
    This procdeure create directory dirent

    """
    if dirent == "" or dirent is None:
        raise Exception("create dir failed", dirent)
    if not os.path.exists(os.path.dirname(dirent)):
        os.makedirs(os.path.dirname(dirent))
        lctx.debug("created:" + dirent)
    else:
        lctx.debug("dir exists:" + dirent)


class CommunicationError(Exception):
    """
    Currently not in use

    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class RedirectStdStreams(object):
    """
    Currently not in use

    """
    def __init__(self, stdout=None, stderr=None):
        self._stdout = stdout or sys.stdout
        self._stderr = stderr or sys.stderr

    def __enter__(self):
        self.old_stdout, self.old_stderr = sys.stdout, sys.stderr
        self.old_stdout.flush()
        self.old_stderr.flush()
        sys.stdout, sys.stderr = self._stdout, self._stderr

    def __exit__(self, exc_type, exc_value, traceback):
        self._stdout.flush()
        self._stderr.flush()
        sys.stdout = self.old_stdout
        sys.stderr = self.old_stderr


class FuncThread(threading.Thread):
    """
    FuncThread is a class for invoking thread threads for performing specific tasks in parallel

    """
    def __del__(self):
        self.stop()

    def __init__(self, target, *args):
        super(FuncThread, self).__init__()
        # Exit the server thread when the main thread terminates
        FuncThread.daemon = args[0]
        self._stop = threading.Event()
        self.paused = False
        self.state = threading.Condition()
        self._target = target
        self._args = args
        threading.Thread.__init__(self)

    def resume(self):
        with self.state:
            self.paused = False
            self.state.notify()  # unblock self if waiting

    def pause(self):
        with self.state:
            self.paused = True  # make self block and wait

    def stop(self):
        self._stop.set()

    def run(self):
        self._target(*self._args)

    def stopped(self):
        return self._stop.isSet()

    def check(self):
        with self.state:
            if self.paused:
                self.state.wait()  # block until notified
            if self._stop.isSet():
                return False
