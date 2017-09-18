# This is TCP Client used by agent/scheduler for sending Daytona messages to TCP server running on scheduler/agent

#!/usr/bin/env python
import socket
import sys
import common
import time
from common import CommunicationError

class TCPClient:
  def __init__(self, logctx):
    self.lctx = logctx

  def stream_end(self, ip, port, fwip, s, sfile):
    """
    This is a signal that inform scheduler that streaming of output from the agent host is ending.

    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      sfile.write("DAYTONA_STREAM_END")
      sock.connect((ip, port))
      sock.send("DAYTONA_CMD:DAYTONA_STREAM_END:0:"+fwip)
      s.close()
      sys.stdout = sys.__stdout__
    except IOError:
      self.lctx.error("Server not responding, perhaps server not running")
      self.lctx.error(ip + "," +  "could not stream ")
    finally:
      self.lctx.debug("closing sock")
      sock.shutdown(socket.SHUT_RDWR)
      sock.close()
    return

  def stream_start(self, ip, port, fwip):
    """
    This signal marks the start of streaming of output from execution script executing on agent. Upon receiving this
    signal scheduler open a file handler to a file called "execution.log" for this specific test and start writing
    output it is receiving from the agent

    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sfile = None
    try:
      sock.connect((ip, port))
      self.lctx.debug(ip + "," + str(port))
      msg = "DAYTONA_CMD:DAYTONA_STREAM_START:0:"+fwip
      sock.send(msg)
      response = sock.recv(8192)
      if response == "STREAMFILEREADY":
          self.lctx.debug(response)
      sfile = sock.makefile('wb')
      sys.stdout = sfile
    except IOError:
      self.lctx.error("Server not responding, perhaps server not running")
      self.lctx.error(ip + "," +  "could not stream ")
    return (sock,sfile)

  def sendFile(self, ip, port, filename, loc):
    """
    This procedure send files to a remote host using ip and port passed as arguments. In daytona, agent send SAR
    logs tar file and agent test life cycle logs upon test completion. First it send DAYTONA_FILE_UPLOAD message to
    scheduler with filename and destination location to save this file on scheduler. Scheduler upon receiving this
    message does preparation for receiving files and send FILEREADY message back. Agent upon receiving
    acknowledgement from scheduler starts the actual file transfer

    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
      sock.connect((ip, port))
      msg = "DAYTONA_CMD:DAYTONA_FILE_UPLOAD:0:"+filename+","+loc+""
      sock.send(msg)
      self.lctx.debug(msg)
      response = sock.recv(8192)
      if response == "FILEREADY":
          self.lctx.debug(response)
      f = open(filename,'rb')
      l = f.read(8192)
      while l:
        self.lctx.debug("Sending...")
        self.lctx.debug(len(l))
        sock.sendall(l)
        l = f.read(8192)
      f.close()
      self.lctx.debug("Done Sending")
      sock.shutdown(socket.SHUT_WR)
    except IOError:
      self.lctx.error("Server not responding, perhaps server not running")
      self.lctx.error(ip + "," +  "could not send file : " + filename)
    finally:
      self.lctx.debug("closing sock")
      try:
        sock.shutdown(socket.SHUT_RDWR)
      except:
        pass
      sock.close()
    return

  def send(self, ip, port, message):
    """
    This procedure send Daytona messages from one host to another. It is mainly used by scheduler to send messages 
    to agent

    """
    l = message.split(":")
    self.lctx.debug(l[0]+":"+l[1]+":"+l[2])
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    response = None
    try:
      sock.settimeout(5)
      sock.connect((ip, port))
      sock.sendall(message)
      response = sock.recv(8192)
      self.lctx.debug("Received: {}".format(response))
    except IOError:
      self.lctx.error("Server not responding, perhaps server not running")
      self.lctx.error("Could not send message to " + ip + ":" + str(port)+ "," +  message)
    finally:
      self.lctx.debug("closing sock")
      try:
        sock.shutdown(socket.SHUT_RDWR)
      except:
        pass
      sock.close()
    return response
