# This file implement class which is used as envelope which is used to carry payload in Daytona messages
# from scheduler/agent

#!/usr/bin/env python
import uuid

class DaytonaEnvelope:
    def __init__(self):
        self.msgid = None
        self.timestamp = None
        self.host = None

    def construct(self, command, data):
        self.msgid = uuid.uuid4()
        return "DAYTONA_CMD:" + command + ":" + str(self.msgid) + ":" + data

