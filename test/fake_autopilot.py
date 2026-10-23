#
# Copyright (C) 2026 DroneCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
'''
 fake pymavlink connection behaving like an ArduPilot autopilot, for testing
 the MAVCAN driver without hardware or SITL
'''

import threading
import time
from collections import deque, OrderedDict

from pymavlink import mavutil
from pymavlink.dialects.v20 import ardupilotmega as mavlink2


def _decode(buf):
    '''round trip through the wire format, so floats arrive as float32'''
    return mavlink2.MAVLink(None).parse_buffer(buf)[0]


class _Sink(object):
    def __init__(self, handler):
        self.handler = handler

    def write(self, buf):
        self.handler(_decode(buf))


class FakeAutopilot(object):
    '''
    Stands in for the connection returned by mavutil.mavlink_connection().
    Messages the driver sends are decoded and answered like ArduPilot would.
    '''

    def __init__(self, params=None, sysid=1, compid=mavlink2.MAV_COMP_ID_AUTOPILOT1):
        self.sysid = sysid
        self.compid = compid
        # name -> [value, param_type]
        self.param_table = OrderedDict()
        for name, v in (params or {}).items():
            if isinstance(v, tuple):
                self.param_table[name] = list(v)
            else:
                self.param_table[name] = [float(v), mavlink2.MAV_PARAM_TYPE_REAL32]
        self.readonly = set()           # sets of these names are ignored and echo the old value
        self.drop_replies = {}          # name -> number of PARAM_VALUE replies to drop
        self.drop_list_indices = set()  # indices dropped once from a PARAM_REQUEST_LIST stream
        self.never_reply_indices = set()
        self.reboot_result = mavlink2.MAV_RESULT_ACCEPTED  # None means never ack
        self.send_param_errors = False  # ArduPilot since 2025-09 answers failures with PARAM_ERROR
        self.hold = False               # queue PARAM_VALUE replies until release()
        self.received = []              # every message the driver sent us
        self.fd = None

        self._held = []
        self._inbox = deque()
        self._cond = threading.Condition()
        self.mav = mavlink2.MAVLink(_Sink(self._handle), srcSystem=250,
                                    srcComponent=mavlink2.MAV_COMP_ID_MAVCAN)
        self.heartbeat()

    # ---- pymavlink connection API used by io_process ----

    def recv_match(self, type=None, blocking=False, timeout=None):
        if isinstance(type, str):
            type = [type]
        deadline = None if timeout is None else time.time() + timeout
        with self._cond:
            while True:
                while self._inbox:
                    m = self._inbox.popleft()
                    if type is None or m.get_type() in type:
                        return m
                if not blocking:
                    return None
                remaining = None if deadline is None else deadline - time.time()
                if remaining is not None and remaining <= 0:
                    return None
                self._cond.wait(remaining)

    def setup_signing(self, *args, **kwargs):
        pass

    def packet_loss(self):
        return 0.0

    def close(self):
        pass

    # ---- test controls ----

    def send_from(self, sysid, compid, msgname, *args):
        '''inject a message from any system/component'''
        mav = mavlink2.MAVLink(None, srcSystem=sysid, srcComponent=compid)
        msg = getattr(mav, msgname + '_encode')(*args)
        self._deliver(_decode(msg.pack(mav)))

    def heartbeat(self):
        self.send_from(self.sysid, self.compid, 'heartbeat', mavlink2.MAV_TYPE_QUADROTOR,
                       mavlink2.MAV_AUTOPILOT_ARDUPILOTMEGA, 0, 0, 0)

    def release(self):
        '''stop holding and send every held PARAM_VALUE'''
        with self._cond:
            self.hold = False
            held, self._held = self._held, []
        for m in held:
            self._deliver(m)

    def sent(self, msgtype):
        return [m for m in list(self.received) if m.get_type() == msgtype]

    # ---- autopilot behaviour ----

    def _deliver(self, msg):
        with self._cond:
            self._inbox.append(msg)
            self._cond.notify_all()

    def _for_us(self, m):
        return m.target_system == self.sysid and m.target_component == self.compid

    def _send_param(self, name):
        names = list(self.param_table.keys())
        value, ptype = self.param_table[name]
        mav = mavlink2.MAVLink(None, srcSystem=self.sysid, srcComponent=self.compid)
        msg = mav.param_value_encode(name.encode('ascii'), value, ptype, len(names), names.index(name))
        msg = _decode(msg.pack(mav))
        with self._cond:
            if self.drop_replies.get(name, 0) > 0:
                self.drop_replies[name] -= 1
                return
            if self.hold:
                self._held.append(msg)
                return
        self._deliver(msg)

    def _param_error(self, m, error):
        '''reply to the requester only, like ArduPilot'''
        if self.send_param_errors:
            self.send_from(self.sysid, self.compid, 'param_error', m.get_srcSystem(), m.get_srcComponent(),
                           m.param_id.encode('ascii'), getattr(m, 'param_index', -1), error)

    def _handle(self, m):
        self.received.append(m)
        mtype = m.get_type()
        if mtype == 'PARAM_REQUEST_READ' and self._for_us(m):
            names = list(self.param_table.keys())
            if m.param_index >= 0:
                if m.param_index < len(names) and m.param_index not in self.never_reply_indices:
                    self._send_param(names[m.param_index])
            elif m.param_id in self.param_table:
                self._send_param(m.param_id)
            else:
                self._param_error(m, mavlink2.MAV_PARAM_ERROR_DOES_NOT_EXIST)
        elif mtype == 'PARAM_SET' and self._for_us(m):
            if m.param_id not in self.param_table:
                self._param_error(m, mavlink2.MAV_PARAM_ERROR_DOES_NOT_EXIST)
                return
            if m.param_id in self.readonly:
                self._param_error(m, mavlink2.MAV_PARAM_ERROR_PERMISSION_DENIED)
            else:
                self.param_table[m.param_id][0] = m.param_value
            self._send_param(m.param_id)
        elif mtype == 'PARAM_REQUEST_LIST' and self._for_us(m):
            for idx, name in enumerate(list(self.param_table.keys())):
                if idx in self.drop_list_indices:
                    self.drop_list_indices.discard(idx)
                    continue
                if idx in self.never_reply_indices:
                    continue
                self._send_param(name)
        elif mtype == 'COMMAND_LONG' and self._for_us(m):
            if m.command == mavlink2.MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN and self.reboot_result is not None:
                self.send_from(self.sysid, self.compid, 'command_ack', m.command, self.reboot_result)


class FakeLogFile(FakeAutopilot, mavutil.mavlogfile):
    '''a fake that io_process sees as a read-only log replay'''
    pass
