#
# Copyright (C) 2022 DroneCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
'''
 driver for CAN over MAVLink, using MAV_CMD_CAN_FORWARD and CAN_FRAME messages
'''

import os
import sys
import time
import select
import signal
import struct
import multiprocessing
from collections import OrderedDict
from logging import getLogger
from .common import DriverError, ParamError, CANFrame, AbstractDriver
from pymavlink import mavutil

try:
    import queue
except ImportError:
    # noinspection PyPep8Naming,PyUnresolvedReferences
    import Queue as queue

if 'darwin' in sys.platform:
    RX_QUEUE_SIZE = 32767   # http://stackoverflow.com/questions/5900985/multiprocessing-queue-maxsize-limit-is-32767
else:
    RX_QUEUE_SIZE = 1000000
TX_QUEUE_SIZE = 1000
CAN_TYPES = ['CAN_FRAME', 'CANFD_FRAME']
RECV_TYPES = CAN_TYPES + ['HEARTBEAT', 'PARAM_VALUE', 'PARAM_ERROR', 'COMMAND_ACK']

logger = getLogger(__name__)
kill_process = False


def parent_process_alive(parent_pid):
    '''check if the process that spawned us is still alive.

    We can't compare os.getppid() to parent_pid: under the 'forkserver'
    start method (the default on Linux from Python 3.14) the IO process is
    forked from the fork-server, so getppid() returns the fork-server's pid,
    not the bridge's. Check the recorded parent pid directly instead.'''
    if sys.platform.startswith('win'):
        return True             # os.kill(pid, 0) is unreliable on Windows
    try:
        os.kill(parent_pid, 0)
    except OSError:
        return False
    return True


class ControlMessage(object):
    def __init__(self, command, data):
        self.command = command
        self.data = data


INT_PARAM_TYPES = set(range(mavutil.mavlink.MAV_PARAM_TYPE_UINT8, mavutil.mavlink.MAV_PARAM_TYPE_INT64 + 1))
PARAM_NAME_LEN = 16
PARAM_MAX_OUTSTANDING = 10


def float32(value):
    '''value as it survives the trip through a MAVLink float field'''
    return struct.unpack('<f', struct.pack('<f', value))[0]


def param_value_from_mavlink(value, param_type):
    '''ArduPilot casts integer parameters to float, so cast them back'''
    if param_type in INT_PARAM_TYPES:
        return int(round(value))
    return value


class ParamOp(object):
    '''a get or set of one parameter, shared by every request waiting on it'''
    def __init__(self, kind, name, value, timeout, retries, now):
        self.kind = kind
        self.name = name
        self.value = value
        self.retries = max(retries, 1)
        self.interval = timeout / self.retries
        self.timeout = timeout
        self.created = now
        self.attempts = 0
        self.next_send = 0
        self.sent_set = False
        self.last_echo = None
        self.waiters = []


class ParamEngine(object):
    '''
    Runs in io_process. Turns get/set/fetch/reboot requests from the parent into
    MAVLink parameter traffic with the autopilot, and reports back on reply_queue.
    There is at most one op in flight per name, later requests for the name queue behind it.
    '''

    def __init__(self, get_conn, reply_queue, max_outstanding):
        self.get_conn = get_conn
        self.reply_queue = reply_queue
        self.max_outstanding = max_outstanding
        self.target_system = 0
        self.readonly = False
        self.types = {}
        self.ops = OrderedDict()     # name -> list of ParamOp, head is the active one
        self.requests = {}           # req_id -> [results, errors, remaining]
        self.fetch = None
        self.reboot = None

    def reply(self, *args):
        self.reply_queue.put_nowait(args)

    def from_autopilot(self, m):
        return (self.target_system != 0 and m.get_srcSystem() == self.target_system and
                m.get_srcComponent() == mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1)

    # ---- requests from the parent ----

    def control(self, command, data, now):
        '''handle a ControlMessage, returning False if it is not ours'''
        handlers = {'ParamGet': self.start_get, 'ParamSet': self.start_set,
                    'ParamFetchAll': self.start_fetch, 'Reboot': self.start_reboot}
        if command not in handlers:
            return False
        handlers[command](now, *data)
        return True

    def start_get(self, now, req_id, names, timeout, retries):
        self.start_ops(now, req_id, [('get', n, None) for n in names], timeout, retries)

    def start_set(self, now, req_id, values, timeout, retries):
        self.start_ops(now, req_id, [('set', n, v) for n, v in values.items()], timeout, retries)

    def start_ops(self, now, req_id, items, timeout, retries):
        if self.readonly:
            self.reply('done', req_id, {}, dict((n, 'connection is read-only') for _, n, _ in items))
            return
        self.requests[req_id] = [{}, {}, len(items)]
        if not items:
            self.finish_request(req_id)
        for kind, name, value in items:
            queued = self.ops.setdefault(name, [])
            if kind == 'get' and queued:
                # a read waits behind whatever is already in flight for the name
                queued[-1].waiters.append(req_id)
                continue
            op = ParamOp(kind, name, value, timeout, retries, now)
            op.waiters.append(req_id)
            queued.append(op)

    def start_fetch(self, now, req_id, timeout, retries):
        if self.readonly:
            self.reply('fetched', req_id, {}, [])
            return
        if self.fetch is not None:
            self.fetch['waiters'].append(req_id)
            return
        self.fetch = dict(waiters=[req_id], timeout=timeout, retries=retries, received={},
                          count=None, last_rx=now, stalls=0, seen_at_gap=0, last_progress=0)
        self.send_request_list()

    def start_reboot(self, now, req_id, timeout):
        if self.readonly or self.target_system == 0:
            self.reply('reboot', req_id, 'connection is read-only' if self.readonly else 'no autopilot heartbeat seen')
            return
        self.reboot = (req_id, now + timeout)
        self.get_conn().mav.command_long_send(self.target_system, mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1,
                                              mavutil.mavlink.MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN,
                                              0, 1, 0, 0, 0, 0, 0, 0)

    # ---- messages from the autopilot ----

    def handle(self, m, now):
        mtype = m.get_type()
        if mtype == 'HEARTBEAT':
            if (self.target_system == 0 and m.get_srcComponent() == mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1 and
                    m.autopilot != mavutil.mavlink.MAV_AUTOPILOT_INVALID):
                self.target_system = m.get_srcSystem()
        elif not self.from_autopilot(m):
            return
        elif mtype == 'PARAM_VALUE':
            self.handle_param_value(m, now)
        elif mtype == 'PARAM_ERROR':
            self.handle_param_error(m)
        elif mtype == 'COMMAND_ACK' and m.command == mavutil.mavlink.MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN:
            self.handle_reboot_ack(m)

    def handle_param_value(self, m, now):
        name = m.param_id
        self.types[name] = m.param_type
        value = param_value_from_mavlink(m.param_value, m.param_type)
        self.reply('param', name, value)
        if self.fetch is not None and m.param_index < m.param_count:
            self.fetch_received(m.param_index, m.param_count, name, value, now)
        queued = self.ops.get(name)
        if not queued:
            return
        op = queued[0]
        if op.kind == 'get':
            self.resolve(op, value, None)
        elif op.sent_set:
            if float32(m.param_value) == float32(op.value):
                self.resolve(op, value, None)
            else:
                op.last_echo = value
        elif op.attempts > 0:
            # the read that told us the type is done, now send the set itself
            op.attempts = 0
            self.send_op(op, now)

    def handle_param_error(self, m):
        '''newer ArduPilot reports a failed read or set instead of staying silent'''
        mav = self.get_conn().mav
        if (m.target_system, m.target_component) != (mav.srcSystem, mav.srcComponent):
            return
        queued = self.ops.get(m.param_id)
        if not queued or queued[0].attempts == 0:
            return
        error = mavutil.mavlink.enums['MAV_PARAM_ERROR'].get(m.error)
        self.resolve(queued[0], None, 'autopilot reported %s' % (error.name if error else 'error %u' % m.error))

    def handle_reboot_ack(self, m):
        if self.reboot is None:
            return
        req_id = self.reboot[0]
        self.reboot = None
        if m.result != mavutil.mavlink.MAV_RESULT_ACCEPTED:
            self.reply('reboot', req_id, 'reboot refused, result %u' % m.result)
            return
        for queued in list(self.ops.values()):
            for op in list(queued):
                self.resolve(op, None, 'autopilot rebooting')
        if self.fetch is not None:
            self.finish_fetch()
        self.types = {}
        self.reply('rebooted')
        self.reply('reboot', req_id, None)

    # ---- timers, called every pass of the io loop ----

    def tick(self, now):
        active = sum(1 for queued in self.ops.values() if queued[0].attempts > 0)
        for queued in list(self.ops.values()):
            op = queued[0]
            if op.attempts == 0:
                if self.target_system == 0:
                    if now - op.created > op.timeout:
                        self.resolve(op, None, 'no autopilot heartbeat seen')
                elif active < self.max_outstanding:
                    self.send_op(op, now)
                    active += 1
            elif now >= op.next_send:
                self.retry_op(op, now)
        if self.fetch is not None and now - self.fetch['last_rx'] > self.fetch['timeout']:
            self.fetch_gap(now)
        if self.reboot is not None and now > self.reboot[1]:
            self.reply('reboot', self.reboot[0], 'no reboot acknowledgement from autopilot')
            self.reboot = None

    # ---- internals ----

    def retry_op(self, op, now):
        if op.attempts < op.retries:
            self.send_op(op, now)
        elif op.last_echo is not None:
            self.resolve(op, None, 'autopilot kept value %s' % op.last_echo)
        else:
            self.resolve(op, None, 'no reply from autopilot (unknown parameter or link loss)')

    def send_op(self, op, now):
        op.attempts += 1
        op.next_send = now + op.interval
        mav = self.get_conn().mav
        name = op.name.encode('ascii')
        if op.kind == 'set' and op.name in self.types:
            op.sent_set = True
            mav.param_set_send(self.target_system, mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1,
                               name, float(op.value), self.types[op.name])
        else:
            mav.param_request_read_send(self.target_system, mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1, name, -1)

    def resolve(self, op, value, error):
        queued = self.ops[op.name]
        queued.remove(op)
        if not queued:
            del self.ops[op.name]
        for req_id in op.waiters:
            results, errors, _ = self.requests[req_id]
            if error is None:
                results[op.name] = value
            else:
                errors[op.name] = error
            self.requests[req_id][2] -= 1
            if self.requests[req_id][2] <= 0:
                self.finish_request(req_id)

    def finish_request(self, req_id):
        results, errors, _ = self.requests.pop(req_id)
        self.reply('done', req_id, results, errors)

    def send_request_list(self):
        if self.target_system == 0:
            return
        self.get_conn().mav.param_request_list_send(self.target_system, mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1)

    def fetch_received(self, index, count, name, value, now):
        f = self.fetch
        f['count'] = count
        f['received'][index] = (name, value)
        f['last_rx'] = now
        received = len(f['received'])
        if received >= count or now - f['last_progress'] > 0.1:
            f['last_progress'] = now
            for req_id in f['waiters']:
                self.reply('progress', req_id, received, count)
        if received >= count:
            self.finish_fetch()

    def fetch_gap(self, now):
        '''nothing arrived for a while: re-request what is missing, or give up'''
        f = self.fetch
        f['last_rx'] = now
        if len(f['received']) <= f['seen_at_gap']:
            f['stalls'] += 1
        f['seen_at_gap'] = len(f['received'])
        if f['stalls'] > f['retries']:
            self.finish_fetch()
            return
        if f['count'] is None:
            self.send_request_list()
            return
        missing = [i for i in range(f['count']) if i not in f['received']]
        for index in missing[:self.max_outstanding]:
            self.get_conn().mav.param_request_read_send(self.target_system, mavutil.mavlink.MAV_COMP_ID_AUTOPILOT1,
                                                        b'', index)

    def finish_fetch(self):
        f = self.fetch
        self.fetch = None
        params = dict(f['received'].values())
        missing = [i for i in range(f['count'] or 0) if i not in f['received']]
        for req_id in f['waiters']:
            self.reply('fetched', req_id, params, missing)


def tx_wait_fd(tx_queue):
    '''fd that becomes readable when a frame is queued for transmit.

    Returns None where it cannot be used, in which case the IO loop
    falls back to waiting on the link alone. select() on Windows only
    accepts sockets, so the link and the queue cannot be waited on
    together there.'''
    if sys.platform.startswith('win'):
        return None
    try:
        return tx_queue._reader.fileno()
    except Exception:
        return None

def io_process(url, bus, target_system, baudrate, tx_queue, rx_queue, exit_queue, parent_pid,
               reply_queue, max_outstanding):
    # leave Ctrl-C (SIGINT) handling to the parent process; this daemon
    # child is torn down when the parent exits
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    os.environ['MAVLINK20'] = '1'

    target_component = 0
    last_enable = time.time()
    conn = None
    filter_list = None
    signing_key = None
    last_loss_print_t = time.time()
    exit_proc = False
    readonly = False
    params = ParamEngine(lambda: conn, reply_queue, max_outstanding)
    params.target_system = target_system

    def learn_target(sysid):
        nonlocal target_system
        if target_system == 0:
            target_system = sysid
        if params.target_system == 0:
            params.target_system = sysid

    def connect():
        nonlocal conn, baudrate, readonly
        # a TCP link only reports EOF when the autopilot reboots, so let pymavlink reconnect it
        conn = mavutil.mavlink_connection(url, baud=baudrate, source_system=250,
                                          source_component=mavutil.mavlink.MAV_COMP_ID_MAVCAN,
                                          dialect='ardupilotmega', autoreconnect=url.startswith('tcp:'))
        if conn is None:
            raise DriverError('unable to connect to %s' % url)
        nonlocal signing_key
        if signing_key is not None:
            conn.setup_signing(signing_key, sign_outgoing=True)

        readonly = isinstance(conn, mavutil.mavlogfile)
        params.readonly = readonly

    def reconnect():
        nonlocal exit_proc
        while True and not exit_proc:
            if (not exit_queue.empty() and exit_queue.get() == "QUIT"):
                exit_proc = True
                return
            try:
                time.sleep(1)
                logger.info('reconnecting to %s' % url)
                connect()
                return
            except Exception:
                continue

    def enable_can_forward():
        '''re-enable CAN forwarding. Called at 1Hz'''
        if readonly:
            return
        nonlocal last_enable, bus, target_system
        last_enable = time.time()
        conn.mav.command_long_send(
            target_system,
            target_component,
            mavutil.mavlink.MAV_CMD_CAN_FORWARD,
            0,
            bus+1,
            0,
            0,
            0,
            0,
            0,
            0)
        if filter_list is not None:
            ids = sorted(filter_list[:16])
            num_ids = len(ids)
            if len(ids) < 16:
                ids += [0]*(16-num_ids)
            try:
                conn.mav.can_filter_modify_send(
                    target_system,
                    target_component,
                    bus+1,
                    mavutil.mavlink.CAN_FILTER_REPLACE,
                    num_ids,
                    ids)
            except Exception as ex:
                print(ex)

    def handle_control_message(m):
        '''handle a ControlMessage'''
        if params.control(m.command, m.data, time.time()):
            return
        if m.command == "BusNum":
            nonlocal bus
            bus = int(m.data)
        elif m.command == "FilterList":
            nonlocal filter_list
            filter_list = m.data
        elif m.command == "SigningKey":
            nonlocal signing_key
            signing_key = m.data
            conn.setup_signing(signing_key, sign_outgoing=True)

    connect()
    enable_can_forward()

    tx_fd = tx_wait_fd(tx_queue)

    def wait_set():
        '''fds to wait on, or None to fall back to a blocking receive.
        Recomputed each pass because reconnect() replaces conn.'''
        if tx_fd is None or conn.fd is None:
            return None
        return [conn.fd, tx_fd]

    while True:
        if (not exit_queue.empty() and exit_queue.get() == "QUIT") or exit_proc:
            conn.close()
            return
        if not parent_process_alive(parent_pid):
            # ensure we die when parent dies
            conn.close()
            return
        while not tx_queue.empty():
            if (not exit_queue.empty() and exit_queue.get() == "QUIT") or exit_proc:
                conn.close()
                return
            frame = tx_queue.get()
            if isinstance(frame, ControlMessage):
                # handled even when read-only, so parameter requests get an error back
                handle_control_message(frame)
                continue
            if readonly:
                continue
            message_id = frame.id
            if frame.extended:
                message_id |= 1<<31
            message = frame.data
            mlen = len(message)
            if mlen < frame.MAX_DATA_LENGTH:
                message += bytearray([0]*(frame.MAX_DATA_LENGTH-mlen))
            try:
                if frame.canfd:
                    conn.mav.canfd_frame_send(
                        target_system,
                        target_component,
                        bus,
                        mlen,
                        message_id,
                        message)
                else:
                    conn.mav.can_frame_send(
                        target_system,
                        target_component,
                        bus,
                        mlen,
                        message_id,
                        message)
            except Exception as ex:
                print(ex)
            if time.time() - last_enable > 1:
                enable_can_forward()

        wait_fds = wait_set()
        try:
            if wait_fds is None:
                m = conn.recv_match(type=RECV_TYPES, blocking=True, timeout=0.005)
            else:
                # Wait on the link and the transmit queue together. A
                # blocking receive here would hold a frame queued just
                # after the drain above until the receive times out,
                # putting milliseconds of latency on every frame we
                # send. Take anything already parsed first, since the
                # link fd is not readable while a decoded message sits
                # in the connection's own buffer.
                m = conn.recv_match(type=RECV_TYPES, blocking=False)
                if m is None:
                    select.select(wait_fds, [], [], 0.005)
                    m = conn.recv_match(type=RECV_TYPES, blocking=False)
        except Exception as ex:
            reconnect()
            continue
        now = time.time()
        if m is not None and m.get_type() not in CAN_TYPES:
            params.handle(m, now)
            if params.target_system != 0:
                learn_target(params.target_system)
            m = None
        if not readonly:
            params.tick(now)
        if m is None:
            if now - last_enable > 1:
                enable_can_forward()
            if now - last_loss_print_t > 5:
                last_loss_print_t = now
                print("MAVLink packet loss %.2f%%" % conn.packet_loss())
            continue
        learn_target(m.get_srcSystem())
        is_extended = (m.id & (1<<31)) != 0
        is_canfd = m.get_type() == 'CANFD_FRAME'
        canid = m.id & 0x1FFFFFFF
        frame = CANFrame(canid, m.data[:m.len], is_extended, canfd=is_canfd)
        rx_queue.put_nowait(frame)


# MAVLink CAN driver
#
class MAVCAN(AbstractDriver):
    """
    Driver for MAVLink CAN bus adapters, using CAN_FRAME MAVLink packets
    """

    def __init__(self, url, **kwargs):
        super(MAVCAN, self).__init__()
        self.bus = kwargs.get('bus_number', 1) - 1
        self.target_system = kwargs.get('mavlink_target_system', 0)
        self.filter_list = None
        baudrate = kwargs.get('baudrate', 115200)
        max_outstanding = kwargs.get('param_max_outstanding', PARAM_MAX_OUTSTANDING)
        self._params = {}
        self._param_listeners = []
        self._param_callbacks = {}
        self._next_req_id = 0

        self.rx_queue = multiprocessing.Queue(maxsize=RX_QUEUE_SIZE)
        self.tx_queue = multiprocessing.Queue(maxsize=TX_QUEUE_SIZE)
        self.exit_queue = multiprocessing.Queue(maxsize=1)
        self.reply_queue = multiprocessing.Queue()

        self.proc = multiprocessing.Process(target=io_process, name='mavcan_io_process',
                                            args=(url, self.bus, self.target_system, baudrate,
                                            self.tx_queue, self.rx_queue, self.exit_queue, os.getpid(),
                                            self.reply_queue, max_outstanding))
        self.proc.daemon = True
        self.proc.start()

        # allow signing pass phrase in environment
        pass_phrase = os.environ.get("DRONECAN_SIGNING_KEY",None)
        if pass_phrase:
            self.set_signing_passphrase(pass_phrase)

    def close(self):
        if self.proc is not None and self.proc.is_alive():
            self.exit_queue.put_nowait("QUIT")
            self.proc.join()

    def __del__(self):
        self.close()

    def receive(self, timeout=None):
        tstart = time.time()
        while True:
            self._process_param_replies()
            try:
                frame = self.rx_queue.get(block=0)
            except queue.Empty:
                frame = None
            if frame is not None:
                self._rx_hook(frame)
                return frame
            if timeout is not None:
                timeout = max(timeout, 0.001)
                if time.time() >= tstart + timeout:
                    return

    def send_frame(self, frame):
        self._tx_hook(frame)
        self.tx_queue.put_nowait(frame)

    def is_mavlink_port(device_name, baudrate):
        '''check if a device is sending mavlink'''
        os.environ['MAVLINK20'] = '1'
        conn = mavutil.mavlink_connection(device_name, baud=baudrate, source_system=250, source_component=mavutil.mavlink.MAV_COMP_ID_MAVCAN)
        if not conn:
            return False
        m = conn.recv_match(blocking=True, type=['HEARTBEAT','ATTITUDE', 'SYS_STATUS'], timeout=1.1)
        conn.close()
        return m is not None

    def set_filter_list(self, ids):
        '''set list of message IDs to accept, sent to the remote capture node with mavcan'''
        self.filter_list = ids
        self.tx_queue.put_nowait(ControlMessage('FilterList', self.filter_list))

    def get_filter_list(self, ids):
        '''set list of message IDs to accept, sent to the remote capture node with mavcan'''
        return self.filter_list

    def set_bus(self, busnum):
        '''set the remote bus number to attach to'''
        if busnum <= 0:
            raise DriverError('invalid bus %s' % busnum)
        self.bus = busnum - 1
        self.tx_queue.put_nowait(ControlMessage('BusNum', self.bus))

    def get_bus(self):
        '''get the remote bus number we are attached to'''
        return self.bus+1

    def get_filter_list(self):
        '''get the current filter list'''
        return self.filter_list

    def passphrase_to_key(self, passphrase):
        '''convert a passphrase to a 32 byte key'''
        import hashlib
        h = hashlib.new('sha256')
        if sys.version_info[0] >= 3:
            passphrase = passphrase.encode('ascii')
        h.update(passphrase)
        return h.digest()

    def set_signing_passphrase(self, passphrase):
        '''set MAVLink2 signing passphrase'''
        signing_key = self.passphrase_to_key(passphrase)
        self.tx_queue.put_nowait(ControlMessage('SigningKey', signing_key))

    # ---- autopilot MAVLink parameters ----
    #
    # Requests are carried out by ParamEngine in the io process. Callbacks and
    # listeners are only ever called from receive() or a blocking call below,
    # so they run on whichever thread spins the node.

    def supports_mavlink_params(self):
        return True

    @property
    def params(self):
        '''every autopilot parameter value seen so far, by name'''
        return dict(self._params)

    def add_param_listener(self, listener):
        '''listener(name, value) is called for every parameter value received from the autopilot'''
        self._param_listeners.append(listener)
        return self.HookRemover(lambda: self._param_listeners.remove(listener))

    def request_params(self, names, callback, timeout=3, retries=3):
        '''start reading parameters, callback(results, errors) is called when all have finished'''
        names = _unique([_check_param_name(n) for n in names])
        self._submit('ParamGet', callback, names, timeout, retries)

    def send_params(self, values, callback, timeout=3, retries=3):
        '''start setting parameters, callback(results, errors) gets the values the autopilot confirmed'''
        checked = OrderedDict()
        for name, value in values.items():
            checked[_check_param_name(name)] = _check_param_value(name, value)
        self._submit('ParamSet', callback, checked, timeout, retries)

    def fetch_all_params(self, callback, progress=None, timeout=1.0, retries=3):
        '''
        start fetching every parameter. callback(params, missing_indices) is called at the end and
        progress(received, total) along the way. Slow on low bandwidth links, as all parameters
        share the link with forwarded CAN traffic.
        '''
        req_id = self._submit('ParamFetchAll', callback, timeout, retries)
        if progress is not None:
            self._param_callbacks[req_id] = (callback, progress)

    def get_params(self, names, timeout=3, retries=3):
        '''read parameters, returning {name: value}. Raises ParamError if any failed'''
        return self._wait_params(self.request_params, names, timeout, retries)

    def set_params(self, values, timeout=3, retries=3):
        '''set parameters, returning the confirmed {name: value}. Raises ParamError if any failed'''
        return self._wait_params(self.send_params, values, timeout, retries)

    def get_param(self, name, timeout=3, retries=3):
        return self.get_params([name], timeout, retries)[_check_param_name(name)]

    def set_param(self, name, value, timeout=3, retries=3):
        return self.set_params({name: value}, timeout, retries)[_check_param_name(name)]

    def reboot_autopilot(self, timeout=3):
        '''reboot the autopilot, needed after changing CAN driver settings'''
        outcome = []
        self._submit('Reboot', outcome.append, timeout)
        self._wait_for(outcome, timeout + 2)
        if outcome[0] is not None:
            raise DriverError(outcome[0])

    def _submit(self, command, callback, *args):
        self._next_req_id += 1
        req_id = self._next_req_id
        self._param_callbacks[req_id] = (callback, None)
        self.tx_queue.put_nowait(ControlMessage(command, (req_id,) + args))
        return req_id

    def _wait_params(self, start, what, timeout, retries):
        outcome = []
        start(what, lambda results, errors: outcome.append((results, errors)), timeout, retries)
        # a set of a parameter whose type is unknown reads it first, which can double the time
        self._wait_for(outcome, 2 * timeout + 2)
        results, errors = outcome[0]
        if errors:
            raise ParamError('; '.join('%s: %s' % e for e in sorted(errors.items())), results, errors)
        return results

    def _wait_for(self, outcome, timeout):
        deadline = time.time() + timeout
        while not outcome:
            if time.time() > deadline:
                raise DriverError('no response from MAVLink io process')
            self._process_param_replies(0.01)

    def _process_param_replies(self, timeout=None):
        while True:
            try:
                reply = self.reply_queue.get(timeout=timeout) if timeout else self.reply_queue.get_nowait()
            except queue.Empty:
                return
            timeout = None
            self._handle_param_reply(reply[0], reply[1:])

    def _handle_param_reply(self, kind, args):
        if kind == 'param':
            name, value = args
            self._params[name] = value
            for listener in list(self._param_listeners):
                _call_safely(listener, name, value)
        elif kind == 'rebooted':
            self._params = {}
        elif kind == 'progress':
            progress = self._param_callbacks.get(args[0], (None, None))[1]
            if progress is not None:
                _call_safely(progress, *args[1:])
        else:
            callback = self._param_callbacks.pop(args[0])[0]
            _call_safely(callback, *args[1:])


def _call_safely(fn, *args):
    try:
        fn(*args)
    except Exception as ex:
        logger.error('Uncaught exception from MAVLink parameter callback: %r', ex, exc_info=True)


def _check_param_name(name):
    try:
        name.encode('ascii')
    except (AttributeError, UnicodeError):
        raise DriverError('parameter name %r is not ASCII text' % (name,))
    if not name or len(name) > PARAM_NAME_LEN:
        raise DriverError('parameter name %r must be 1 to %u characters' % (name, PARAM_NAME_LEN))
    return name.upper()


def _check_param_value(name, value):
    try:
        float32(value)
    except (TypeError, ValueError, OverflowError, struct.error):
        raise DriverError('value %r for %s does not fit a MAVLink float' % (value, name))
    return value


def _unique(names):
    return list(OrderedDict.fromkeys(names))
