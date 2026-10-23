#
# Copyright (C) 2026 DroneCAN Development Team  <uavcan.org>
#
# This software is distributed under the terms of the MIT License.
#
'''
 tests for reading and writing autopilot MAVLink parameters through the MAVCAN driver.
 The io process runs in a thread against FakeAutopilot instead of a real link.
'''

import threading
import time
import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from pymavlink.dialects.v20 import ardupilotmega as mavlink2

from dronecan.driver import mavcan
from dronecan.driver.common import AbstractDriver, CANFrame, DriverError, ParamError

from fake_autopilot import FakeAutopilot, FakeLogFile

INT8 = mavlink2.MAV_PARAM_TYPE_INT8
INT32 = mavlink2.MAV_PARAM_TYPE_INT32
REAL32 = mavlink2.MAV_PARAM_TYPE_REAL32

DEFAULT_PARAMS = {
    'CAN_P1_DRIVER': (1.0, INT8),
    'SERIAL1_BAUD': (57.0, INT32),
    'ATC_RAT_RLL_P': (0.135, REAL32),
}


class ThreadProcess(threading.Thread):
    '''runs io_process in a thread so it can share the fake connection'''
    def __init__(self, target, name, args):
        super(ThreadProcess, self).__init__(target=target, name=name, args=args)
        self.daemon = True


class Recorder(object):
    '''callback that remembers its calls and the thread it ran on'''
    def __init__(self):
        self.calls = []
        self.threads = []

    def __call__(self, *args):
        self.calls.append(args)
        self.threads.append(threading.current_thread())


class MAVCANTestCase(unittest.TestCase):
    def start(self, fake=None, **kwargs):
        self.fake = fake if fake is not None else FakeAutopilot(dict(DEFAULT_PARAMS))
        for p in (mock.patch.object(mavcan.mavutil, 'mavlink_connection', return_value=self.fake),
                  mock.patch.object(mavcan.multiprocessing, 'Process', ThreadProcess),
                  # io_process ignores SIGINT, which can only be set from the main thread
                  mock.patch.object(mavcan.signal, 'signal')):
            p.start()
            self.addCleanup(p.stop)
        self.driver = mavcan.MAVCAN('mavcan:fake', **kwargs)
        self.addCleanup(self.driver.close)
        return self.driver

    def pump(self, seconds):
        '''spin the driver like node.spin() would'''
        deadline = time.time() + seconds
        while time.time() < deadline:
            self.driver.receive(timeout=0.01)

    def pump_until(self, condition, timeout=3.0):
        deadline = time.time() + timeout
        while not condition() and time.time() < deadline:
            self.driver.receive(timeout=0.01)
        self.assertTrue(condition(), 'condition not met within %.1fs' % timeout)

    def requests_for(self, name):
        return [m for m in self.fake.sent('PARAM_REQUEST_READ') if m.param_id == name]


class GetParamTest(MAVCANTestCase):
    def test_supports_mavlink_params(self):
        self.assertTrue(self.start().supports_mavlink_params())

    def test_get_param_returns_int_for_integer_types(self):
        driver = self.start()
        value = driver.get_param('CAN_P1_DRIVER')
        self.assertEqual(value, 1)
        self.assertIsInstance(value, int)
        self.assertAlmostEqual(driver.get_param('ATC_RAT_RLL_P'), 0.135, places=6)
        self.assertIsInstance(driver.get_param('ATC_RAT_RLL_P'), float)

    def test_get_params_returns_dict_keyed_by_uppercased_name(self):
        driver = self.start()
        self.assertEqual(driver.get_params(['can_p1_driver', 'SERIAL1_BAUD']),
                         {'CAN_P1_DRIVER': 1, 'SERIAL1_BAUD': 57})

    def test_bad_names_rejected_before_sending(self):
        driver = self.start()
        for bad in ['A' * 17, u'CAN_P1_DRIVERé']:
            with self.assertRaises(DriverError):
                driver.get_param(bad)
            with self.assertRaises(DriverError):
                driver.set_param(bad, 1)
        self.pump(0.1)
        self.assertEqual(self.fake.sent('PARAM_REQUEST_READ'), [])
        self.assertEqual(self.fake.sent('PARAM_SET'), [])

    def test_unknown_param_raises_after_retries(self):
        driver = self.start()
        with self.assertRaises(DriverError):
            driver.get_param('NO_SUCH_PARAM', timeout=0.3, retries=3)
        self.assertEqual(len(self.requests_for('NO_SUCH_PARAM')), 3)

    def test_partial_failure_reports_results_and_errors(self):
        driver = self.start()
        with self.assertRaises(ParamError) as ctx:
            driver.get_params(['CAN_P1_DRIVER', 'NO_SUCH_PARAM'], timeout=0.3, retries=2)
        self.assertEqual(ctx.exception.results, {'CAN_P1_DRIVER': 1})
        self.assertEqual(list(ctx.exception.errors.keys()), ['NO_SUCH_PARAM'])
        self.assertIsInstance(ctx.exception, DriverError)

    def test_get_retries_dropped_reply(self):
        driver = self.start()
        self.fake.drop_replies['SERIAL1_BAUD'] = 2
        self.assertEqual(driver.get_param('SERIAL1_BAUD', timeout=0.9, retries=3), 57)
        self.assertEqual(len(self.requests_for('SERIAL1_BAUD')), 3)

    def test_requests_target_autopilot_learned_from_heartbeat(self):
        fake = FakeAutopilot(dict(DEFAULT_PARAMS), sysid=7)
        # a GCS heartbeat arriving first must not be mistaken for the autopilot
        fake._inbox.clear()
        fake.send_from(255, 190, 'heartbeat', mavlink2.MAV_TYPE_GCS, mavlink2.MAV_AUTOPILOT_INVALID, 0, 0, 0)
        fake.heartbeat()
        driver = self.start(fake)
        self.assertEqual(driver.get_param('CAN_P1_DRIVER'), 1)
        req = self.requests_for('CAN_P1_DRIVER')[0]
        self.assertEqual((req.target_system, req.target_component), (7, mavlink2.MAV_COMP_ID_AUTOPILOT1))

    def test_readonly_log_connection_raises_immediately(self):
        driver = self.start(FakeLogFile(dict(DEFAULT_PARAMS)))
        t0 = time.time()
        with self.assertRaises(DriverError):
            driver.get_param('CAN_P1_DRIVER', timeout=3)
        with self.assertRaises(DriverError):
            driver.set_param('CAN_P1_DRIVER', 0, timeout=3)
        self.assertLess(time.time() - t0, 1.0)


class SetParamTest(MAVCANTestCase):
    def test_set_param_returns_confirmed_value(self):
        driver = self.start()
        value = driver.set_param('ATC_RAT_RLL_P', 0.1)
        # echoed back as float32, which must still count as a match
        self.assertAlmostEqual(value, 0.1, places=6)
        self.assertAlmostEqual(self.fake.param_table['ATC_RAT_RLL_P'][0], 0.1, places=6)

    def test_set_integer_param_sends_plain_float(self):
        driver = self.start()
        self.assertEqual(driver.set_param('SERIAL1_BAUD', 115), 115)
        sent = self.fake.sent('PARAM_SET')[-1]
        # ArduPilot style cast, not byte-wise packing of the integer into the float
        self.assertEqual(sent.param_value, 115.0)
        self.assertEqual(sent.param_type, INT32)

    def test_set_params_sets_several(self):
        driver = self.start()
        self.assertEqual(driver.set_params({'CAN_P1_DRIVER': 0, 'SERIAL1_BAUD': 921}),
                         {'CAN_P1_DRIVER': 0, 'SERIAL1_BAUD': 921})
        self.assertEqual(self.fake.param_table['CAN_P1_DRIVER'][0], 0.0)
        self.assertEqual(self.fake.param_table['SERIAL1_BAUD'][0], 921.0)

    def test_set_retries_when_echo_lost(self):
        driver = self.start()
        # read first so the type is known and the dropped reply is the set echo
        driver.get_param('CAN_P1_DRIVER')
        self.fake.drop_replies['CAN_P1_DRIVER'] = 1
        self.assertEqual(driver.set_param('CAN_P1_DRIVER', 2, timeout=0.9, retries=3), 2)
        sets = [m for m in self.fake.sent('PARAM_SET') if m.param_id == 'CAN_P1_DRIVER']
        self.assertEqual(len(sets), 2)

    def test_set_mismatched_echo_raises(self):
        driver = self.start()
        self.fake.readonly.add('SERIAL1_BAUD')
        with self.assertRaises(DriverError):
            driver.set_param('SERIAL1_BAUD', 115, timeout=0.3, retries=2)

    def test_set_unknown_param_raises(self):
        driver = self.start()
        with self.assertRaises(DriverError):
            driver.set_param('NO_SUCH_PARAM', 1, timeout=0.3, retries=2)


class AsyncParamTest(MAVCANTestCase):
    def test_callbacks_run_on_the_thread_calling_receive(self):
        driver = self.start()
        cb = Recorder()
        driver.request_params(['CAN_P1_DRIVER'], cb)
        self.pump_until(lambda: cb.calls)
        self.assertEqual(cb.calls, [({'CAN_P1_DRIVER': 1}, {})])
        self.assertIs(cb.threads[0], threading.current_thread())

    def test_send_params_callback(self):
        driver = self.start()
        cb = Recorder()
        driver.send_params({'SERIAL1_BAUD': 230, 'NO_SUCH_PARAM': 1}, cb, timeout=0.3, retries=1)
        self.pump_until(lambda: cb.calls)
        results, errors = cb.calls[0]
        self.assertEqual(results, {'SERIAL1_BAUD': 230})
        self.assertEqual(list(errors.keys()), ['NO_SUCH_PARAM'])

    def test_blocking_call_dispatches_other_callbacks(self):
        driver = self.start()
        cb = Recorder()
        driver.request_params(['CAN_P1_DRIVER'], cb)
        driver.get_param('SERIAL1_BAUD')
        driver.get_param('ATC_RAT_RLL_P')
        self.assertEqual(len(cb.calls), 1)

    def test_concurrent_requests_for_same_name_are_merged(self):
        driver = self.start()
        self.fake.hold = True
        cb1, cb2 = Recorder(), Recorder()
        driver.request_params(['CAN_P1_DRIVER'], cb1)
        driver.request_params(['CAN_P1_DRIVER'], cb2)
        self.pump(0.2)
        self.assertEqual(len(self.requests_for('CAN_P1_DRIVER')), 1)
        self.fake.release()
        self.pump_until(lambda: cb1.calls and cb2.calls)
        self.assertEqual(cb1.calls[0][0], {'CAN_P1_DRIVER': 1})
        self.assertEqual(cb2.calls[0][0], {'CAN_P1_DRIVER': 1})

    def test_read_waits_for_pending_set(self):
        driver = self.start()
        self.fake.hold = True
        set_cb, get_cb = Recorder(), Recorder()
        driver.send_params({'CAN_P1_DRIVER': 5}, set_cb)
        driver.request_params(['CAN_P1_DRIVER'], get_cb)
        self.pump(0.2)
        self.fake.release()
        self.pump_until(lambda: set_cb.calls and get_cb.calls)
        self.assertEqual(get_cb.calls[0][0], {'CAN_P1_DRIVER': 5})

    def test_outstanding_requests_are_limited(self):
        params = dict(('P%02d' % i, float(i)) for i in range(25))
        driver = self.start(FakeAutopilot(params))
        self.fake.hold = True
        cb = Recorder()
        driver.request_params(sorted(params.keys()), cb)
        self.pump(0.3)
        self.assertEqual(len(self.fake.sent('PARAM_REQUEST_READ')), 10)
        self.fake.release()
        self.pump_until(lambda: cb.calls)
        results, errors = cb.calls[0]
        self.assertEqual(errors, {})
        self.assertEqual(len(results), 25)
        self.assertEqual(len(self.fake.sent('PARAM_REQUEST_READ')), 25)

    def test_outstanding_limit_is_configurable(self):
        params = dict(('P%02d' % i, float(i)) for i in range(10))
        driver = self.start(FakeAutopilot(params), param_max_outstanding=3)
        self.fake.hold = True
        driver.request_params(sorted(params.keys()), Recorder())
        self.pump(0.3)
        self.assertEqual(len(self.fake.sent('PARAM_REQUEST_READ')), 3)


class ParamErrorTest(MAVCANTestCase):
    '''ArduPilot since 2025-09 answers failed reads and sets with PARAM_ERROR instead of silence'''

    def start_with_errors(self):
        fake = FakeAutopilot(dict(DEFAULT_PARAMS))
        fake.send_param_errors = True
        return self.start(fake)

    def test_unknown_param_fails_without_waiting_for_timeout(self):
        driver = self.start_with_errors()
        t0 = time.time()
        with self.assertRaises(ParamError) as ctx:
            driver.get_param('NO_SUCH_PARAM', timeout=3, retries=3)
        self.assertLess(time.time() - t0, 0.5)
        self.assertIn('DOES_NOT_EXIST', ctx.exception.errors['NO_SUCH_PARAM'])
        self.assertEqual(len(self.requests_for('NO_SUCH_PARAM')), 1)

    def test_set_of_unknown_param_fails_without_waiting_for_timeout(self):
        driver = self.start_with_errors()
        t0 = time.time()
        with self.assertRaises(ParamError) as ctx:
            driver.set_param('NO_SUCH_PARAM', 1, timeout=3, retries=3)
        self.assertLess(time.time() - t0, 0.5)
        self.assertIn('DOES_NOT_EXIST', ctx.exception.errors['NO_SUCH_PARAM'])

    def test_permission_denied_fails_without_retrying(self):
        driver = self.start_with_errors()
        self.fake.readonly.add('SERIAL1_BAUD')
        t0 = time.time()
        with self.assertRaises(ParamError) as ctx:
            driver.set_param('SERIAL1_BAUD', 115, timeout=3, retries=3)
        self.assertLess(time.time() - t0, 0.5)
        self.assertIn('PERMISSION_DENIED', ctx.exception.errors['SERIAL1_BAUD'])
        self.assertEqual(len(self.fake.sent('PARAM_SET')), 1)

    def test_batch_reports_param_error_alongside_results(self):
        driver = self.start_with_errors()
        with self.assertRaises(ParamError) as ctx:
            driver.get_params(['CAN_P1_DRIVER', 'NO_SUCH_PARAM'])
        self.assertEqual(ctx.exception.results, {'CAN_P1_DRIVER': 1})
        self.assertEqual(list(ctx.exception.errors.keys()), ['NO_SUCH_PARAM'])

    def test_param_errors_meant_for_others_are_ignored(self):
        driver = self.start()
        self.fake.hold = True
        cb = Recorder()
        driver.request_params(['CAN_P1_DRIVER'], cb)
        self.pump(0.1)
        # an error for another GCS, and one from a component that is not the autopilot
        self.fake.send_from(self.fake.sysid, self.fake.compid, 'param_error', 255, 190, b'CAN_P1_DRIVER', -1,
                            mavlink2.MAV_PARAM_ERROR_DOES_NOT_EXIST)
        self.fake.send_from(self.fake.sysid, 154, 'param_error', 250, mavlink2.MAV_COMP_ID_MAVCAN, b'CAN_P1_DRIVER',
                            -1, mavlink2.MAV_PARAM_ERROR_DOES_NOT_EXIST)
        self.pump(0.1)
        self.assertEqual(cb.calls, [])
        self.fake.release()
        self.pump_until(lambda: cb.calls)
        self.assertEqual(cb.calls[0], ({'CAN_P1_DRIVER': 1}, {}))


class CacheAndListenerTest(MAVCANTestCase):
    def test_cache_and_listeners_follow_autopilot_param_values(self):
        driver = self.start()
        heard = Recorder()
        driver.add_param_listener(heard)
        driver.get_param('CAN_P1_DRIVER')
        # unsolicited change, e.g. made by another ground station
        self.fake.send_from(self.fake.sysid, self.fake.compid, 'param_value', b'SERIAL1_BAUD', 38.0, INT32, 3, 1)
        self.pump_until(lambda: len(heard.calls) == 2)
        self.assertEqual(heard.calls, [('CAN_P1_DRIVER', 1), ('SERIAL1_BAUD', 38)])
        self.assertEqual(driver.params, {'CAN_P1_DRIVER': 1, 'SERIAL1_BAUD': 38})

    def test_param_values_from_other_components_and_systems_are_ignored(self):
        driver = self.start()
        heard = Recorder()
        driver.add_param_listener(heard)
        self.fake.send_from(self.fake.sysid, 154, 'param_value', b'MNT_TYPE', 1.0, INT8, 1, 0)
        self.fake.send_from(2, 1, 'param_value', b'OTHER_VEHICLE', 1.0, INT8, 1, 0)
        self.pump(0.2)
        self.assertEqual(heard.calls, [])
        self.assertEqual(driver.params, {})

    def test_listener_can_be_removed(self):
        driver = self.start()
        heard = Recorder()
        driver.add_param_listener(heard).remove()
        driver.get_param('CAN_P1_DRIVER')
        self.assertEqual(heard.calls, [])


class FetchAllTest(MAVCANTestCase):
    PARAMS = dict(('P%02d' % i, float(i)) for i in range(50))

    def test_fetch_all_fills_gaps(self):
        driver = self.start(FakeAutopilot(self.PARAMS))
        self.fake.drop_list_indices.update([3, 17, 40])
        cb, progress = Recorder(), Recorder()
        driver.fetch_all_params(cb, progress=progress, timeout=0.2)
        self.pump_until(lambda: cb.calls)
        params, missing = cb.calls[0]
        self.assertEqual(missing, [])
        self.assertEqual(params, self.PARAMS)
        self.assertEqual(progress.calls[-1], (50, 50))
        self.assertEqual(sorted(m.param_index for m in self.fake.sent('PARAM_REQUEST_READ')), [3, 17, 40])
        self.assertEqual(driver.params, self.PARAMS)

    def test_fetch_all_reports_missing_indices(self):
        driver = self.start(FakeAutopilot(self.PARAMS))
        self.fake.never_reply_indices.update([5, 6])
        cb = Recorder()
        driver.fetch_all_params(cb, timeout=0.2, retries=2)
        self.pump_until(lambda: cb.calls)
        params, missing = cb.calls[0]
        self.assertEqual(missing, [5, 6])
        self.assertEqual(len(params), 48)


class RebootTest(MAVCANTestCase):
    def test_reboot_clears_cache_and_fails_pending(self):
        driver = self.start()
        driver.get_param('CAN_P1_DRIVER')
        self.fake.hold = True
        cb = Recorder()
        driver.request_params(['SERIAL1_BAUD'], cb, timeout=5)
        self.pump(0.1)
        driver.reboot_autopilot()
        # CAN forwarding is re-enabled with COMMAND_LONG too, so pick out the reboot
        cmd = [m for m in self.fake.sent('COMMAND_LONG')
               if m.command == mavlink2.MAV_CMD_PREFLIGHT_REBOOT_SHUTDOWN][-1]
        self.assertEqual(cmd.param1, 1)
        self.assertEqual((cmd.target_system, cmd.target_component), (1, mavlink2.MAV_COMP_ID_AUTOPILOT1))
        self.pump_until(lambda: cb.calls)
        results, errors = cb.calls[0]
        self.assertEqual(results, {})
        self.assertIn('rebooting', errors['SERIAL1_BAUD'])
        self.assertEqual(driver.params, {})

    def test_reboot_rejected_raises(self):
        driver = self.start()
        self.fake.reboot_result = mavlink2.MAV_RESULT_DENIED
        with self.assertRaises(DriverError):
            driver.reboot_autopilot()

    def test_reboot_without_ack_raises(self):
        driver = self.start()
        self.fake.reboot_result = None
        with self.assertRaises(DriverError):
            driver.reboot_autopilot(timeout=0.3)


class CANForwardingTest(MAVCANTestCase):
    def test_can_frames_still_flow(self):
        driver = self.start()
        driver.send_frame(CANFrame(0x123, bytearray([1, 2, 3]), True))
        deadline = time.time() + 2
        while not self.fake.sent('CAN_FRAME') and time.time() < deadline:
            time.sleep(0.01)
        sent = self.fake.sent('CAN_FRAME')[0]
        self.assertEqual(sent.id & 0x1FFFFFFF, 0x123)
        self.fake.send_from(1, 1, 'can_frame', 250, 0, 0, 2, 0x456 | (1 << 31), [9, 8] + [0] * 6)
        frame = None
        while frame is None and time.time() < deadline:
            frame = driver.receive(timeout=0.1)
        self.assertEqual((frame.id, bytes(frame.data)), (0x456, b'\x09\x08'))


class OtherDriversTest(unittest.TestCase):
    def test_drivers_without_mavlink_do_not_support_params(self):
        driver = AbstractDriver()
        self.assertFalse(driver.supports_mavlink_params())
        for call in (lambda: driver.get_param('X'), lambda: driver.get_params(['X']),
                     lambda: driver.set_param('X', 1), lambda: driver.set_params({'X': 1}),
                     lambda: driver.request_params(['X'], None), lambda: driver.send_params({'X': 1}, None),
                     lambda: driver.fetch_all_params(None), lambda: driver.reboot_autopilot(),
                     lambda: driver.add_param_listener(None)):
            with self.assertRaises(DriverError):
                call()


if __name__ == '__main__':
    unittest.main()
