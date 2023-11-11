import logging
import time
from collections import namedtuple
import os
import datetime
import sys
import json
from pathlib import Path
import sqlite3
import schedule
import traceback
from gpiozero import DigitalOutputDevice

logging.basicConfig(stream=sys.stdout,
                    format='%(asctime)s - %(process)d / %(name)s [%(levelname)s]: %(message)s',
                    level=logging.DEBUG)

_LOGGER = logging.getLogger(__name__)

RF_QUEUE_FOLDER = '/data/queue'
DATABASE = '/data/biactrl.db'

dbcon = sqlite3.connect(DATABASE)

RFProtocol = namedtuple('RFProtocol',
                      ['pulselength', 'msg_length', 'repeat_delay',
                       'sync_count', 'sync_delay',
                       'sync_high', 'sync_low',
                       'zero_high', 'zero_low',
                       'one_high', 'one_low'])

PROTOCOLS = (None,
             RFProtocol(100, 40, 10000, 1, 0, 5000, 1500, 3, 8, 7, 4), # "home smart" shutter
             RFProtocol(300, 24, 0, 1, 0, 300, 9000, 1, 3, 3, 1) # LED controller
             )

DEVICE_CODES = {}

class RFDevice:

    def __init__(self, gpio=17):
        """Initialize the RF device."""
        self.gpio = gpio
        self.controller = DigitalOutputDevice(gpio, active_high=True, initial_value=False)
        _LOGGER.debug("Using GPIO " + str(gpio))

    def set_protocol(self, tx_proto, tx_repeat):
        self.gpio = 17
        self.tx_proto = tx_proto
        self.tx_repeat = tx_repeat
        self.tx_pulselength = PROTOCOLS[tx_proto].pulselength
        self.tx_repeat_delay = PROTOCOLS[tx_proto].repeat_delay
        self.tx_sync_count = PROTOCOLS[tx_proto].sync_count
        self.tx_sync_delay = PROTOCOLS[tx_proto].sync_delay
        self.tx_length = PROTOCOLS[tx_proto].msg_length

    def cleanup(self):
        """Disable TX and clean up GPIO."""
        _LOGGER.debug("Cleanup")
        self.controller.close()

    def tx_code(self, codes):
        """
        Send a decimal code.
        Optionally set protocol, pulselength and code length.
        When none given reset to default protocol, default pulselength and set code length to 40 bits.
        """
        rawcodes = []

        for code in codes:
            rawcode = format(code, '#0{}b'.format(self.tx_length + 2))[2:]
            _LOGGER.debug("TX code: " + str(rawcode))
            rawcodes.append(rawcode)
        return self._tx_bin(rawcodes)

    def _tx_bin(self, rawcodes):
        """Send a binary code, consider sync, delay and repeat parameters based on protocol."""
        #_LOGGER.debug("TX bin: {}" + str(rawcodes))
        sent = ""
        # calculate delay between different codes
        delay_between = 4 * (self.tx_pulselength * self.tx_length) + self.tx_sync_count * (self.tx_sync_delay + PROTOCOLS[self.tx_proto].sync_high + PROTOCOLS[self.tx_proto].sync_high)

        for code in rawcodes:
            for _ in range(0, self.tx_repeat):
                for x in list(range(self.tx_sync_count)):
                    if self._tx_sync():
                        sent += "$"
                    else:
                        return False
                if (self.tx_sync_delay > 0):
                    if (self._tx_delay(self.tx_sync_delay)):
                        sent += "&"
                    else:
                        return False
                for byte in range(0, self.tx_length):
                    if code[byte] == '0':
                        if self._tx_l0():
                            sent += "0"
                        else:
                            return False
                    else:
                        if self._tx_l1():
                            sent += "1"
                        else:
                            return False
                if (self.tx_repeat_delay > 0):
                    if self._tx_delay(self.tx_repeat_delay):
                        sent += '|'
                    else:
                        return False
            if self._tx_delay(delay_between):
                sent += "#"
            else:
                return False
        _LOGGER.debug("sent: {}".format(sent))
        return True

    def _tx_l0(self):
        """Send a '0' bit."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform(PROTOCOLS[self.tx_proto].zero_high,
                                PROTOCOLS[self.tx_proto].zero_low)

    def _tx_l1(self):
        """Send a '1' bit."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform(PROTOCOLS[self.tx_proto].one_high,
                                PROTOCOLS[self.tx_proto].one_low)

    def _tx_sync(self):
        """Send a sync."""
        if not 0 < self.tx_proto < len(PROTOCOLS):
            _LOGGER.error("Unknown TX protocol")
            return False
        return self._tx_waveform_irregular(PROTOCOLS[self.tx_proto].sync_high,
                                PROTOCOLS[self.tx_proto].sync_low)

    def _tx_delay(self, delay):
        """Wait between repeats."""
        self.controller.off()
        self._sleep((delay) / 1000000)
        return True

    def _tx_waveform(self, highpulses, lowpulses):
        """Send basic waveform."""
        self.controller.on()
        self._sleep((highpulses * self.tx_pulselength) / 1000000)
        self.controller.off()
        self._sleep((lowpulses * self.tx_pulselength) / 1000000)
        return True

    def _tx_waveform_irregular(self, highpulses, lowpulses):
        """Send waveform without using regular pulse length."""
        self.controller.on()
        self._sleep((highpulses) / 1000000)
        self.controller.off()
        self._sleep((lowpulses) / 1000000)
        return True

    def _sleep(self, delay):
        _delay = delay / 100
        end = time.time() + delay - _delay
        while time.time() < end:
            time.sleep(_delay)

    def tx_biactrl(self, xtype, device, command):
        command_list = []
        if (xtype == 'light'):
            tx_proto = 2
            repeat = 4
        elif (xtype == 'cover'):
            tx_proto = 1
            repeat = 6
        try:
            cmd = DEVICE_CODES.get(xtype).get(device).get(command)
            if (cmd):
                self.set_protocol(tx_proto, repeat)
                command_list.append(cmd)
                self.tx_code(command_list)
                _LOGGER.info(cmd)
            return True
        except Exception as e:
            _LOGGER.error(f"exception while sending command: {e}")
            _LOGGER.error(traceback.format_exc())
            return False

def update_cover_final_state(device, state):
    cur = dbcon.cursor()
    cur.execute(f"UPDATE devices SET state='{state}' WHERE type='cover' and id='{device}'")
    _LOGGER.info(f"updating {device} to {state}")
    dbcon.commit()
    return schedule.CancelJob

def update_device_state(xtype, device, cmd):
    cur = dbcon.cursor()
    if (xtype == 'cover'):
        # cancel if we have any pending jobs for this device
        selcur = dbcon.execute(f"SELECT state FROM devices where type='cover' and id='{device}'")
        current_state = selcur.fetchone()[0]
        jobs = schedule.get_jobs(device)
        for job in jobs:
            _LOGGER.info(f'cancelling job for device {device}')
            schedule.cancel_job(job)

        if (cmd == 'open'):
            cur.execute(f"UPDATE devices SET state='opening' WHERE type='cover' and id='{device}'")
            _LOGGER.info(f"updating {device} to opening")
            schedule.every().day.at(get_cover_update_time()).do(update_cover_final_state, device=device, state='open').tag(device)
        elif (cmd == 'close'):
            cur.execute(f"UPDATE devices SET state='closing' WHERE type='cover' and id='{device}'")
            _LOGGER.info(f"updating {device} to closing")
            schedule.every().day.at(get_cover_update_time()).do(update_cover_final_state, device=device, state='closed').tag(device)
        elif (cmd == 'stop'):
            if (current_state == 'closing' or current_state == 'opening'):
                cur.execute(f"UPDATE devices SET state='open' WHERE type='cover' and id='{device}'")
                _LOGGER.info(f"updating {device} to open")
    elif (xtype == 'light'):
        cur.execute(f"UPDATE devices SET state='{cmd}' WHERE type='light' and id='{device}'")
    dbcon.commit()

def verify_command(xtype, device, cmd):
    if (xtype not in DEVICE_CODES):
        return False
    if (device not in DEVICE_CODES.get(xtype)):
        return False
    if (cmd not in DEVICE_CODES.get(xtype).get(device)):
        return False
    return True

def setup_device_codes_from_db():
    result = dbcon.execute('SELECT d.type,d.id,c.command,c.code FROM devices d, commands c WHERE d.id=c.device_id')
    for row in result.fetchall():
        if not DEVICE_CODES.get(row[0]):
            DEVICE_CODES[row[0]] = {}
        if not DEVICE_CODES.get(row[0]).get(row[1]):
            DEVICE_CODES[row[0]][row[1]] = {}
        if not DEVICE_CODES.get(row[0]).get(row[1]).get(row[2]):
            DEVICE_CODES[row[0]][row[1]][row[2]] = {}
        DEVICE_CODES[row[0]][row[1]][row[2]] = row[3]
    dbcon.commit()
    _LOGGER.info("imported device codes: {}".format(DEVICE_CODES))

def get_cover_update_time():
    COVER_ROLL_TIME = 15
    return (datetime.datetime.now() + datetime.timedelta(seconds=COVER_ROLL_TIME)).strftime("%H:%M:%S")

def remove_queue_file(name):
    _LOGGER.info("removing file")
    ff = Path("{}/{}".format(RF_QUEUE_FOLDER, name))
    ff.unlink()

if __name__ == '__main__':

    rf = RFDevice()
    setup_device_codes_from_db()

    while True:
        for f in os.listdir(RF_QUEUE_FOLDER):
            try:
                with open("{}/{}".format(RF_QUEUE_FOLDER, f), "r") as cmd_file:
                    data = json.load(cmd_file)
                now = datetime.datetime.now()
                tstamp = datetime.datetime.fromtimestamp(data.get('time'))
                since = (now-tstamp).total_seconds()
                _LOGGER.info(f"happened {since} ago")
                if (since > 10):
                    remove_queue_file(f)
                else:
                    _LOGGER.info("processing file")
                    xtype = data.get('type')
                    device = data.get('device')
                    cmd = data.get('cmd')
                    if not verify_command(xtype, device, cmd):
                        remove_queue_file(f)
                        next
                    result = rf.tx_biactrl(xtype, device, cmd)
                    if result:
                        remove_queue_file(f)
                        _LOGGER.info("sent command")
                        update_device_state(xtype, device, cmd)
            except Exception as e:
                _LOGGER.error(str(e))
                _LOGGER.error(traceback.format_exc())
            finally:
                _LOGGER.info(f'Cleaning up GPIO ports!')

            time.sleep(0.2)
        time.sleep(0.1)
        schedule.run_pending()