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
                       'one_high', 'one_low', 'repeat_count'])

PROTOCOLS = (None,
             RFProtocol(20, 40, 10000, 1, 0, 5000, 1472, 17, 37, 35, 19, 6), # "home smart" shutter
             RFProtocol(300, 24, 0, 1, 0, 300, 9000, 1, 3, 3, 1, 4) # LED controller
             )

DEVICE_CODES = {}

class RFDevice:

    def __init__(self, gpio=17):
        """Initialize the RF device."""
        self.gpio = gpio
        self.controller = DigitalOutputDevice(gpio, active_high=True, initial_value=False)
        _LOGGER.debug("Using GPIO " + str(gpio))

    def set_protocol(self, tx_proto):
        self.gpio = 17
        self.tx_proto = tx_proto
        self.tx_repeat = PROTOCOLS[tx_proto].repeat_count
        self.tx_pulselength = PROTOCOLS[tx_proto].pulselength
        self.tx_repeat_delay = PROTOCOLS[tx_proto].repeat_delay
        self.tx_sync_count = PROTOCOLS[tx_proto].sync_count
        self.tx_sync_delay = PROTOCOLS[tx_proto].sync_delay
        self.tx_length = PROTOCOLS[tx_proto].msg_length

    def get_total_tx_length(self):
        return (self.tx_repeat * (
                    (self.tx_pulselength * self.tx_length) +
                    self.tx_sync_count * (self.tx_sync_delay + PROTOCOLS[self.tx_proto].sync_high + PROTOCOLS[self.tx_proto].sync_low)
                    )
                    + self.tx_repeat_delay * (self.tx_repeat - 1)
                )

    def cleanup(self):
        """Disable TX and clean up GPIO."""
        _LOGGER.debug("Cleanup")
        self.controller.close()

    def tx_code(self, code):
        """
        Send a decimal code.
        Optionally set protocol, pulselength and code length.
        When none given reset to default protocol, default pulselength and set code length to 40 bits.
        """

        rawcode = format(code, '#0{}b'.format(self.tx_length + 2))[2:]
        _LOGGER.debug("TX code: " + str(rawcode))
        return self._tx_bin(rawcode)

    def _tx_bin(self, rawcode):
        """Send a binary code, consider sync, delay and repeat parameters based on protocol."""
        #_LOGGER.debug("TX bin: {}" + str(rawcodes))
        sent = ""

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
                if rawcode[byte] == '0':
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
        time.sleep((delay) / 1000000.0)
        return True

    def _tx_waveform(self, highpulses, lowpulses):
        """Send basic waveform."""
        self.controller.on()
        time.sleep((highpulses * self.tx_pulselength) / 1000000.0)
        self.controller.off()
        time.sleep((lowpulses * self.tx_pulselength) / 1000000.0)
        return True

    def _tx_waveform_irregular(self, highpulses, lowpulses):
        """Send waveform without using regular pulse length."""
        self.controller.on()
        time.sleep((highpulses) / 1000000.0)
        self.controller.off()
        time.sleep((lowpulses) / 1000000.0)
        return True

    def tx_biactrl(self, xtype, device, command):
        if (xtype == 'light'):
            tx_proto = 2
        elif (xtype == 'cover'):
            tx_proto = 1
        try:
            cmd = DEVICE_CODES.get(xtype).get(device).get(command)
            if (cmd):
                self.set_protocol(tx_proto)
                _LOGGER.info(cmd)
                self.tx_code(cmd)
                time.sleep(2 * (rf.get_total_tx_length() / 1000000.0))
                return True
            return False
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
        if ('_all' in device):
            group = device.split('_')[0]
            group_devs_cur = dbcon.execute(f"SELECT id FROM devices where type='cover' and id like '{group}_%' and id not like '%_all'")
            for group_dev in group_devs_cur.fetchall():
                update_device_state(xtype, group_dev, cmd)
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
    try:
        cmd = DEVICE_CODES.get(xtype).get(device).get(cmd)
        return True
    except:
        return False

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
                _LOGGER.info(data)
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
        
        time.sleep(0.1)
        schedule.run_pending()
        