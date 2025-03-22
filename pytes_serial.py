#!/usr/bin/env python
import traceback
import logging
from logging.handlers import RotatingFileHandler
import serial
import time, datetime
import json
import mysql.connector as mariadb
from configparser import ConfigParser
import paho.mqtt.client as mqtt 
from paho.mqtt.client import CallbackAPIVersion
import re
import os

# Get the current working directory
current_working_directory = os.getcwd()

# Check if config file exists
config_file = os.path.join(current_working_directory, 'pytes_serial.cfg')
if not os.path.exists(config_file):
    print(f"Config file '{config_file}' not found. Exiting.")
    exit(1)
    
# ---------------------------variables initialization----------
config                = ConfigParser()
config.read('pytes_serial.cfg')

serial_port           = config.get('serial', 'serial_port')
serial_baudrate       = int(config.get('serial', 'serial_baudrate'))
reading_freq          = int(config.get('serial', 'reading_freq'))
output_path           = config.get('general', 'output_path')
powers                = int(config.get('battery_info', 'powers'))
cells                  = int(config.get('battery_info', 'cells'))
dev_name              = config.get('battery_info', 'dev_name')
manufacturer          = config.get('battery_info', 'manufacturer')
model                 = config.get('battery_info', 'model')
sw_ver                = "PytesSerial v0.9.0_20250321"
version               = sw_ver

if reading_freq < 5  : reading_freq = 5

SQL_active            = config.get('Maria DB connection', 'SQL_active')
host                  = config.get('Maria DB connection', 'host')
db_port               = config.get('Maria DB connection', 'db_port')
user                  = config.get('Maria DB connection', 'user')
password              = config.get('Maria DB connection', 'password')
database              = config.get('Maria DB connection', 'database')

MQTT_active           = config.get('MQTT', 'MQTT_active')
MQTT_broker           = config.get('MQTT', 'MQTT_broker')
MQTT_port             = int(config.get('MQTT', 'MQTT_port'))
MQTT_username         = config.get('MQTT', 'MQTT_username')
MQTT_password         = config.get('MQTT', 'MQTT_password')

LOGGING_LEVEL          = config.get('logging', 'LOGGING_LEVEL')
log_level_info = {'logging.DEBUG': logging.DEBUG,
                'logging.INFO': logging.INFO,
                'logging.WARNING': logging.WARNING,
                'logging.ERROR': logging.ERROR,
                }
LOGGING_LEVEL_FILE     = (log_level_info[LOGGING_LEVEL])
LOGGING_FILE_MAX_SIZE  = int(config.get('logging', 'LOGGING_FILE_MAX_SIZE'))
LOGGING_FILE_MAX_FILES = int(config.get('logging', 'LOGGING_FILE_MAX_FILES'))

cells_monitoring       = config.get('cells_monitoring', 'cells_monitoring')
cells_mon_level        = config.get('cells_monitoring', 'monitoring_level')

events_monitoring      = config.get('events_monitoring', 'events_monitoring')
events_mon_level       = config.get('events_monitoring', 'monitoring_level')
cells_details          = config.get('events_monitoring', 'cells_details')

parsing_stat_interval = int(config.get('stat_parsing', 'parsing_stat_interval'))

start_time            = time.time()                         # init time
up_time               = time.time()                         # used to calculate uptime
pwr                   = []                                  # used to serialise JSON data
bat                   = []                                  # used to record cells data -- def parsing_bat
bats                  = []                                  # used to serialise JSON data -- def check_cells
loops_no              = 0                                   # used to count no of loops and to calculate % of errors
errors_no             = 0                                   # used to count no of errors and to calculate %
trials                = 0                                   # used to improve data reading accuracy -- def parsing_serial
errors                = 'false'
line_str_array        = []                                  # type: list[str] # used to get line strings from serial
bat_events_no         = 0                                   # used to count numbers of battery events
pwr_events_no         = 0                                   # used to count numbers of power events
sys_events_no         = 0                                   # used to count numbers of system events
parsing_stat_lastexec = 0                                   # used to calculate parsing_stat execution times

power_events_list = {
0:["info","0x0","No events"],
1:["warning","0x1","Overvoltage alarm"],
2:["warning","0x2","High voltage alarm"],
4:["info","0x4","*tbc*The voltage is normal"],
8:["warning","0x8","*tbc*Low voltage alarm"],
16:["warning","0X10","*tbc*Under voltage alarm"],
32:["warning","0x20","*tbc*Cell sleep"],
64:["warning","0X40","*tbc*Battery life alarm 1"],
128:["warning","0x80","*tbc*System startup"],
256:["warning","0x100","*tbc*Over temperature alarm"],
512:["warning","0x200","*tbc*High temperature alarm"],
1024:["warning","0x400","*tbc*Temperature is normal"],
2048:["warning","0x800","*tbc*Low temperature alarm"],
4096:["warning","0x1000","*tbc*Under temperature alarm"],
8192:["info","0x2000","Full charge"],
16384:["info","0x4000","Normal power"],
32768:["warning","0x8000","*tbc*Low power"],
65536:["warning","0x10000","*tbc*Short circuit protection"],
131072:["warning","0x20000","*tbc*Discharge overcurrent protection 2"],
262144:["warning","0x40000","*tbc*Charging overcurrent protection 2"],
524288:["warning","0x80000","*tbc*Discharge overcurrent protection"],
1048576:["warning","0x100000","*tbc*Charging overcurrent protection"],
2097152:["info","0x200000","System idle"],
4194304:["info","0x400000","Charging"],
8388608:["info","0x800000","Discharging"],
16777216:["warning","0x1000000","*tbc*System power failure"],
33554432:["warning","0x2000000","*tbc*System idle"],
67108864:["warning","0x4000000","*tbc*Charging"],
134217728:["warning","0x8000000","*tbc*Discharging"],
268435456:["warning","0x10000000","*tbc*System error"],
536870912:["warning","0x20000000","*tbc*System hibernation"],
1073741824:["warning","0x40000000","*tbc*System shutdown"],
2147483648:["warning","0x80000000","*tbc*Battery life alarm 2"]
}

sys_events_list = {
0:["info","0x0","No events"],
1:["warning","0x1","Reverse connection of external power input"],
2:["warning","0x2","External power input overvoltage"],
4:["warning","0x4","Current detection error"],
8:["warning","0x8","OZ abnormal"],
16:["warning","0x10","Sleep module abnormal"],
32:["warning","0x20","temperature sensor error"],
64:["warning","0x40","Voltage detection error"],
128:["warning","0x80","I2C bus error"],
256:["warning","0x100","CAN bus address assignment error"],
512:["warning","0x200","Internal CAN bus communication error"],
1024:["warning","0x400","Charge MOS FAIL"],
2048:["warning","0x800","Discharge MOS FAIL"]
}

# ------------------------logging definiton ----------------------------
formatter = logging.Formatter('%(asctime)s | %(levelname)7s | %(message)s ',datefmt='%Y%m%d %H:%M:%S') # logging formating
def setup_logger(name, log_file, level=LOGGING_LEVEL_FILE):

    """To setup as many loggers as you want"""
    fileHandler = RotatingFileHandler(log_file, mode='a', maxBytes=LOGGING_FILE_MAX_SIZE*1000, backupCount=LOGGING_FILE_MAX_FILES, encoding=None, delay=False)
    fileHandler.setFormatter(formatter)
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)  

    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(fileHandler)
    logger.addHandler(console_handler)

    return logger

pytes_serial_log    = setup_logger('pytes_serial', 'pytes_serial.log')
battery_events_log  = setup_logger('battery_events', 'battery_events.log')

# ------------------------functions area----------------------------
def serial_sendcommand(req, start, stop, timeout=10):
    global errors
    
    # Define a simple result class
    class CommandResult:
        def __init__(self, success, response=None):
            self.success  = success
            self.response = response
    
    try:
        serial_exec_time = time.time()
        start_time = time.time()
        line_str_array = []
        orig_start = start
        orig_stop = stop

        # Ensure serial port is open
        if not ser.is_open:
            ser.open()
            time.sleep(0.5)
            pytes_serial_log.debug('...opened serial port')

        # Clear buffers
        ser.reset_input_buffer()
        ser.reset_output_buffer()

        # Send command
        bytes_req = str(req).encode('latin-1')
        ser.write(bytes_req + b'\n')
        ser.flush()

        # Wait for initial response
        while (time.time() - start_time) < 1:
            if ser.in_waiting > 0:
                pytes_serial_log.debug(f'...command sent, buffer has data after {round(time.time() - start_time, 2)}s')
                break
            time.sleep(0.1)
        else:
            pytes_serial_log.debug('...timeout waiting for initial response')
            return CommandResult(False)

        # Read response
        start_time = time.time()
        found_start = False if start != 'none' else True
        
        while (time.time() - start_time) < timeout:
            if ser.in_waiting > 0:
                line = ser.read_until(b'\n')
                try:
                    decoded_line = line.decode('latin-1').strip()
                except UnicodeDecodeError:
                    pytes_serial_log.warning(f"Failed to decode line: {line}")
                    continue

                # Handle start/stop conditions
                if not found_start and start in decoded_line:
                    found_start = True

                if found_start:
                    line_str_array.append(decoded_line)
                    
                    if stop in decoded_line:
                        pytes_serial_log.debug(f'...found stop condition after {round(time.time() - start_time, 2)}s, total exec_time: {round(time.time() - serial_exec_time, 2)}s')
                        return CommandResult(True, line_str_array)

            time.sleep(0.01)

        pytes_serial_log.debug('...timeout reached')
        return CommandResult(False)

    except Exception as e:
        errors = True
        pytes_serial_log.warning(
            f"SERIAL SENDCOMMAND serial_sendcommand('{req}', '{orig_start}', '{orig_stop}') - "
            f"error: {str(e)}"
        )
        pytes_serial_log.debug(f'SERIAL SENDCOMMAND - array: {line_str_array}')
        return CommandResult(False)

def seconds_to_duration(seconds):
    if not isinstance(seconds, (int, float)) or seconds < 0:
        return "Invalid input"
    
    seconds = int(seconds)  # Convert to integer for simplicity
    
    days = seconds // (24 * 3600)
    seconds %= (24 * 3600)
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    
    parts = []
    if days > 0:
        parts.append(f"{days} day{'s' if days != 1 else ''}")
    if hours > 0:
        parts.append(f"{hours} hour{'s' if hours != 1 else ''}")
    if minutes > 0:
        parts.append(f"{minutes} minute{'s' if minutes != 1 else ''}")
    if seconds > 0 or not parts:  # Include seconds if no larger units
        parts.append(f"{seconds} second{'s' if seconds != 1 else ''}")
    
    return ", ".join(parts)
    
def parsing_pwrsys():
    try:
        global line_str_array
        global errors
        global banks_total              
        global banks_current            
        global banks_sleep              
        global system_voltage              
        global system_current           
        global system_rc                
        global system_fcc              
        global system_soc               
        global system_soh               
        global system_highest_voltage   
        global system_average_voltage   
        global system_lowest_voltage    
        global system_highest_temp      
        global system_average_temp      
        global system_lowest_temp       
        global system_recommend_chg_volt
        global system_recommend_dsg_volt
        global system_recommend_chg_curr
        global system_recommend_dsg_curr

        banks_total = 0
        banks_current = 0
        banks_sleep = 0
        system_voltage = 0
        system_current = 0
        system_rc = 0
        system_fcc = 0
        system_soc = 0
        system_soh = 0
        system_highest_voltage = 0
        system_average_voltage = 0
        system_lowest_voltage = 0
        system_highest_temp = 0
        system_average_temp = 0
        system_lowest_temp = 0
        system_recommend_chg_volt = 0
        system_recommend_dsg_volt = 0
        system_recommend_chg_curr = 0
        system_recommend_dsg_curr = 0
        
        req  = ('pwrsys')
        result = serial_sendcommand(req, 'Power System Information','Command completed')

        # Check and use the result
        if result.success == False:
            return "false"

        line_str_array = result.response
        for line_str in line_str_array:
            #parsing data
            if 'Total Num                :' in line_str: banks_total                  = int(line_str[27:36])
            if 'Present Num              :' in line_str: banks_current                = int(line_str[27:36])
            if 'Sleep Num                :' in line_str: banks_sleep                  = int(line_str[27:36])
            if 'System Volt              :' in line_str: system_voltage               = round(int(line_str[27:36])/1000, 3)
            if 'System Curr              :' in line_str: system_current               = round(int(line_str[27:36])/1000, 3)
            if 'System RC                :' in line_str: system_rc                    = round(int(line_str[27:36])/1000, 3)
            if 'System FCC               :' in line_str: system_fcc                   = round(int(line_str[27:36])/1000, 3)
            if 'System SOC               :' in line_str: system_soc                   = int(line_str[27:36])
            if 'System SOH               :' in line_str: system_soh                   = int(line_str[27:36])
            if 'Highest voltage          :' in line_str: system_highest_voltage       = round(int(line_str[27:36])/1000, 3)
            if 'Average voltage          :' in line_str: system_average_voltage       = round(int(line_str[27:36])/1000, 3)
            if 'Lowest voltage           :' in line_str: system_lowest_voltage        = round(int(line_str[27:36])/1000, 3)
            if 'Highest temperature      :' in line_str: system_highest_temp          = round(int(line_str[27:36])/1000, 3)
            if 'Average temperature      :' in line_str: system_average_temp          = round(int(line_str[27:36])/1000, 3)
            if 'Lowest temperature       :' in line_str: system_lowest_temp           = round(int(line_str[27:36])/1000, 3)
            if 'Recommend chg voltage    :' in line_str: system_recommend_chg_volt    = round(int(line_str[27:36])/1000, 3)
            if 'Recommend dsg voltage    :' in line_str: system_recommend_dsg_volt    = round(int(line_str[27:36])/1000, 3)
            if 'Recommend chg current    :' in line_str: system_recommend_chg_curr    = round(int(line_str[27:36])/1000, 3)
            if 'Recommend dsg current    :' in line_str: system_recommend_dsg_curr    = round(int(line_str[27:36])/1000, 3)

            if 'Command completed' in line_str:   # mark end of the block
                try:
                    pytes_serial_log.debug ('--------- PWRSYS ----------')
                    pytes_serial_log.debug (f'banks_total               : {banks_total}')
                    pytes_serial_log.debug (f'banks_current             : {banks_current}')
                    pytes_serial_log.debug (f'banks_sleep               : {banks_sleep}')
                    pytes_serial_log.debug (f'system_volt               : {system_voltage}')
                    pytes_serial_log.debug (f'system_current            : {system_current}')
                    pytes_serial_log.debug (f'system_rc                 : {system_rc}')
                    pytes_serial_log.debug (f'current_fcc               : {system_fcc}')
                    pytes_serial_log.debug (f'system_soc                : {system_soc}')
                    pytes_serial_log.debug (f'system_soh                : {system_soh}')
                    pytes_serial_log.debug (f'system_highest_voltage    : {system_highest_voltage}')
                    pytes_serial_log.debug (f'system_average_voltage    : {system_average_voltage}')
                    pytes_serial_log.debug (f'system_lowest_voltage     : {system_lowest_voltage}')
                    pytes_serial_log.debug (f'system_highest_temp       : {system_highest_temp}')
                    pytes_serial_log.debug (f'system_average_temp       : {system_average_temp}')
                    pytes_serial_log.debug (f'system_lowest_temp        : {system_lowest_temp}')
                    pytes_serial_log.debug (f'system_recommend_chg_volt : {system_recommend_chg_volt}')
                    pytes_serial_log.debug (f'system_recommend_dsg_volt : {system_recommend_dsg_volt}')
                    pytes_serial_log.debug (f'system_recommend_chg_curr : {system_recommend_chg_curr}')
                    pytes_serial_log.debug (f'system_recommend_dsg_curr : {system_recommend_dsg_curr}')
                    pytes_serial_log.debug ('---------------------------')

                    line_str_array = []
                    line_str       = ""

                    break

                except Exception as e:
                    errors = 'true'
                    pytes_serial_log.warning ('PARSING SERIAL - error handling message: '+str(e))

        pytes_serial_log.info ('...parsing_pwrsys: ok')
        return "true"

    except Exception as e:
        errors = 'true'
        pytes_serial_log.info ('PARSING BAT - error handling message: ' + str(e))

def parsing_serial():
    try:
        global errors
        global trials
        global pwr
        volt_st      = None
        current_st   = None
        temp_st      = None
        coul_st      = None
        soh_st       = None
        heater_st    = None
        bat_events   = None
        power_events = None
        sys_events   = None

        data_set           = 0
        pwr                = []

        for power in range (1, powers + 1):
            req  = ('pwr '+ str(power))

            result = serial_sendcommand(req, req, 'Command completed')
            if result.success == False:
                pytes_serial_log.warning ('...parsing_serial: error')
                return "false"
            
            for line_str in result.response:
                if 'Voltage         :' in line_str: voltage      = round(int(line_str[18:26])/1000, 2)
                if 'Current         :' in line_str: current      = round(int(line_str[18:26])/1000, 2)
                if 'Temperature     :' in line_str: temp         = round(int(line_str[18:26])/1000, 1)
                if 'Coulomb         :' in line_str: soc          = int(line_str[18:26])
                if 'Basic Status    :' in line_str: basic_st     = line_str[18:26]
                if 'Volt Status     :' in line_str: volt_st      = line_str[18:26]
                if 'Current Status  :' in line_str: current_st   = line_str[18:26]
                if 'Tmpr. Status    :' in line_str: temp_st      = line_str[18:26]
                if 'Coul. Status    :' in line_str: coul_st      = line_str[18:26]
                if 'Soh. Status     :' in line_str: soh_st       = line_str[18:26]
                if 'Heater Status   :' in line_str: heater_st    = line_str[18:26]
                if 'Bat Events      :' in line_str: bat_events   = int(line_str[18:27],16)
                if 'Power Events    :' in line_str: power_events = int(line_str[18:27],16)
                if 'System Fault    :' in line_str: sys_events   = int(line_str[18:27],16)

                if 'Command completed' in line_str:   # mark end of the block
                    try:
                        pytes_serial_log.debug (f'--------- PWR ({power}) ----------')
                        pytes_serial_log.debug (f'Voltage         : {voltage}')
                        pytes_serial_log.debug (f'Current         : {current}')
                        pytes_serial_log.debug (f'Temperature     : {temp}')
                        pytes_serial_log.debug (f'SOC [%]         : {soc}')
                        pytes_serial_log.debug (f'Basic Status    : {basic_st}')
                        pytes_serial_log.debug (f'Volt Status     : {volt_st}')
                        pytes_serial_log.debug (f'Current Status  : {current_st}')
                        pytes_serial_log.debug (f'Tmpr. Status    : {temp_st}')
                        pytes_serial_log.debug (f'Coul. Status    : {coul_st}')
                        pytes_serial_log.debug (f'Soh. Status     : {soh_st}')
                        pytes_serial_log.debug (f'Heater Status   : {heater_st}')
                        pytes_serial_log.debug (f'Bat Events      : {bat_events}')
                        pytes_serial_log.debug (f'Power Events    : {power_events}')
                        pytes_serial_log.debug (f'System Fault    : {sys_events}')
                        pytes_serial_log.debug ('---------------------------')

                        pwr_array = {
                                    'power': power,
                                    'voltage': voltage,
                                    'current': current,
                                    'temperature': temp,
                                    'soc': soc,
                                    'basic_st': basic_st,
                                    'volt_st': volt_st,
                                    'current_st': current_st,
                                    'temp_st':temp_st,
                                    'soh_st':soh_st,
                                    'coul_st': coul_st,
                                    'heater_st': heater_st,
                                    'bat_events': bat_events,
                                    'power_events': power_events,
                                    'sys_events': sys_events}
                        
                        data_set       = data_set + 1
                        pwr.append(pwr_array)
                        pytes_serial_log.info(f'...parsing_serial: power {power} - ok')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               

                        break

                    except Exception as e:
                        pytes_serial_log.warning ('PARSING SERIAL - error handling message: '+str(e))

            if data_set != power:
                break

        if data_set == powers:
            statistics()
            errors='false'
            trials=0

            pytes_serial_log.info(f'...parsing_serial: ok')
            return "true"

        else:
            pytes_serial_log.warning (f'...parsing_serial: data_set({data_set}) != powers({powers}) error')
            errors = 'true'
            return

    except Exception as e:
        errors = 'true'

        pytes_serial_log.error ('PARSING SERIAL - error handling message: '+str(e))

        if ser.is_open == True:
            ser.close()
            pytes_serial_log.debug ('...close serial')

        return

def statistics():
    try:
        global errors
        global sys_voltage
        global sys_current
        global sys_soc
        global sys_temp
        global sys_basic_st
        sys_voltage  = 0
        sys_current  = 0
        sys_soc      = 0
        sys_temp     = 0
        sys_basic_st = ""

        for power in range (1, powers+1):
            sys_voltage       = sys_voltage + pwr[power-1]['voltage']             # voltage will be the average of all batteries
            sys_current       = round((sys_current + pwr[power-1]['current']),1)  # current will be sum of all banks
            sys_soc           = sys_soc + pwr[power-1]['soc']                     # soc will be the average of all batteries
            sys_temp          = sys_temp + pwr[power-1]['temperature']            # temperature will be the average of all batteries

        sys_voltage  = round((sys_voltage / powers), 1)
        sys_soc      = int(sys_soc / powers)
        sys_basic_st = pwr[0]['basic_st']                                         # status will be the master status
        sys_temp     = round((sys_temp / powers), 1)
        
    except Exception as e:
        errors = 'true'
        pytes_serial_log.exception ('...json serialization error: ' + str(e))

def json_serialize():
    try:
        global parsing_time
        global loops_no
        global errors_no
        global errors
        global json_data
        global json_data_old
        global bat_events_no
        global pwr_events_no
        global sys_events_no
        global bats

        json_data_old = json_data
        json_data={'relay_local_time':TimeStamp,
                   'powers' : banks_current,
                   'powers_total' : banks_total,
                   'powers_sleep' : banks_sleep,
                   'voltage': system_voltage,
                   'current': system_current,
                   'system_rc': system_rc,
                   'system_fcc': system_fcc,
                   'temperature': sys_temp,
                   'soc': system_soc,
                   'soh': system_soh,
                   'highest_voltage': system_highest_voltage,
                   'average_voltage': system_average_voltage,
                   'lowest_voltage': system_lowest_voltage,
                   'highest_temp': system_highest_temp,
                   'average_temp': system_average_temp,
                   'lowest_temp': system_lowest_temp,
                   'recommend_chg_volt': system_recommend_chg_volt,
                   'recommend_dsg_volt': system_recommend_dsg_volt,
                   'recommend_chg_curr': system_recommend_chg_curr,
                   'recommend_dsg_curr': system_recommend_dsg_curr,
                   'basic_st': sys_basic_st,
                   'devices':pwr,
                   'cells_data':bats,
                   'serial_stat': {'uptime':uptime,
                                   'loops':loops_no,
                                   'errors': errors_no,
                                   'bat_events_no': bat_events_no,
                                   'pwr_events_no': pwr_events_no,
                                   'sys_events_no': sys_events_no,
                                   'efficiency' :round((1-(errors_no/loops_no))*100,2),
                                   'ser_round_trip':round(parsing_time,2)}
                   }

        with open(output_path + dev_name + '_status.json', 'w') as outfile:
            json.dump(json_data, outfile)
        pytes_serial_log.debug ('...json creation:  ok')

    except Exception as e:
        pytes_serial_log.error ('JSON SERIALIZATION - error handling message: ' + str(e))

        errors = 'true'

def maria_db():
    try:
        mydb = mariadb.connect(host=host,port=db_port,user=user,password=password,database=database)

        for power in range (1, powers+1):
            values= (pwr[power-1]['power'],
                     pwr[power-1]['voltage'],
                     pwr[power-1]['current'],
                     pwr[power-1]['temperature'],
                     pwr[power-1]['soc'],
                     pwr[power-1]['basic_st'],
                     pwr[power-1]['volt_st'],
                     pwr[power-1]['current_st'],
                     pwr[power-1]['temp_st'],
                     pwr[power-1]['coul_st'],
                     pwr[power-1]['soh_st'],
                     pwr[power-1]['heater_st'],
                     pwr[power-1]['bat_events'],
                     pwr[power-1]['power_events'],
                     pwr[power-1]['sys_events'])

            sql="INSERT INTO pwr_data\
            (power,\
            voltage,current,\
            temperature,\
            soc,\
            basic_st,\
            volt_st,\
            current_st,\
            temp_st,\
            coul_st,\
            soh_st,\
            heater_st,\
            bat_events,\
            power_events,\
            sys_events) \
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor = mydb.cursor()
            mycursor.execute(sql, values)
            mydb.commit()

        mycursor.close()
        mydb.close()
        pytes_serial_log.debug ('...mariadb upload: ok')

    except Exception as e:
        pytes_serial_log.debug ('...mariadb writing error: '+ str(e))
        pytes_serial_log.warning ('MARIADB WRITING - error handling message: '+ str(e))

def mqtt_discovery():
    try:
        config    = 1
        max_config= 0
        msg       = {}

        # define system sensors
        names        =["Current",       "Voltage" ,     "Temperature",  "SOC",          "Status",   "Remaining Capacity",   "Full Charge Capacity",   "System SOH",      "Last update"]
        ids          =["current",       "voltage" ,     "temperature",  "soc",          "basic_st", "system_rc",            "system_fcc",             "soh",             "relay_local_time"]
        dev_cla      =["current",       "voltage",      "temperature",  "battery",      None,       "battery",              "battery",                "battery",         "timestamp"]
        stat_cla     =["measurement",   "measurement",  "measurement",  "measurement",  None,       "measurement",          "measurement",            "measurement",     None]
        unit_of_meas =["A",             "V",            "°C",           "%",            None,       "AH",                   "AH",                     "%",               None]

        max_config   = max_config + len(ids)

        for n in range(len(ids)):
            msg ["uniq_id"]      = dev_name + "_" + ids[n]
            state_topic          = "homeassistant/sensor/" + dev_name + "/" + msg["uniq_id"] + "/config"
            msg ["name"]         = names[n]
            msg ["stat_t"]       = "pytes_serial/" + dev_name + "/" + ids[n]
            if dev_cla[n]  != None:
                msg ["dev_cla"]  = dev_cla[n]
            if stat_cla[n] != None:
                msg ["stat_cla"] = stat_cla[n]
            if unit_of_meas[n] != None:
                msg ["unit_of_meas"] = unit_of_meas[n]

            msg ["val_tpl"]      = "{{ value_json.value }}"
            msg ["dev"]          = {"identifiers": [dev_name],"manufacturer": manufacturer,"model": model,"name": dev_name,"sw_version": sw_ver}
            message              = json.dumps(msg)

            client.publish(state_topic, message, retain=True)

            b = "...mqtt auto discovery - system sensors:" + str(round(config/max_config *100)) +" %"
            print (b, end="\r")

            msg                  = {}
            config               = config +1

        pytes_serial_log.debug ("...mqtt auto discovery")

        # define individual batteries sensors
        names        =["Current",       "Voltage" ,     "Temperature",  "SOC",          "Status",   "Cycles"]
        ids          =["current",       "voltage" ,     "temperature",  "soc",          "basic_st", "cycle_times"]
        dev_cla      =["current",       "voltage",      "temperature",  "battery",      None,       None]
        stat_cla     =["measurement",   "measurement",  "measurement",  "measurement",  None,       None]
        unit_of_meas =["A",             "V",            "°C",           "%",            None,       None]

        max_config   = max_config + powers*len(ids)

        for power in range (1, powers+1):
            for n in range(len(ids)):
                msg ["uniq_id"]      = dev_name + "_" + ids[n] +"_" + str(power)
                state_topic          = "homeassistant/sensor/" + dev_name + "/" + msg["uniq_id"] + "/config"
                msg ["name"]         = names[n]+"_"+str(power)
                msg ["stat_t"]       = "pytes_serial/" + dev_name + "/" + str(power-1) + "/" + ids[n]
                if dev_cla[n] != None:
                    msg ["dev_cla"]  = dev_cla[n]
                if stat_cla[n] != None:
                    msg ["stat_cla"]  = stat_cla[n]
                if unit_of_meas[n] != None:
                    msg ["unit_of_meas"] = unit_of_meas[n]

                msg ["val_tpl"]      = "{{ value_json.value }}"
                msg ["dev"]          = {"identifiers": [dev_name],"manufacturer": manufacturer,"model": model,"name": dev_name,"sw_version": sw_ver}
                message              = json.dumps(msg)

                client.publish(state_topic, message, retain=True)

                b = "...mqtt auto discovery - battery sensors:" + str(round(config/max_config *100)) +" %"
                print (b, end="\r")

                msg                  ={}
                config               = config +1
                #max_config           = len(ids)+ powers*len(ids)

        pytes_serial_log.debug ("...mqtt auto discovery")

        # define individual cells sensors
        if cells_monitoring == 'true':
            # individual sensors based on monitoring level
            if cells_mon_level == 'high':
                names        =["voltage",       "temperature",  "soc",          "status",   "volt_st",  "curr_st",  "temp_st"]
                ids          =["voltage",       "temperature",  "soc",          "basic_st", "volt_st",  "curr_st",  "temp_st"]
                dev_cla      =["voltage",       "temperature",  "battery",      None,       None,       None,       None]
                stat_cla     =["measurement",   "measurement",  "measurement",  None,       None,       None,       None]
                unit_of_meas =["V",             "°C",           "%",            None,       None,       None,       None]
                
            elif cells_mon_level == 'medium':
                names        =["voltage",       "temperature",  "volt_st"]
                ids          =["voltage",       "temperature",  "volt_st"]
                dev_cla      =["voltage",       "temperature",       None]
                stat_cla     =["measurement",   "measurement",       None]
                unit_of_meas =["V",             "°C",                None]
                
            else:
                names        =["voltage"]
                ids          =["voltage"]
                dev_cla      =["voltage"]
                stat_cla     =["measurement"]
                unit_of_meas =["V"]            
            
            max_config   = max_config + powers*len(ids)*cells

            for power in range (1, powers+1):
                for n in range(len(ids)):
                    for cell in range(1, cells+1):
                        if cell < 10:
                            cell_no ="0" + str(cell)
                        else:
                            cell_no ="" + str(cell)

                        msg ["uniq_id"]      = dev_name + "_" + ids[n] + "_" + str(power) + cell_no
                        state_topic          = "homeassistant/sensor/" + dev_name + "/" + msg["uniq_id"] + "/config"
                        msg ["name"]         = names[n]+"_"+str(power) + cell_no
                        msg ["stat_t"]       = "pytes_serial/" + dev_name + "/" + str(power-1) + "/cells/" + str(cell-1) + "/" + ids[n]
                        if dev_cla[n] != None:
                            msg ["dev_cla"]  = dev_cla[n]
                        if stat_cla[n] != None:
                            msg ["stat_cla"]  = stat_cla[n]
                        if unit_of_meas[n] != None:
                            msg ["unit_of_meas"] = unit_of_meas[n]

                        msg ["val_tpl"]      = "{{ value_json.value }}"
                        msg ["dev"]          = {"identifiers": [dev_name+"_cells"],"manufacturer": manufacturer,"model": model,"name": dev_name+"_cells","sw_version": sw_ver}
                        message              = json.dumps(msg)

                        client.publish(state_topic, message, retain=True)

                        b = "...mqtt auto discovery - cell sensors:" + str(round(config/max_config *100)) +" %"
                        print (b, end="\r")

                        msg                  ={}
                        config               = config +1
                        
            # only for medium and high monitoring level
            if cells_mon_level == 'medium' or cells_mon_level == 'high':
                
                pytes_serial_log.debug ("...mqtt auto discovery")
                
                # define individual cells sensors -- statistics
                names        =["voltage_delta", "voltage_min",  "voltage_max",  "temperature_delta",    "temperature_min",  "temperature_max"]
                ids          =["voltage_delta", "voltage_min",  "voltage_max",  "temperature_delta",    "temperature_min",  "temperature_max"]
                dev_cla      =["voltage",       "voltage",      "voltage",      "temperature",          "temperature",      "temperature"]
                stat_cla     =["measurement",   "measurement",  "measurement",  "measurement",          "measurement",      "measurement"]
                unit_of_meas =["V",             "V",            "V",            "°C",                   "°C",               "°C"]

                max_config   = max_config + powers*len(ids)

                for power in range (1, powers+1):
                    for n in range(len(ids)):
                        msg ["uniq_id"]      = dev_name + "_" + ids[n] + "_" + str(power)
                        state_topic          = "homeassistant/sensor/" + dev_name + "/" + msg["uniq_id"] + "/config"
                        msg ["name"]         = names[n]+"_"+str(power)
                        msg ["stat_t"]       = "pytes_serial/" + dev_name + "/" + str(power-1) + "/cells/" + ids[n]
                        if dev_cla[n] != None:
                            msg ["dev_cla"]  = dev_cla[n]
                        if stat_cla[n] != None:
                            msg ["stat_cla"]  = stat_cla[n]
                        if unit_of_meas[n] != None:
                            msg ["unit_of_meas"] = unit_of_meas[n]

                        msg ["val_tpl"]      = "{{ value_json.value }}"
                        msg ["dev"]          = {"identifiers": [dev_name+"_cells"],"manufacturer": manufacturer,"model": model,"name": dev_name+"_cells","sw_version": sw_ver}
                        message              = json.dumps(msg)

                        client.publish(state_topic, message, retain=True)

                        b = "...mqtt auto discovery - statistics sensors:" + str(round(config/max_config *100)) +" %"
                        print (b, end="\r")

                        msg                  ={}
                        config               = config +1

        pytes_serial_log.debug ("...mqtt auto discovery")

    except Exception as e:
        pytes_serial_log.warning ('MQTT DISCOVERY - error handling message: '  + str(e))

def mqtt_publish():
    try:
        # Publish system topics
        for key, value in json_data.items():
            # We will publish these later
            if key in ["devices", "cells_data"]:
                continue

            # If the value was published before, skip it
            if json_data_old and value == json_data_old[key]:
                continue

            state_topic = "pytes_serial/" + dev_name + "/" + key
            if isinstance(value, dict) or isinstance(value, list):
                message = json.dumps(value)
            else:
                message = json.dumps({'value': value})
            client.publish(state_topic, message)

        # Publish device topics
        for device in json_data["devices"]:
            device_idx = str(device["power"] - 1)

            for key, value in device.items():
                # Do not publish these
                if key in ["power"]:
                    continue

                # If the value was published before, skip it
                if (
                    json_data_old and
                    len(json_data["devices"]) == powers and
                    len(json_data_old["devices"]) == powers and
                    value == json_data_old["devices"][device["power"] - 1][key]
                ):
                    continue

                state_topic = "pytes_serial/" + dev_name + "/" + device_idx + "/" + key
                if isinstance(value, dict) or isinstance(value, list):
                    message = json.dumps(value)
                else:
                    message = json.dumps({'value': value})
                client.publish(state_topic, message)

        if cells_monitoring == 'true':
            for device in json_data["cells_data"]:
                device_idx = str(device["power"] - 1)

                # Publish cell statistics
                #low
                for key, value in device.items():
                    # Do not publish these
                    if key in ["power", "cells"]:
                        continue

                    # If the value was published before, skip it
                    if (
                        json_data_old and
                        len(json_data["cells_data"]) == powers and
                        len(json_data_old["cells_data"]) == powers and
                        value == json_data_old["cells_data"][device["power"] - 1][key]
                    ):
                        continue

                    state_topic = "pytes_serial/" + dev_name + "/" + device_idx + "/cells/" + key
                    if isinstance(value, dict) or isinstance(value, list):
                        message = json.dumps(value)
                    else:
                        message = json.dumps({'value': value})
                    client.publish(state_topic, message)

                # Publish cell topics
                for cell in device["cells"]:
                    cell_idx = str(cell["cell"] - 1)

                    for key, value in cell.items():
                        # Do not publish these
                        if key in ["power", "cell"]:
                            continue

                        # If the value was published before, skip it
                        if(
                            json_data_old and
                            len(json_data["cells_data"]) == powers and
                            len(json_data_old["cells_data"]) == powers and
                            len(json_data["cells_data"][device["power"] - 1]["cells"]) == cells and
                            len(json_data_old["cells_data"][device["power"] - 1]["cells"]) == cells and
                            value == json_data_old["cells_data"][device["power"] - 1]["cells"][cell["cell"] - 1][key]
                        ):
                            continue

                        state_topic = "pytes_serial/" + dev_name + "/" + device_idx + "/cells/" + cell_idx + "/" + key
                        if isinstance(value, dict) or isinstance(value, list):
                            message = json.dumps(value)
                        else:
                            message = json.dumps({'value': value})
                        client.publish(state_topic, message)

        pytes_serial_log.debug ('...mqtt publish  : ok')

    except Exception as e:
        pytes_serial_log.warning ('MQTT PUBLISH - error handling message: ' + str(e))

def check_events ():
    try:
        global pwr
        global bat_events_no
        global pwr_events_no
        global sys_events_no

        for power in range (1, powers+1):
            cell_data_req = "false"

            if power_events_list[pwr[power-1]['bat_events']][0] == events_mon_level or events_mon_level =="info":
                pytes_serial_log.warning (f'...bat_event logged  : {str(power_events_list[pwr[power-1]['bat_events']][1])}, {str(power_events_list[pwr[power-1]['bat_events']][2])}')

                cell_data_req = "true"
                bat_events_no = bat_events_no + 1

            if power_events_list[pwr[power-1]['power_events']][0] == events_mon_level or events_mon_level =="info":
                pytes_serial_log.warning (f'...power_event logged: {str(power_events_list[pwr[power-1]['power_events']][1])}, {str(power_events_list[pwr[power-1]['power_events']][2])}')

                cell_data_req = "true"
                pwr_events_no = pwr_events_no + 1

            if sys_events_list[pwr[power-1]['sys_events']][0] == events_mon_level or events_mon_level =="info":
                pytes_serial_log.warning (f'...sys_event logged  : {str(sys_events_list[pwr[power-1]['sys_events']][1])}, {str(sys_events_list[pwr[power-1]['sys_events']][2])}')

                cell_data_req = "true"
                sys_events_no = sys_events_no + 1

            if cell_data_req == "true" and cells_details =='true':
                if parsing_bat(power)=="true":
                    pytes_serial_log.warning (f"--------------------- BAT ({power}) ----------------------")
                    for cell in bat:
                        pytes_serial_log.warning (cell)
                    pytes_serial_log.warning ("-----------------------------------------------------")

                    pass

                else:
                    battery_events_log.info ('CHECK EVENTS - power_'+ str(power)+' cells details:cells data could not be read')

    except Exception as e:
        error_trace = traceback.format_exc()
        pytes_serial_log.warning (f'CHECK EVENTS - error handling message: {str(e)}\nTraceback:\n{error_trace}')

def parsing_stat():
    try:
        global errors
        global parsing_stat_lastexec
        global parsing_stat_interval

        if (parsing_stat_lastexec == 0) or (time.time() - parsing_stat_lastexec > parsing_stat_interval):

            for power in range (1, powers + 1):
                pytes_serial_log.info (f'...parsing_stat: power: {power}')

                req  = ('stat '+ str(power))
                result = serial_sendcommand(req, 'Device address', 'Command completed')
                if result.success == False:
                    pytes_serial_log.warning ('PARSING STAT - error')
                    return "false"
                
                for line_str in result.response:
                    value = int(line_str[18:27])
                    if 'Command completed' in line_str or 'Device address' in line_str:
                        pytes_serial_log.debug ('Command completed or Device address in line_str -- skipping line')
                        continue # Skip the last line and the first line

                    if 'Data Items      :' in line_str:
                        pwr[power - 1]['data_items'] = value
                        pytes_serial_log.debug (f'...data_items: {value} injected into pwr[{power - 1}][data_items]')
                    if 'HisData Items   :' in line_str:
                        pwr[power - 1]['histdata_items'] = value
                        pytes_serial_log.debug (f'...histdata_items: {value} injected into pwr[{power - 1}][histdata_items]')
                    if 'MiscData Items  :' in line_str:
                        pwr[power - 1]['miscdata_item'] = value
                        pytes_serial_log.debug (f'...miscdata_item: {value} injected into pwr[{power - 1}][miscdata_item]')

                    if 'CYCLE Times     :' in line_str:
                        pwr[power - 1]['cycle_times'] = value
                        pytes_serial_log.debug (f'...cycle_times: {value} injected into pwr[{power - 1}][cycle_times]')

                        parsing_stat_lastexec = time.time()
                        break # No need to continue parsing

                line_str       = ""
                pytes_serial_log.info (f"...parsing_stat: power {power} - ok")

            pytes_serial_log.info ("...parsing_stat: ok")
            return "true"

        else:
            pytes_serial_log.info (f"...parsing_stat: skipped. Last exectution was { seconds_to_duration(int(time.time() - parsing_stat_lastexec)) } ago. Next execution in { seconds_to_duration(int(parsing_stat_interval - (time.time() - parsing_stat_lastexec))) }.")
            return "true"

    except Exception as e:
        errors = 'true'
        pytes_serial_log.error ('PARSING STAT - error handling message: ' + str(e))

def parsing_bat(power):
    try:
        global errors
        global bat
        bat = []

        pytes_serial_log.info (f'...parsing_bat: power: {power}')

        req  = ('bat '+ str(power))
        result = serial_sendcommand(req, 'Battery', 'Command completed')
        if result.success == False:
            pytes_serial_log.warning ('PARSING BAT - error')
            return "false"
        
        cell_idx        = -1
        volt_idx        = -1
        curr_idx        = -1
        temp_idx        = -1
        base_st_idx     = -1
        volt_st_idx     = -1
        curr_st_idx     = -1
        temp_st_idx     = -1
        soc_idx         = -1
        coulomb_idx     = -1
        is_pylontech    = False

        for i, line_str in enumerate(result.response):
            # Last line is command completed message
            if i == len(result.response) - 1:
                break

            # First line is table header
            elif i == 0:
                line = re.split(r'\s{2,}', line_str.strip()) # type: list[str] # Each column is delimited by at least 2 spaces
                for j, l in enumerate(line):
                    if l == 'Battery':
                        cell_idx = j
                    elif l == 'Volt':
                        volt_idx = j
                    elif l == 'Curr':
                        curr_idx = j
                    elif l == 'Tempr':
                        temp_idx = j
                    elif l == 'Base State':
                        base_st_idx = j
                    elif l == 'Volt. State':
                        volt_st_idx = j
                    elif l == 'Curr. State':
                        curr_st_idx = j
                    elif l == 'Temp. State':
                        temp_st_idx = j
                    elif l == 'SOC':
                        soc_idx = j
                    elif l == 'Coulomb':
                        coulomb_idx = j

                # Workaround for Pytes firmware missing SOC column in the header
                if soc_idx == -1 and coulomb_idx != -1:
                    soc_idx = coulomb_idx
                    coulomb_idx = coulomb_idx + 1

            # All the other lines are cell data
            # Parameters are selected based on monitoring level
            else:
                line = re.split(r'\s{2,}', line_str.strip()) # Each column is delimited by at least 2 spaces
                cell_data = {} # type: dict[str, int|float|str]

                cell_data['power']              = power

                if cell_idx != -1:
                    cell_data['cell']           = int(line[cell_idx]) + 1
                if volt_idx != -1:
                    cell_data['voltage']        = int(line[volt_idx]) / 1000            # V
                if cells_mon_level=='high' and curr_idx != -1:
                    cell_data['current']        = int(line[curr_idx]) / 1000            # A
                if (cells_mon_level=='medium' or cells_mon_level=='high') and temp_idx != -1:
                    cell_data['temperature']    = int(line[temp_idx]) / 1000            # deg C
                if cells_mon_level=='high' and base_st_idx != -1:
                    cell_data['basic_st']       = line[base_st_idx]
                if (cells_mon_level=='medium' or cells_mon_level=='high') and volt_st_idx != -1:
                    cell_data['volt_st']        = line[volt_st_idx]
                if cells_mon_level=='high' and curr_st_idx != -1:
                    cell_data['curr_st']        = line[curr_st_idx]
                if cells_mon_level=='high' and temp_st_idx != -1:
                    cell_data['temp_st']        = line[temp_st_idx]
                if cells_mon_level=='high' and soc_idx != -1:
                    cell_data['soc']            = int(line[soc_idx][:-1])               # %
                if cells_mon_level=='high' and coulomb_idx != -1:
                    cell_data['coulomb']        = int(line[coulomb_idx][:-4]) / 1000    # Ah

                bat.append(cell_data)
                pytes_serial_log.debug (cell_data)

        pytes_serial_log.debug ('---------------------------')
        pytes_serial_log.info(f'...parsing_bat: power {power} - ok')
        return "true"

    except Exception as e:
        errors = 'true'
        pytes_serial_log.info ('PARSING BAT - error handling message: ' + str(e))

def check_cells():
    try:
        global bats
        
        for power in range (1, powers+1):
            if parsing_bat(power)=="true":
                
                # statistics availailable only for medium and high monitoring level
                if cells_mon_level=='medium' or cells_mon_level=='high':
                   # statistics -- calculate min,mix of cells data of each power
                    output = {"voltage" : [float('inf'),float('-inf')],
                              "temperature" : [float('inf'),float('-inf')]
                              }

                    for item in bat:
                        for each in output.keys():
                            if item[each]<output[each][0]:
                                output[each][0] = item[each]

                            if item[each]>output[each][1]:
                                output[each][1] = item[each]

                    stat = {
                        'power':power,
                        'voltage_delta':round(output['voltage'][1] - output['voltage'][0],3),
                        'voltage_min':output['voltage'][0],
                        'voltage_max':output['voltage'][1],
                        'temperature_delta': round(output['temperature'][1] - output['temperature'][0],3),
                        'temperature_min':output['temperature'][0],
                        'temperature_max':output['temperature'][1],
                        'cells':bat
                    }
                    
                else:
                    # statistics not available for 'low' level monitoring 
                    stat = {
                        'power':power,
                        'cells':bat
                    }

                bats.append(stat)

            else:
                pass

    except Exception as e:
        pytes_serial_log.info ('CHECK CELLS - error handling message: ' + str(e))

def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        client.publish(availability_topic, "online", qos=1, retain=True)
        pytes_serial_log.info("Connected to MQTT broker successfully")
    else:
        pytes_serial_log.warning(f"Connection failed with code {rc}")

def on_disconnect(client, userdata, rc, properties=None):
    if rc != 0:
        pytes_serial_log.warning(f"Unexpected disconnection (rc={rc}). Reconnecting...")

def main_loop():
    global errors
    global trials
    global pwr
    global bats
    global start_time
    global up_time
    global loops_no
    global errors_no
    global bat_events_no
    global pwr_events_no
    global sys_events_no
    global parsing_time
    global TimeStamp    
    global uptime

    try:
        while True:
            time.sleep(0.2)
            if (time.time() - start_time) > reading_freq:

                loops_no       = loops_no +1

                now            = datetime.datetime.now(datetime.UTC)
                TimeStamp      = now.strftime("%Y-%m-%d %H:%M:%SZ")
                pytes_serial_log.debug (f'relay local time: {TimeStamp}')

                uptime = round((time.time()- up_time)/86400, 3)
                pytes_serial_log.debug (f'serial uptime   : {uptime}')
                start_time = time.time()
                    
                if errors == 'false':
                    parsing_time = time.time()
                    parsing_serial()
                    parsing_time = time.time() - parsing_time
                    #pytes_serial_log.debug (round(parsing_time, 2)) #debug

                if errors == 'false':
                    parsing_pwrsys_time = time.time()
                    parsing_pwrsys()
                    parsing_pwrsys_time = time.time() - parsing_pwrsys_time
                    parsing_time     = parsing_time + parsing_pwrsys_time
                    #pytes_serial_log.debug (round(parsing_time, 2)) #debug

                if errors == 'false':
                    parsing_stat_time = time.time()
                    parsing_stat()
                    parsing_stat_time = time.time() - parsing_stat_time
                    parsing_time     = parsing_time + parsing_stat_time
                    pytes_serial_log.debug ('parsing_stat_time: ' + str(parsing_stat_time))

                if cells_monitoring == 'true' and errors == 'false':
                    check_cells_time = time.time()
                    check_cells()
                    check_cells_time = (time.time() - check_cells_time)
                    parsing_time     = parsing_time + check_cells_time
                    #pytes_serial_log.debug (round(check_cells_time, 2)) #debug
                    
                if events_monitoring=='true' and errors == 'false':
                    check_events()

                if errors == 'false':
                    json_serialize()

                if errors == 'false' and SQL_active == 'true':
                    maria_db()

                if errors == 'false' and MQTT_active == 'true':
                    mqtt_publish_time = time.time()
                    mqtt_publish()
                    mqtt_publish_time = (time.time() - mqtt_publish_time)
                    #pytes_serial_log.debug (round(mqtt_publish_time, 2)) #debug
                    
                if errors != 'false' :
                    errors_no = errors_no + 1

                pytes_serial_log.info (f'...serial stat   : loops: {loops_no}, errors: {errors_no}, efficiency: {round((1-(errors_no/loops_no))*100, 2)}')
                pytes_serial_log.info (f'...serial stat   : bat events_no: {bat_events_no}, pwr events_no: {pwr_events_no}, sys events_no: {sys_events_no}')
                pytes_serial_log.info (f'...serial stat   : parsing round-trip: {round(parsing_time, 2)}')
                pytes_serial_log.info ('------------------------------------------------------')


                #clear variables
                pwr        = []
                bats       = []
                errors     = 'false'
                trials     = 0

    except Exception as e:
        pytes_serial_log.error ('MAIN LOOP - error handling message: ' + str(e))


# Create an MQTT client instance
client = mqtt.Client(
    client_id="pytes_mqtt_publisher",
    callback_api_version=CallbackAPIVersion.VERSION2
)

availability_topic = "pytes_serial/" + dev_name + "/availability"
client.will_set(availability_topic, "offline", qos=1, retain=True)
client.on_connect = on_connect
client.on_disconnect = on_disconnect

# Enable automatic reconnection
client.reconnect_delay_set(min_delay=1, max_delay=120)  # Optional: Set reconnection delays (in seconds)

client.username_pw_set(MQTT_username, MQTT_password)
client.connect(MQTT_broker, MQTT_port, keepalive=60)
# Start the loop to process network events
client.loop_start()  # Runs a thread in the background to handle network events

# --------------------------serial initialization-------------------
try:
    ser = serial.Serial (port=serial_port,\
          baudrate=serial_baudrate,\
          parity=serial.PARITY_NONE,\
          stopbits=serial.STOPBITS_ONE,\
          bytesize=serial.EIGHTBITS,\
          timeout=10)

    if ser.portstr: pytes_serial_log.debug ('...connected to: ' + ser.portstr)

except Exception as e:
    pytes_serial_log.error ('OPEN SERIAL - error handling message: ' + str(e))
    pytes_serial_log.error ('...program initialisation failed -- exit')

    exit()

# --------------------------mqtt auto discovery (HA)----------------
if MQTT_active =='true':  mqtt_discovery()

#-----------------------------main loop-----------------------------
pytes_serial_log.debug ('...program initialisation completed starting main loop')

pytes_serial_log.info ('START - ' + version)
battery_events_log.info ('START - ' + version)
json_data = {}

main_loop()

