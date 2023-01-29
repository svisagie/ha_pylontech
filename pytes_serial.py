#!/usr/bin/python
import serial
import time, datetime
import json
import mysql.connector as mariadb
from configparser import ConfigParser
import paho.mqtt.publish as publish

# ---------------------------variables initialization---------- 
config                = ConfigParser()
config.read('pytes_serial.cfg')

serial_port           = config.get('serial', 'serial_port') 
serial_baudrate       = int(config.get('serial', 'serial_baudrate'))
reading_freq          = int(config.get('serial', 'reading_freq'))
powers                = int(config.get('general', 'powers'))
output_path           = config.get('general', 'output_path') 

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

start_time            = time.time()
up_time               = time.time()
pwr                   = []                                  # used to serialise JSON data
loops_no              = 0                                   # used to count no of loops and to calculate % of errors
errors_no             = 0                                   # used to count no of errors and to calculate % 
errors = 'false'

print('PytesSerial build: v0.3.2_20230129')

# ------------------------functions area----------------------------
def log (str) :
    try:
        with open('event.log','a') as file:
            file.write(time.strftime("%d/%m/%Y %H:%M:%S "))
            file.write(str + "\r\n")
        file.close()
        return
    except Exception as e:
        print("Errorhandling: double error in EventLog", e)
        
def parsing_serial():
    try:
        global errors
        global trials
        global pwr                                                                                 
        volt_st      = None                                                                        #  initiate non critical variable to ensure various firmaware combatibility    
        current_st   = None   
        temp_st      = None     
        coul_st      = None
        soh_st       = None
        heater_st    = None
        bat_events   = None
        power_events = None
        sys_events   = None

        data_set = 0
        pwr      = []
        
        if ser.is_open != True:
           ser.open()
           print ('...serial opened')
           
        for power in range (1, powers+1):
            line_str       = ""                                                                    # clear line_str
            line           = ""                                                                    # clear line            
            power_bytes    = bytes(str(power), 'ascii')                                            # convert to bytes
            ser.write(b'pwr '+ power_bytes + b'\n')                                                # write on serial port 'pwr x' command            ser.flush()
            ser.flush()
            
            time.sleep(0.5)                                                                        # calm down a bit ...
            print ('...writing complete, in buffer:', ser.in_waiting )          
      
            decode = 'false'
            while True:
                line = ser.read()
                if line:
                    #print(line.decode("Ascii"))
                    #print(line)
                    if ser.in_waiting == 0:
                        break
                    if line != b'\n':
                        line_str = line_str + line.decode("Ascii")
                    else:
                        #print (line_str)
                        if line_str[1:6].startswith('Power'):
                            decode ='true'
                            
                        if decode =='true':
                            if line_str[1:18] == 'Voltage         :': voltage      = int(line_str[19:27])/1000
                            if line_str[1:18] == 'Current         :': current      = int(line_str[19:27])/1000
                            if line_str[1:18] == 'Temperature     :': temp         = int(line_str[19:27])/1000
                            if line_str[1:18] == 'Coulomb         :': soc          = int(line_str[19:27])
                            if line_str[1:18] == 'Basic Status    :': basic_st     = line_str[19:27]        
                            if line_str[1:18] == 'Volt Status     :': volt_st      = line_str[19:27]      
                            if line_str[1:18] == 'Current Status  :': current_st   = line_str[19:27]    
                            if line_str[1:18] == 'Tmpr. Status    :': temp_st      = line_str[19:27]     
                            if line_str[1:18] == 'Coul. Status    :': coul_st      = line_str[19:27]
                            if line_str[1:18] == 'Soh. Status     :': soh_st       = line_str[19:27]
                            if line_str[1:18] == 'Heater Status   :': heater_st    = line_str[19:27]
                            if line_str[1:18] == 'Bat Events      :': bat_events   = int(line_str[19:27],16)
                            if line_str[1:18] == 'Power Events    :': power_events = int(line_str[19:27],16)
                            if line_str[1:18] == 'System Fault    :': sys_events   = int(line_str[19:27],16)
                            
                            if line_str[1:18] == 'Command completed':
                                decode ='false' 
                                print ('power           :', power)
                                print ('voltage         :', voltage)    
                                print ('current         :', current)
                                print ('temperature     :', temp)
                                print ('soc [%]         :', soc)
                                print ('basic_st        :', basic_st)         
                                print ('volt_st         :', volt_st)      
                                print ('current_st      :', current_st)     
                                print ('temp_st         :', temp_st)     
                                print ('coul_st         :', coul_st)
                                print ('soh_st          :', soh_st)
                                print ('heater_st       :', heater_st)
                                print ('bat_events      :', bat_events)
                                print ('power_events    :', power_events)
                                print ('sys_fault       :', sys_events)              
                                print ('---------------------------')
                                
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

                                pwr.append(pwr_array)
                                
                                data_set = data_set +1

                        line_str = ""
        
        if data_set == powers:
            statistics()
            errors='false'
            trials=0
            print ('...serial parsing: ok')
        else:
            errors='true'
            if trials < 6:
                print ('...incomplete data set, trying again')
                trials = trials+1
                #log('*'+str(errors_no)+'*trials: '+str(trials)+'**>'+str(line))                   # [DPO] for debug purpose                 
                time.sleep(1)
                parsing_serial()
            else:
                if ser.is_open == True:
                    ser.close()
                    print ('...not solved, serial closed')
                    log('*'+str(errors_no)+'*'+'incomplete data set*>'+str(line))                 # [DPO] for debug purpose                    
                    return
                    
    except Exception as e:
        print("...serial parsing error: " + str(e))
        errors = 'true'
        if ser.is_open == True:
            ser.close()
            print ('...serial closed')
            
        log('*'+str(errors_no)+'*'+'Except:'+str(e)+'**>'+str(line))                               # [DPO] for debug purpose
    
        return

def statistics():
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
        sys_current       = round((sys_current + pwr[power-1]['current']),3)  # current will be sum of all banks          
        sys_soc           = sys_soc + pwr[power-1]['soc']                     # soc will be the average of all batteries
        sys_temp          = sys_temp + pwr[power-1]['temperature']            # temperature will be the average of all batteries
   
    sys_voltage  = round((sys_voltage / powers), 3)    
    sys_soc      = int(sys_soc / powers)   
    sys_basic_st = pwr[0]['basic_st']                                         # status will be the master status
    sys_temp     = round((sys_temp / powers), 1)

def json_serialize():
    global errors
    global json_data
    try:
        json_data={'relay_local_time':TimeStamp,
                   'serial_uptime':uptime,
                   'powers' : powers,
                   'voltage': sys_voltage,
                   'current': sys_current,
                   'temperature': sys_temp,
                   'soc': sys_soc,
                   'basic_st': sys_basic_st,
                   'pytes':pwr}
        
        with open(output_path + 'pytes_status.json', 'w') as outfile:
            json.dump(json_data, outfile)
        print('...json creation:  ok')
        
    except Exception as e:
        print('...json serailization error: ' + str(e))
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
        print ('...mariadb upload: ok')
        
    except Exception as e:
        print('...mariadb writing error: '+ str(e))

def mqtt_discovery():
    try:
        MQTT_auth = None
        if len(MQTT_username) >0:
            MQTT_auth = { 'username': MQTT_username, 'password': MQTT_password }
        msg          ={} 
        config       = 1
        names        =["pytes_current", "pytes_voltage" , "pytes_temperature", "pytes_soc", "pytes_status"]
        ids          =["current", "voltage" , "temperature", "soc", "basic_st"] #do not change the prefix "pytes_"
        dev_cla      =["current", "voltage", "temperature", "battery","None"]
        unit_of_meas =["A","v","°C", "%",""]
        
        # define system sensors 
        for n in range(5):
            state_topic          ="homeassistant/sensor/pytes/"+str(config)+"/config"
            msg ["name"]         = names[n]      
            msg ["stat_t"]       = "homeassistant/sensor/pytes/state"
            msg ["uniq_id"]      = "pytes_"+ids[n]
            if dev_cla[n] != "None":
                msg ["dev_cla"]  = dev_cla[n]
            msg ["unit_of_meas"] = unit_of_meas[n]
            msg ["val_tpl"]      = "{{ value_json." + ids[n]+ "}}"
            msg ["dev"]          = {"identifiers": ["pytes"],"manufacturer": "PYTES","model": "E-Box48100R","name": "pytes_ebox","sw_version": "1.0"}
            
            message              = json.dumps(msg)
            publish.single(state_topic, message, hostname=MQTT_broker, port= MQTT_port, auth=MQTT_auth, qos=0, retain=True)

            b = "...mqtt auto discovery initialization :" + str(round(config/(5*powers+5)*100)) +" %"
            print (b, end="\r")
            
            msg                  ={}
            config               = config +1
            time.sleep(2)
        
        # define individual batteries sensors
        for power in range (1, powers+1):
            for n in range(5):
                state_topic          ="homeassistant/sensor/pytes/"+str(config)+"/config"
                msg ["name"]         = names[n]+"_"+str(power)         
                msg ["stat_t"]       = "homeassistant/sensor/pytes/state"
                msg ["uniq_id"]      = "pytes_"+ids[n]+"_"+str(power)
                if dev_cla[n] != "None":
                    msg ["dev_cla"]  = dev_cla[n]
                msg ["unit_of_meas"] = unit_of_meas[n]
                msg ["val_tpl"]      = "{{ value_json.pytes[" + str(power-1) + "]." + ids[n]+ "}}"
                msg ["dev"]          = {"identifiers": ["pytes"],"manufacturer": "PYTES","model": "E-Box48100R","name": "pytes_ebox","sw_version": "1.0"}
                
                message              = json.dumps(msg)
                publish.single(state_topic, message, hostname=MQTT_broker, port= MQTT_port, auth=MQTT_auth, qos=0, retain=True)

                b = "...mqtt auto discovery initialization :" + str(round(config/(5*powers+5)*100)) +" %"
                print (b, end="\r")
                
                msg                  ={}
                config               = config +1
                time.sleep(2)
                
        print("...mqtt auto discovery initialization completed")
        
    except Exception as e:
        print('...mqtt_discovery failed' + str(e))     

def mqtt_publish():
    try:
        MQTT_auth = None
        if len(MQTT_username) >0:
            MQTT_auth = { 'username': MQTT_username, 'password': MQTT_password }
        state_topic = "homeassistant/sensor/pytes/state"
        message     = json.dumps(json_data)
        publish.single(state_topic, message, hostname=MQTT_broker, port=MQTT_port, auth=MQTT_auth)
        print ('...mqtt publish  : ok')
        
    except Exception as e:
        print ('...mqtt publish error' + str(e))

# --------------------------serial initialization------------------- 
try:
    ser = serial.Serial (port=serial_port,\
          baudrate=serial_baudrate,\
          parity=serial.PARITY_NONE,\
          stopbits=serial.STOPBITS_ONE,\
          bytesize=serial.EIGHTBITS,\
          timeout=0)
    print('...connected to: ' + ser.portstr)

except Exception as e:
    print('...serial connection error ' + str(e))
    
# --------------------------mqtt auto discovery (HA)----------------
if MQTT_active =='true':  mqtt_discovery()

#-----------------------------main loop-----------------------------
print('...program initialisation completed starting main loop')

while True:
    if (time.time() - start_time) > reading_freq:                       # try every x sec
        
        loops_no       = loops_no +1                                    # count no of loops for error calculation
        
        now            = datetime.datetime.now()
        TimeStamp      = now.strftime("%Y-%m-%d %H:%M:%S")       
        print ('relay local time:', TimeStamp)
        
        start_time            = time.time() 
        uptime = round((time.time()- up_time)/86400,3)
        print ('serial uptime   :', uptime)

        if errors == 'false':
            parsing_serial()
        if errors == 'false':
            json_serialize()
        if errors == 'false' and SQL_active == 'true':
            maria_db()
        if errors == 'false' and MQTT_active == 'true':
            mqtt_publish()
            
        if errors != 'false' :
            errors_no = errors_no + 1
        print ('...errors        :', errors_no, 'loops:' , loops_no, 'efficiency:', round((1-(errors_no/loops_no))*100,2))
        print ('------------------------------------------------------')
        
        #clear variables
        pwr = []
        errors = 'false'
        trials = 0        