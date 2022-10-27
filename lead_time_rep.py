import json
import pyodbc
import math
import os
import time
from datetime import date
import pandas as pd

def print_banner():
    print("$$\                                $$\       $$$$$$$$\ $$\                \n"          
"$$ |                               $$ |      \__$$  __|\__|                         \n"
"$$ |      $$$$$$\   $$$$$$\   $$$$$$$ |         $$ |   $$\ $$$$$$\$$$$\   $$$$$$\   \n"
"$$ |     $$  __$$\  \____$$\ $$  __$$ |         $$ |   $$ |$$  _$$  _$$\ $$  __$$\  \n"
"$$ |     $$$$$$$$ | $$$$$$$ |$$ /  $$ |         $$ |   $$ |$$ / $$ / $$ |$$$$$$$$ | \n"
"$$ |     $$   ____|$$  __$$ |$$ |  $$ |         $$ |   $$ |$$ | $$ | $$ |$$   ____| \n"
"$$$$$$$\ \$$$$$$$\ \$$$$$$$ |\$$$$$$$ |         $$ |   $$ |$$ | $$ | $$ |\$$$$$$$\  \n"
"\_______| \_______| \_______| \_______|         \__|   \__|\__| \__| \__| \_______| \n")

    print("$$$$$$$\                                            $$\               \n"          
"$$  __$$\                                           $$ |                        \n"
"$$ |  $$ | $$$$$$\   $$$$$$\   $$$$$$\   $$$$$$\  $$$$$$\    $$$$$$\   $$$$$$\  \n"
"$$$$$$$  |$$  __$$\ $$  __$$\ $$  __$$\ $$  __$$\ \_$$  _|  $$  __$$\ $$  __$$\ \n"
"$$  __$$< $$$$$$$$ |$$ /  $$ |$$ /  $$ |$$ |  \__|  $$ |    $$$$$$$$ |$$ |  \__|\n"
"$$ |  $$ |$$   ____|$$ |  $$ |$$ |  $$ |$$ |        $$ |$$\ $$   ____|$$ |      \n"
"$$ |  $$ |\$$$$$$$\ $$$$$$$  |\$$$$$$  |$$ |        \$$$$  |\$$$$$$$\ $$ |      \n"
"\__|  \__| \_______|$$  ____/  \______/ \__|         \____/  \_______|\__|      \n"
"                    $$ |\n"                                                       
"                    $$ |\n"                                                       
"                    \__|\n")

def print_loadscreen():
    print("______                              _  \n"
"| ___ \                            | |           \n"
"| |_/ /__ _ __  ___  __ _ _ __   __| | ___       \n"
"|  __/ _ \ '_ \/ __|/ _` | '_ \ / _` |/ _ \      \n"
"| | |  __/ | | \__ \ (_| | | | | (_| | (_) | _ _ \n"
"\_|  \___|_| |_|___/\__,_|_| |_|\__,_|\___(_|_|_)\n")

def print_donescreen():
    print("______ _             _      _                             _\n"       
"|  ___(_)           | |    | |                           | |      \n"
"| |_   _ _ __     __| | ___| |  _ __ ___ _ __   ___  _ __| |_ ___ \n"
"|  _| | | '_ \   / _` |/ _ \ | | '__/ _ \ '_ \ / _ \| '__| __/ _ \ \n"
"| |   | | | | | | (_| |  __/ | | | |  __/ |_) | (_) | |  | ||  __/\n"
"\_|   |_|_| |_|  \__,_|\___|_| |_|  \___| .__/ \___/|_|   \__\___|\n"
"                                        | |\n"                       
"                                        |_|\n")

os.system('cls')
print_banner()

json_file = 'config.json'

dsn = ""
uid = ""
pwd = ""
type = ""
query = ""
queries_to_choose = []

json_handle = open(json_file)
json_data = json.load(json_handle)

select_ind = 0
for dict in json_data:  
    if dict["type"] == "cstring":
        dsn = dict["DSN"]
        uid = dict["UID"]
        pwd = dict["PSW"]
        type = dict["type"]
    elif dict["type"] == "query":
        select_ind += 1
        queries_to_choose.append((select_ind,dict["tagid"]))

for query_tuple in queries_to_choose:
    print(f'{query_tuple[0]}. {query_tuple[1]}')

while True:
    selection = input('Seleccione el query: ')
    try:
        selection = int(selection)
        requested_tagid = queries_to_choose[int(selection)-1][1]
        os.system('cls')
        break
    except:
        print('Selección Invalida')
        continue

for dict in json_data:
    if dict["tagid"] == requested_tagid:
        query = dict["query"]

while True:
    print_banner()
    starting_date = input('Desde (mm/dd/yyyy): ')
    if 'STARTING-DATE' in query:
        query = query.replace('STARTING-DATE', starting_date)
    else:
        print('El query ya tiene una fecha de inicio especificada  ^_^')
    
    ending_date = input('Hasta (mm/dd/yyyy): ')
    if 'ENDING-DATE' in query:
        query = query.replace('ENDING-DATE', starting_date)
    else:
        print('El query ya tiene una fecha de inicio especificada ^_^')
    
    confirmation = input('¿Los datos ingresados son correctos? (y/n): ')

    if confirmation != 'n' or confirmation != 'N':
        os.system('cls')
        print_banner()
        break

conn_string = "DSN=" + dsn + ";UID=" + uid + ";PWD=" + pwd

try:
    cx = pyodbc.connect(conn_string)

except Exception as ex:
    print(f"Failed to connect:\n{ex}")


try:
    data = pd.read_sql(query, cx)
    os.system('cls')
    print_banner()
    print("Successful connection")
except:
    os.system('cls')
    print_banner()
    print('Ha ocurrido un error con el query.\nAlguna fecha está incorrecta o el query ha sido modificado.')

print_loadscreen()

df = pd.DataFrame(data)
indexes = df.index.values
cx.close()
#print(df)

#        Tray, Date, Time
info = [[None, None, 0]]

for index in indexes:
    #prev_date = ""
    #prev_time = 0
    line = []

    #Fetch values
    tray = df.loc[index, 'traynum']
    if index == 0:
        info[0][0] = tray
    date = df.loc[index, 'station_date']
    stn_time = df.loc[index, 'station_time']
    
    line.append(str(tray).strip())
    line.append(str(date).strip())
    line.append(str(stn_time).strip())
    info.append(line)

def convert_time2secs(hour):
    #Convert string to int
    hour = str(hour)
    hour = int(hour)
    #Build the hour in
    split_hour = [[],[],[]]
    prev_pair = 0.0
    for pair in split_hour:
        f_hour = hour / 100.0 # Get the hour in float whith the last two digits as decimal places
        hour = hour / 100 # Get the whole numbers
        remain = f_hour % 1 # Get only the decimal places
        remain = remain - prev_pair / 100.0 # Substract the previous time for accuracy
        prev_pair = remain
        remain = (remain * 100)
        remain = remain + 0.5
        remain = math.ceil(remain) - 1 #Round up or down
        pair.append(remain) 

    #Convert to raw seconds
    split_hour[0] = split_hour[0].pop()
    split_hour[1] = split_hour[1].pop() * 60
    split_hour[2] = split_hour[2].pop() * 3600

    split_hour = sum(split_hour)
    return(split_hour)

def convert_date(iso_date):
    if iso_date == None:
        return(None)

    iso_date = str(iso_date)
    #split_date = date.split("-")    
    #split_date[2] = split_date[2] * 86400 #86400 is the ammount of seconds in a day

    class_date = date.fromisoformat(iso_date)

    return(class_date)

prev_log = []
lead_time = []

for log in info:
    
    if log[1] == None:

        prev_log = log
        continue

    tray = log[0]
    prev_tray = prev_log[0]
    curr_date = convert_date(log[1])
    prev_date = convert_date(prev_log[1])
    present_time = convert_time2secs(log[2])
    prev_time = convert_time2secs(prev_log[2])
    
    if prev_date == None:
        #lead_time.append(None)
        prev_date = curr_date

    #time_delta = int(time) - int(prev_time)    
    time_delta = present_time - prev_time    
    date_delta = curr_date - prev_date
    date_delta_secs = date_delta.days * 86400

    if tray != prev_tray:
        log.append(0)
        lead_time.append(None)
    else:
        log.append(date_delta_secs)
        lead_time.append((time_delta + date_delta_secs)/60)


    prev_log = log

df['lead_time'] = lead_time
cols = list(df.columns.values)
tray_ind = cols.index('traynum')

for row in df.iterrows():
    line_number = row[0]
    tray = str(row[1][tray_ind])
    tray = tray.strip()
    tray = int(tray)
    #print(tray)
    if tray < 100000:
        df.drop(line_number, axis=0, inplace = True)

stations = df.filter(['s_code', 'sub_desc'])
stations = stations.drop_duplicates(keep = 'first')
stations = stations.set_index('s_code')
stations = stations.sort_index(ascending=True)

grouped_df = df.copy()
grouped_df = grouped_df.groupby(['s_code']).agg({'lead_time' : 'sum'})
grouped_df = grouped_df.sort_index(ascending=True)

os.system('cls')
print_banner()
rep_name = input('Ingrese el nombre del reporte: ')
rep_name = rep_name.replace(' ', '_')

df.to_csv(rep_name + '_raw.csv', index=False)
stations.to_csv(rep_name + '_stations.csv')
grouped_df.to_csv(rep_name + '_bystn.csv')

os.system('cls')
print_banner()
print_donescreen()
time.sleep(5.0)