
import numpy as np
import pandas as pd
from datetime import datetime,timedelta
import time as tm
from collections import OrderedDict
from argparse import ArgumentParser

def get_time_before(current,sec):
    '''
    get the date and time before some seconds
    '''
    return (datetime.strptime(str(current), '%Y-%m-%d %H:%M:%S') - timedelta(seconds = sec)).strftime('%Y-%m-%d %H:%M:%S')


def time_substraction(t1,t2):
    '''
    Calculate the time in the format('%H:%M:%S') after substraction 
    @parameters:
    t1: Format('%Y-%m-%d %H:%M:%S', string)
    t2: Format('%Y-%m-%d %H:%M:%S', string)
    @return 
    time: time difference in seconds
    '''
    FMT = '%Y-%m-%d %H:%M:%S'
    tdelta = datetime.strptime(str(t1), FMT) - datetime.strptime(str(t2), FMT)
    return tdelta.days * 24 *3600 + tdelta.seconds

def process(log_file,inactivity_file,output_file):
    table = pd.DataFrame(columns = ['ip','date_time'])    # Pandas DataFrame     
    iplist = [] # list: store ips
    timelist = []# list: store times
    timeflow = False # boolean : flag for time changing
    line_count = 0
    total_lines = sum(1 for line in open(log_file)) - 1

    with open(inactivity_file) as file:
        inactivity_period = int(file.readline())
    f = open(output_file,'w')
    
    for chunk in pd.read_csv(log_file, chunksize=1):
        line_count += 1
        time = list(chunk['time'])[0]
        date = list(chunk['date'])[0]
        ip = list(chunk['ip'])[0]
        iplist.append(ip)
        if time not in timelist:
            timelist.append(time)
            timeflow = True
        else:
            timeflow = False
       
        date_time = date + ' ' + time
        table = table.append({'ip':ip,'date_time':date_time}, ignore_index=True)
    
        current = timelist[-1]
    
        if line_count == total_lines:
            for ipid in list(OrderedDict.fromkeys(iplist)):
                time_range = list(table[table['ip'] == ipid]['date_time'])

                if len(time_range) > 0:
                
                    count =  len(table[table['ip'] == ipid])
                    time_length = time_substraction(time_range[-1],time_range[0]) + 1
                    # print (ipid + ',' + time_range[0] + ','  + time_range[-1] + ',' + str(time_length) + ',' + str(count))
                    f.write(ipid + ',' + time_range[0] + ','  + time_range[-1] + ',' + str(time_length) + ',' + str(count) + '\n')
                            

        else:
            if timeflow:
                for ipid in iplist:
#         means currently at this time there is no request, some ip may finish requesting
                    time_range = list(table[table['ip'] == ipid]['date_time'])
        
                    if len(table[(table['date_time'] == current) & (table['ip'] == ipid)]) == 0:
                    
                        starttime = get_time_before(date_time,inactivity_period + 1)
                        if len(time_range) > 0 and time_substraction(starttime, time_range[-1]) >= 0:
                            current_table = table[(table['date_time'] == starttime) & (table['ip'] == ipid)]
                            count = len(current_table)
                            index = list(current_table.index)
                        
                            time_length = time_substraction(time_range[-1],time_range[0]) + 1

                            if count > 0:
                                table.drop(index = index, inplace = True)
                            
                                # print (ipid + ',' + starttime + ',' + starttime + ',' + str(time_length) + ',' + str(count))
                                f.write(ipid + ',' + starttime + ','  + starttime + ',' + str(time_length) + ',' + str(count) + '\n')
                                while count > 0 :
                                    iplist.remove(ipid)
                                    count -= 1
    f.close()





if __name__ == '__main__':
    # process('./input/log.csv','./input/inactivity_period.txt','./output/sessionization.txt')
    parser = ArgumentParser()
    parser.add_argument('log', help='Name of input log file in CSV format')
    parser.add_argument('inactivity_period_file',
        help='Name of file that defines length of inactivity period')
    parser.add_argument('output', help='Name of output log file in CSV format')

    args = parser.parse_args()
    process(args.log,args.inactivity_period_file, args.output)

