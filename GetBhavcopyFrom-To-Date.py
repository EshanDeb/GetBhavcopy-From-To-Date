import pandas as pd
import numpy as np
import csv
import os.path
import requests, zipfile, os, io, pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
import webbrowser
import urllib.request
import sys
from zipfile import ZipFile
import time
import webbrowser

#taking the starting date from the user in dd-mm-yyyy format
ltdt=input("Enter the Starting Date(dd-mm-yyyy format)-  ")
today = datetime.today().date()
lastdt = datetime.strptime(ltdt,'%d-%m-%Y')

diff= today-lastdt.date()
print(today,lastdt,diff)

#running the contents of the for loop from the start date to the current date(today)
for i in range(0,diff.days+1):
    t = lastdt+ relativedelta(days=i)
    d, m, y = '%02d' % t.day, '%02d' % t.month, '%02d' % t.year
    dmonth={'01':'JAN','02':'FEB','03':'MAR','04':'APR','05':'MAY','06':'JUN','07':'JUL','08':'AUG','09':'SEP','10':'OCT','11':'NOV','12':'DEC'}

    #link to nseindia website-downloading the bhavcopy(daily)
    try:
      url='https://www1.nseindia.com/content/historical/DERIVATIVES/'+y+'/'+dmonth[m]+'/fo'+d+dmonth[m]+y+'bhav.csv.zip'
      r = requests.head(url)
      if r.status_code==404:
        print(str(t)+'-NO BHAVCOPY FOUND'+'\n')
        continue
      webbrowser.open(url)
      print(str(t)+"-SUCCESFULLY DOWNLOADED")
    except requests.ConnectionError:
        print(str(t)+'-NO BHAVCOPY FOUND'+'\n')
        continue

    #waiting for the zip file to download
    time.sleep(10)

    file_name='fo'+d+dmonth[m]+y+'bhav.csv.zip'

    #extracting the zip file
    with ZipFile(file_name, 'r') as zip: 
        zip.extractall() 
        print("ZIP SUCCESFULL")

    time.sleep(10)

    file_name='fo'+d+dmonth[m]+y+'bhav.csv'
    reader=pd.read_csv(file_name)

    future_index=reader[np.logical_or(reader.INSTRUMENT=='FUTIDX', reader.INSTRUMENT=='FUTSTK')]
    data=[]
    symbols_repeat=set()
    counter=-1
    for symbol in future_index.SYMBOL:
      counter+=1
      if((symbol in symbols_repeat)):
        data[-1]['COI']+=future_index['OPEN_INT'][counter]
      else:
        symbols_repeat.add(symbol)
        index=list(future_index.SYMBOL).index(symbol)
        data.append({'TIMESTAMP':future_index['TIMESTAMP'][index],
                     'INSTRUMENT':future_index['INSTRUMENT'][index],
                     'SYMBOL':future_index['SYMBOL'][index],
                     'OPEN':future_index['OPEN'][index],
                     'HIGH':future_index['HIGH'][index],
                     'LOW':future_index['LOW'][index],
                     'CLOSE':future_index['CLOSE'][index],
                     'COI':future_index['OPEN_INT'][index],
                     'PCR':0.00})

    print(len(symbols_repeat))

    output_file=pd.DataFrame(data)

    for symbol in output_file['SYMBOL']:
      index=list(output_file['SYMBOL']).index(symbol)
      if(output_file['INSTRUMENT'][index]=='FUTIDX'):
        target='OPTIDX'
      else:
        target='OPTSTK'
      sub_reader=reader[np.logical_and(reader.INSTRUMENT==target,reader.SYMBOL==symbol)]
      if(sub_reader.empty):
        continue
      else:
        CE_reader=sub_reader[reader['OPTION_TYP']=='CE']
        PE_reader=sub_reader[reader['OPTION_TYP']=='PE']
        CE_sum=1+sum(list(CE_reader['OPEN_INT']))
        PE_sum=sum(list(PE_reader['OPEN_INT']))
        output_file['PCR'][index]=PE_sum/CE_sum

    filename='OutputFile.csv'
    file_exists = (os.path.isfile(filename))

    with open('OutputFile.csv', 'a',  newline='') as f:
        output_file.to_csv(f, header=not file_exists)
