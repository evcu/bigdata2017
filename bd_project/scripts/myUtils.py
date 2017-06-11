from __future__ import print_function

import sys
from operator import add
from csv import reader
import pickle
import validation_utils as va

# d = getFieldDic() and then you can call d[4] and it returns 'trip_distance' OR the other way around d['trip_distance']=4.
_fields = ['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']
_fieldsDic = dict((_fields[i],i) for i in range(len(_fields)))
_fieldsDic.update(dict((i,_fields[i]) for i in range(len(_fields))))

def getFieldDic():
    return _fieldsDic

def readAtt(x,field_name):
    return x[_fieldsDic[field_name]]

def readFiles (files,sc):
    concatenatedFiles = ','.join(files)
    
    csvfile = sc.textFile(concatenatedFiles)
    header = csvfile.first()

    csvfile = csvfile.filter(lambda line : line != header)
    # taxi_data.map(lambda t: map(float,t[5:7]))
    
    taxi_data = csvfile.mapPartitions(lambda x: reader(x))
    if "yellow" in concatenatedFiles:
        return (taxi_data,"yellow")
    else:
        return (taxi_data,"green")
    
OUR_DATABASE_PATH = '/user/dv697/data/yellow_tripdata_'
def readAllFiles (sc):
    y_m_dic = dict((y, [k for k in range(1,13)]) for y in range(2013,2017))
    return readFiles2(y_m_dic,sc)

def readFiles2 (year_months_dic,sc):
    basePath = OUR_DATABASE_PATH
    new_type = []
    old_type = []
    for y,m_array in year_months_dic.items():
        for m in m_array:
            # No gps 
            if y == 2016 and m > 6:
                new_type.append(basePath + '%d-%02d.csv' %(y,m))
            else:
                old_type.append(basePath + '%d-%02d.csv' %(y,m))

    oldTypeFiles = ','.join(old_type)
    newTypeFiles = ','.join(new_type)
    zones_mean = pickle.load(open('zones_mean.pickle','rb'))
    def getCoord(i):
        if i<=263:
            return zones_mean[i-1]
        elif i==264 or i==265:
            return ['NULL','NULL']
        else:
            raise Exception('Error: %d' % i)

    if oldTypeFiles:
        csvfile = sc.textFile(oldTypeFiles)
        header = csvfile.first()
        csvfile = csvfile.filter(lambda line : line != header).filter(lambda line: 'endor' not in line)
        # taxi_data.map(lambda t: map(float,t[5:7]))
    
        taxi_data = csvfile.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) != 0)
        if newTypeFiles:
            csvfile2 = sc.textFile(newTypeFiles)
            header2 = csvfile2.first()
            csvfile2 = csvfile2.filter(lambda line : line != header2).filter(lambda line: 'endor' not in line)
            taxi_data2 = csvfile2.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) != 0).map(lambda a: a[:5] + getCoord(int(a[7])) + a[5:7] + getCoord(int(a[8])) + a[9:])
            taxi_data = taxi_data.union(taxi_data2)
    else:
        csvfile = sc.textFile(newTypeFiles)
        header = csvfile.first()
        csvfile = csvfile.filter(lambda line : line != header).filter(lambda line: 'endor' not in line)
        taxi_data = csvfile.mapPartitions(lambda x: reader(x)).filter(lambda x: len(x) != 0).map(lambda a: a[:5] + getCoord(int(a[7])) + a[5:7] + getCoord(int(a[8])) + a[9:])

        

    ## Convert VendorID
    def convertVendorInt(x):
        i = _fieldsDic['VendorID']
        if x[i] == 'CMT': x[i]=1
        elif x[i] == 'VTS': x[i]=2
        return x

    taxi_data.filter(lambda x: len(x)!=0).map(convertVendorInt) # There are 1 empty array for each file. So lets remove them.   
    ## Convert Payment type
    def convertPaymentTypeInt(x):
        i = _fieldsDic['payment_type']
        if x[i] == 'CRD': x[i]=1
        elif x[i] == 'CSH': x[i]=2
        elif x[i] == 'NOC': x[i]=3
        elif x[i] == 'DIS': x[i]=4
        elif x[i] == 'UNK': x[i]=5
        return x
        
    taxi_data = taxi_data.map(convertVendorInt).map(convertPaymentTypeInt).filter(lambda x: len(x)!=0) # There are 1 empty array for each file. So lets remove them.   
    
    def correctLengthOfFields(x):
        i = _fieldsDic['improvement_surcharge']
        if len(x) == 18: 
            temp = x[i]
            x[i] = 0
            x.append(temp)      
        return x
         
    taxi_data = taxi_data.map(correctLengthOfFields)    
    if "yellow" in oldTypeFiles + newTypeFiles:
        return (taxi_data,"yellow")
    else:
        return (taxi_data,"green")

def getAllFileNames():
    y_m_dic = dict((y, [k for k in range(1,13)]) for y in range(2013,2017))
    return getSomeFileNames(y_m_dic)

def getSomeFileNames(year_months_dic):
    basePath = OUR_DATABASE_PATH
    file_names = []
    for y,m_array in year_months_dic.items():
        for m in m_array:
            file_names.append(basePath + '%d-%02d.csv' %(y,m))
    return file_names

class coordinateMapper(object):
    def __init__(self,path='zones.pickle'):
        import pickle
        import matplotlib.path as mplPath
        import numpy as np
        self.all_poly=pickle.load(open(path,'rb'))
        print(self.all_poly[15])
        
    def convert(self,xy):
        #xy is a tuple(x,y)
        for i,poly in self.all_poly:
            if poly.contains_point(xy):
                print('yes')
                return i
        
def getConverterFunc():
    mapper = coordinateMapper()
    return lambda t: mapper.convert(t)

def checkValid(f,range):
    try:
        ff = float(f)
        if  range[1] >f > range[0]:
            return "Valid"
        else:
            return "Invalid_NotNYC"
    except ValueError:
        return "Invalid_NotFloat"

def cleanByFields(data,fields):
    #fields: array of strings ['pickup_longitude','pickup_latitude']
    t=data
    df=va.getAllValidationFunctions()
    for f in fields:
        i = _fieldsDic[f]
        funi = df[f]
        t = t.filter(lambda e: funi(e[i]).startswith('Valid'))
    return t

def aggregateOnDateTime(data,datetime_col='tpep_pickup_datetime',row_fun =lambda x:1,months={'all_months':range(1,13)},days={'all_days':range(7)},hours={'all_hours':range(24)}):
    reverse_dic={}
    i=_fieldsDic[datetime_col]
    for mk,mv in months.items():
        for m in mv:
            for dk,dv in days.items():
                for d in dv:
                    for hk,hv in hours.items():
                        for h in hv:
                            reverse_dic[(m,d,h)]=(mk,dk,hk)
    def aggFun(a):
        from datetime import datetime
        dt = datetime.strptime(a[i], '%Y-%m-%d %H:%M:%S')
        key = reverse_dic[dt.month,dt.weekday(),dt.hour]
        return(key,row_fun(a))
    return data.map(aggFun)


_weather_fields = ['stn', 'wban', 'date', 'temp', 'temp_count', 'DEWP', 'DEWP_count', 'SLP', 'SLP_count', 'STP', 'STP_count', 'VISIB', 'VISIB_count', 'windspeed', 'windspeed_count', 'maxspeed', 'gust', 'max_temp', 'min_temp','precipitation','snow_depth','FRSHTT']
_weather_fieldsDic = dict((_weather_fields[i],i) for i in range(len(_weather_fields)))
_weather_fieldsDic.update(dict((i,_weather_fields[i]) for i in range(len(_weather_fields))))


final_weather_fields = ['date','temp','windspeed','maxspeed','gust','max_temp','min_temp','precipitation','snow_depth','FRSHTT']
final_weather_fieldsDic = dict((final_weather_fields[i],i) for i in range(len(final_weather_fields)))
final_weather_fieldsDic.update(dict((i,final_weather_fields[i]) for i in range(len(final_weather_fields))))

def readWeatherData(sc):
    file_path = '/user/dv697/data/weather_data_2013-2016.csv'
    csvfile = sc.textFile(file_path)
    header = csvfile.first()
    csvfile = csvfile.filter(lambda line : line != header)
    weather_data = csvfile.mapPartitions(lambda x: reader(x)).map(lambda x: (x[_weather_fieldsDic['date']].strip(),x[_weather_fieldsDic['temp']].strip(),x[_weather_fieldsDic['windspeed']].strip(),x[_weather_fieldsDic['maxspeed']].strip(),x[_weather_fieldsDic['gust']].strip(),x[_weather_fieldsDic['max_temp']].strip(),x[_weather_fieldsDic['min_temp']].strip(),x[_weather_fieldsDic['precipitation']].strip(),x[_weather_fieldsDic['snow_depth']].strip(),x[_weather_fieldsDic['FRSHTT']].strip()))
    return weather_data
