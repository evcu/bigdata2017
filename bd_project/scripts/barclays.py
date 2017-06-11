from pyspark import SparkContext
sc=SparkContext()

import myUtils as my
import spatialUtils as sp
import pickle
from datetime import date
import datetime
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")

data,flag = my.readAllFiles(sc)
b_dates = [date(e[0],e[2],e[1]) for e in sp.getBarclaysEventDates()]
b_xy =[-73.975357,40.682625]


#### LON_LIM = 0.002
#### LAT_LIM = 0.0012 
######################################################
LON_LIM = 0.002
LAT_LIM = 0.0012 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
def onDates(a,datetime_col='tpep_pickup_datetime'):
    i=my._fieldsDic[datetime_col]
    dt = datetime.datetime.strptime(a[i], '%Y-%m-%d %H:%M:%S').date()
    return any([dt==d for d in b_dates])

date_filtered =  filtered.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))


#### LON_LIM = 0.004
#### LAT_LIM = 0.0024
######################################################
LON_LIM = 0.004
LAT_LIM = 0.0024 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
date_filtered =  filtered.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))

#### LON_LIM = 0.008
#### LAT_LIM = 0.005
######################################################
LON_LIM = 0.008
LAT_LIM = 0.005 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')    
date_filtered =  filtered.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))


#### LON_LIM = 0.016
#### LAT_LIM = 0.01
######################################################
LON_LIM = 0.016
LAT_LIM = 0.01 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')    
date_filtered =  filtered.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))

#### LON_LIM = 0.016
#### LAT_LIM = 0.01
######################################################
LON_LIM = 0.08
LAT_LIM = 0.05 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')    
date_filtered =  filtered.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))


#### ALL-DATE-FILTERED
######################################################
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
date_filtered =  pickup_cleaned.filter(onDates)
agg_data=my.aggregateOnDateTime(date_filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('barclays_all.out','wb'))

#### ALL
######################################################
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
agg_data=my.aggregateOnDateTime(pickup_cleaned,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('all.out','wb'))

### ALL_Dates
#### LON_LIM = 0.002
#### LAT_LIM = 0.0012 
######################################################
LON_LIM = 0.002
LAT_LIM = 0.0012 
box_fun = sp.createBox(b_xy,LON_LIM,LAT_LIM)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=my.aggregateOnDateTime(filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days={'all_days':range(7)},
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('all_lon%f_lat%f.out' % (LON_LIM,LAT_LIM),'wb'))
