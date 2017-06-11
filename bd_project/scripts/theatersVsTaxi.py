from pyspark import SparkContext
sc=SparkContext()
import myUtils as my
import spatialUtils as sp
import pickle
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")


data,flag = my.readAllFiles(sc)
#data,flag =my.readFiles2(dict((y, [k for k in range(1,2)]) for y in range(2015,2016)),sc)
theaters = sp.loadNYCtheaters()
LON_LIM = 0.002
LAT_LIM = 0.0012 

#### APOLLO
######################################################
ven = theaters['Apollo Theater']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=my.aggregateOnDateTime(filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days= dict(((k,[k]) for k in range(7))),
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('Apollo_24h_7d.out','wb'))

#### Lincoln
######################################################
ven = theaters['The Walter Reade Theater at Lincoln Center']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=my.aggregateOnDateTime(filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days= dict(((k,[k]) for k in range(7))),
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('Lincoln_24h_7d.out','wb'))

#### 59E59
######################################################
ven = theaters['59E59']
box_fun = sp.createBox(ven,LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=my.aggregateOnDateTime(filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days= dict(((k,[k]) for k in range(7))),
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('59E59_24h_7d.out','wb'))


#filtered.saveAsTextFile('broadway.out')
The Walter Reade Theater at Lincoln Center
####ALLL
######################################################
data,flag = my.readAllFiles(sc)
#data,flag =my.readFiles2(dict((y, [k for k in range(1,2)]) for y in range(2015,2016)),sc)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
agg_data=my.aggregateOnDateTime(pickup_cleaned,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days= dict(((k,[k]) for k in range(7))),
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('all_24h_7d.out','wb'))

#### All_theaters
######################################################
box_fun = sp.createUnionOfBoxes(theaters.values(),LON_LIM,LAT_LIM)

pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
filtered = sp.filterTheBox(pickup_cleaned,box_fun,'pickup_longitude','pickup_latitude')
agg_data=my.aggregateOnDateTime(filtered,datetime_col='tpep_pickup_datetime',
								row_fun =lambda x:1,months={'all_months':range(1,13)},
								days= dict(((k,[k]) for k in range(7))),
								hours=dict(((k,[k]) for k in range(24)))).reduceByKey(lambda x,y: x+y)
res=agg_data.collect()
pickle.dump(res,open('allTheaters_24h_7d.out','wb'))

