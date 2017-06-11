import myUtils as my
import spatialUtils as sp
import pickle
sc.addPyFile("myUtils.py")
sc.addPyFile("validation_utils.py")
sc.addPyFile("spatialUtils.py")


data,flag =my.readFiles2(dict((y, [k for k in [8]]) for y in range(2015,2016)),sc)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
coordinates = pickup_cleaned.map(lambda a: (my.readAtt(a,'pickup_longitude'), my.readAtt(a,'pickup_latitude')))
coordinates.saveAsTextFile("2015_8_coor.out")


data,flag =my.readFiles2(dict((y, [k for k in [1]]) for y in range(2015,2016)),sc)
pickup_cleaned = my.cleanByFields(data,['pickup_longitude','pickup_latitude'])
coordinates = pickup_cleaned.map(lambda a: (my.readAtt(a,'pickup_longitude'), my.readAtt(a,'pickup_latitude')))
coordinates.saveAsTextFile("2015_1_coor.out")
