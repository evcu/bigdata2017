

csvfile = sc.textFile('data/yellow_tripdata_2016-01.csv,data/yellow_tripdata_2016-02.csv,data/yellow_tripdata_2016-03.csv,data/yellow_tripdata_2016-04.csv,data/yellow_tripdata_2016-05.csv,data/yellow_tripdata_2016-06.csv')

header = csvfile.first()

csvfile = csvfile.filter(lambda line : line != header)
 
taxi_data = csvfile.map(lambda line: line.split(','))
schema_taxi = spark.createDataFrame (taxi_data,('vendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'rateCodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount','extra','mta_tax','improvement_surcharge','tip_amount','tolls_amount','total_amount'))

schema_taxi.createOrReplaceTempView("yellow_taxi") 
 
spark.sql("select store_and_fwd_flag,count(*) from yellow_taxi group by Store_and_fwd_flag").show()
spark.sql("select count(*) from yellow_taxi where Store_and_fwd_flag='store_and_fwd_flag'").show()


spark.sql("select rateCodeID,count(*) from yellow_taxi group by rateCodeID").show() -- 99 value 216 count

spark.sql("select passenger_count,count(*) from yellow_taxi group by passenger_count").show() # value 0 how? why?

spark.sql("select payment_type,count(*) from yellow_taxi group by payment_type ").show() 

spark.sql("select vendorID,count(*) from yellow_taxi group by vendorID").show() --No issues

spark.sql("select TO_DATE(tpep_pickup_datetime),count(*) from yellow_taxi group by  TO_DATE(tpep_pickup_datetime) order by TO_DATE(tpep_pickup_datetime)").show(200)

spark.sql("select TO_DATE(tpep_dropoff_datetime),count(*) from yellow_taxi group by  TO_DATE(tpep_dropoff_datetime)").show(200) --the most interesting dropoff dates and pickup dates are different. Some of the differences is lasts for days

spark.sql("select TO_DATE(tpep_pickup_datetime),TO_DATE(tpep_dropoff_datetime),count(*) from yellow_taxi group by  TO_DATE(tpep_pickup_datetime),TO_DATE(tpep_dropoff_datetime)").show(300)

#Lots of Negative amounts and 0: Reason :refund?
#8k+ rows where the total amount is less than $1
spark.sql("select count(*) from yellow_taxi where total_amount<=1.0").show()
spark.sql("select distinct(total_amount) from yellow_taxi order by  total_amount ").show(300)

date_total_amount = spark.sql("select TO_DATE(tpep_pickup_datetime),sum(total_amount) from yellow_taxi where total_amount >= 0 group by TO_DATE(tpep_pickup_datetime)")

date_avg_amount = spark.sql("select TO_DATE(tpep_pickup_datetime),avg(total_amount) from yellow_taxi where total_amount >= 0 group by TO_DATE(tpep_pickup_datetime)")


#Fare amount
spark.sql("select count(*) from yellow_taxi where fare_amount<=1.0").show()

date_fare_total_amount = spark.sql("select TO_DATE(tpep_pickup_datetime),sum(fare_amount) from yellow_taxi where fare_amount >= 0 group by TO_DATE(tpep_pickup_datetime)")

date_fare_avg_amount = spark.sql("select TO_DATE(tpep_pickup_datetime),avg(fare_amount) from yellow_taxi where fare_amount >= 0 group by TO_DATE(tpep_pickup_datetime)")


#There are many rows where trip distance is less than 0
spark.sql("select count(*) from yellow_taxi where trip_distance <=0  ").show() 

date_total_distance = spark.sql("select TO_DATE(tpep_pickup_datetime),sum(trip_distance) from yellow_taxi where trip_distance >= 0 group by TO_DATE(tpep_pickup_datetime)")

date_avg_distance = spark.sql("select TO_DATE(tpep_pickup_datetime),avg(trip_distance) from yellow_taxi where trip_distance >= 0 group by TO_DATE(tpep_pickup_datetime)")


#Trip distance
spark.sql("select max(trip_distance) from yellow_taxi ").show() 
spark.sql("select min(trip_distance) from yellow_taxi ").show() 
spark.sql("select sum(trip_distance) from yellow_taxi where trip_distance >= 0 ").show()
spark.sql("select avg(trip_distance) from yellow_taxi where trip_distance >= 0 ").show()


spark.sql("select count(*) from yellow_taxi where mta_tax<=0.0").show()

#number of passengers per day 
spark.sql("select min(passenger_count) from yellow_taxi ").show() 

#Frequency of passengers (To find correlation we can see rainy days and passenger count
spark.sql("select passenger_count,count(*) from yellow_taxi group by passenger_count").show()

#Total number of passengers every day (Look for spikes)
spark.sql("select TO_DATE(tpep_pickup_datetime),sum(passenger_count) from yellow_taxi group by TO_DATE(tpep_pickup_datetime)").show()
 
#pick up and drop off locations 1 of the outliers will be that if the pickup and drop location is same
spark.sql("select count(*) from yellow_taxi where pickup_longitude=dropoff_longitude and pickup_latitude=dropoff_latitude and trip_distance > 0").show()


