#!/bin/bash 

# Make sure we have access to the correct lbibraries
module load pandas/0.18.1 

array=(VendorID tpep_dropoff_datetime passenger_count trip_distance pickup_longitude pickup_latitude store_and_fwd_flag dropoff_longitude dropoff_latitude payment_type fare_amount extra mta_tax tip_amount tolls_amount improvement_surcharge total_amount tpep_pickup_datetime RatecodeID)

# Run
spark-submit  validate.py

# Get output files
for i in "${array[@]}"
do
	hadoop fs -getmerge "$i"_valid.out "$i"_valid.out
done
hadoop fs -rm -r -f *.out
