# NYC Taxi Cabs

In this repository you will find a series of scripts designed to explore [NYC Taxi and Limousine data](http://www.nyc.gov/html/tlc/html/about/trip_record_data.shtml) using pyspark.

To get a count of Valid and Invlaid values per column of data spanning 3 years, you need to run the submit_job bash script. This will give an individual out file for each column.

Command to run bash script: `sh submit_job.sh`


Command to run outliers detection:

`spark-submit outlier_detection.py`

To get the files:

`hadoop fs -getmerge  total_amount_valid_including_outlier.out total_amount_valid_including_outlier.out`

`hadoop fs -getmerge  trip_distance_valid_including_outlier.out trip_distance_valid_including_outlier.out`



Command to run to get the number of trips a day:

`spark-submit pickup_date_frequency.py`

To get the output files:

`hadoop fs -getmerge  pickup_date_frequency.out pickup_date_frequency.out`


Command to run to get total amount per date:

`spark-submit total_amount_per_date_frequency.py`

`hadoop fs -getmerge  total_amount_per_date_frequency.out total_amount_per_date_frequency.out`

Command to run to get total fare amount per date:

`spark-submit fare_amount_per_date_frequency.py`

`hadoop fs -getmerge  fare_amount_valid_report.out fare_amount_valid_report.out`


Command to get payment type frequency:

`spark-submit payment_type_frequency.py`

`hadoop fs -getmerge  payment_type_frequency.out payment_type_frequency.out`

[This notebook](https://github.com/anudeepti2004/big_data_project/blob/master/shape%20conversion.ipynb) has the methods we used to create `zones.pickle` and `zones_mean.pickle` data. As explained in the report we decided to assign a fixed coordinates to every zone-id used for the data starting from July 2016. `zones_mean.pickle` has those mean coordinates. `zones.pickle` has the `matplotlib.Path` polygons for checking a point is whether inside the polygon or not. One can use the `myUtils.coordinateMapper` class to map the coordinate tuples to zone-id's.
