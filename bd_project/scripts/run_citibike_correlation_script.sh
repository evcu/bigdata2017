

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r "taxi_data_vs_citbike_frequency.out" 
hadoop fs -rm -r "taxi_data_vs_citbike_frequency_passenger_count_1.out"

# Run
spark-submit pickup_date_passenger_count__citibike_correlation_frequency.py > log

hadoop fs -getmerge "taxi_data_vs_citbike_frequency.out" "taxi_data_vs_citbike_frequency.out"
hadoop fs -getmerge "taxi_data_vs_citbike_frequency_passenger_count_1.out" "taxi_data_vs_citbike_frequency_passenger_count_1.out"
