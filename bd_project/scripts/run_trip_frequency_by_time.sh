

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r pickup_time_frequency.out 
hadoop fs -rm -r "pickup_time_frequency_max.out" 
hadoop fs -rm -r "pickup_time_frequency_min.out"

# Run
spark-submit  --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) --driver-memory 3g --executor-memory 3g pickupTime_frequency.py > log

hadoop fs -getmerge pickup_time_frequency.out pickup_time_frequency.out
hadoop fs -getmerge "pickup_time_frequency_max.out" "pickup_time_frequency_max.out"
hadoop fs -getmerge "pickup_time_frequency_min.out" "pickup_time_frequency_min.out"
