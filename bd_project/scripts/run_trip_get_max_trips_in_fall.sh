

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r "fall_gps_coordinates_frequency_summer.out"
hadoop fs -rm -r "fall_gps_coordinates_frequency_fall.out"
hadoop fs -rm -r "fall_gps_coordinates_frequency_aug_oct.out"

# Run
spark-submit  --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) --driver-memory 3g --executor-memory 3g get_max_trips_in_fall.py > log

hadoop fs -getmerge "fall_gps_coordinates_frequency_summer.out" "fall_gps_coordinates_frequency_summer.out"
hadoop fs -getmerge "fall_gps_coordinates_frequency_fall.out" "fall_gps_coordinates_frequency_fall.out"
hadoop fs -getmerge "fall_gps_coordinates_frequency_aug_oct.out" "fall_gps_coordinates_frequency_aug_oct.out"
