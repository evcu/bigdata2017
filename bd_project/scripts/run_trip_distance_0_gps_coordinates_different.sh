

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0



# Run
spark-submit  --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) --driver-memory 3g --executor-memory 3g trip_distance_0_gps_coordinates_different.py > log

