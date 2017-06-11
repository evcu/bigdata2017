

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r "payment_type_trip_distance_relation.out" 
hadoop fs -rm -r "payment_type_1_2_trip_distance_relation.out"

# Run
spark-submit  --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) --driver-memory 3g --executor-memory 3g relation_between_trip_distance_payment_type.py > log

hadoop fs -getmerge "payment_type_trip_distance_relation.out" "payment_type_trip_distance_relation.out"
hadoop fs -getmerge  "payment_type_1_2_trip_distance_relation.out" "payment_type_1_2_trip_distance_relation.out"
