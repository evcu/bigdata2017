

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r "tip_amount_fare_amount.out"
# Run
spark-submit --driver-memory 3g --executor-memory 3g correlation_between_fare_amount_tip_amount.py > log

hadoop fs -getmerge "tip_amount_fare_amount.out" "tip_amount_fare_amount.out"
