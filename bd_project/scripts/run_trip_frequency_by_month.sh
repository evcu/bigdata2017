



hadoop fs -rm -r "pickup_month_year_frequency.out" 

# Run
spark-submit  --conf spark.ui.port=$(shuf -i 6000-9999 -n 1) --driver-memory 3g --executor-memory 3g pickup_month_year_frequency.py > log

hadoop fs -getmerge "pickup_month_year_frequency.out" "pickup_month_year_frequency.out" 
