

module load python/gnu/3.4.4
export PYSPARK_PYTHON=/share/apps/python/3.4.4/bin/python
export PYTHONHASHSEED=0
export SPARK_YARN_USER_ENV=PYTHONHASHSEED=0


hadoop fs -rm -r  "pickup_date_weather.out"
hadoop fs -rm -r  "pickup_date_windspeed.out"
hadoop fs -rm -r "pickup_date_precipitation.out"
hadoop fs -rm -r "pickup_date_snow_depth.out"
hadoop fs -rm -r "pickup_date_winter_months_temperature_frequency.out"
hadoop fs -rm -r "pickup_date_fall_months_temperature_frequency.out"
hadoop fs -rm -r "pickup_date_summer_months_temperature_frequency.out"

# Run
spark-submit pickupdate_correlate_with_temperature.py > log

hadoop fs -getmerge "pickup_date_weather.out" "pickup_date_weather.out"
hadoop fs -getmerge "pickup_date_windspeed.out" "pickup_date_windspeed.out"
hadoop fs -getmerge "pickup_date_precipitation.out" "pickup_date_precipitation.out"
hadoop fs -getmerge "pickup_date_snow_depth.out" "pickup_date_snow_depth.out"
hadoop fs -getmerge "pickup_date_winter_months_temperature_frequency.out" "pickup_date_winter_months_temperature_frequency.out"
hadoop fs -getmerge "pickup_date_fall_months_temperature_frequency.out" "pickup_date_fall_months_temperature_frequency.out"
hadoop fs -getmerge "pickup_date_summer_months_temperature_frequency.out" "pickup_date_summer_months_temperature_frequency.out"

