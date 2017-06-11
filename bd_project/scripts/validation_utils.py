from __future__ import print_function

import sys
from operator import add
from pyspark import SparkContext
import datetime

def checkVendorIDValid(vendor_id):
    try:
        int(vendor_id)
        if int(vendor_id) == 1 or int(vendor_id) == 2:
            return "Valid"
        else:
            return "Invalid"
    except ValueError:
            return "Invalid"

def checkPickUpDateValid(date_text):
    if date_text is not None or date_text:
        try:
            given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            year = given_date.year
            if year >= 2013 and year <= 2016:
                return "Valid"
            else:
                    return "Invalid_year"
        except ValueError:
            return "Invalid_date"
    else:
        return "Invalid_Null_date"

def checkDropoffDateValid(date_text):
    if date_text is not None:
        try:
            given_date = datetime.datetime.strptime(date_text, '%Y-%m-%d %H:%M:%S')
            year = given_date.year
            if year >= 2013 and year <= 2017:
                return "Valid"
            else:
                return "Invalid_year"
        except ValueError:
            return "Invalid_date"
    else:
        return "Invalid_Null_date"

def checkPassengerCountValid(passenger_count):
    try:
        int(passenger_count)
        if int(passenger_count) >= 0 and int(passenger_count) < 10:
            return "Valid"
        else:
            return "Invalid_not_within_range"
    except ValueError:
        return "Invalid_not_integer"



def checkTripDistanceValid(trip_distance):
    try:
        num = float(trip_distance)
        if  num > 0:
            return "Valid"
        elif num == 0:
            return "Invalid_ZeroTripDistance"
        else:
            return "Invalid_NegativeTripDistance"
    except ValueError:
        return "Invalid_NotFloat"

def checkRateCodeIdValid(rateCodeId):
    try:
        int(rateCodeId)
        if int(rateCodeId) > 0 and int(rateCodeId) <= 6 :
            return "Valid"
        else:
            return "Invalid_NotWithinRange"
    except ValueError:
        return "Invalid_NotInteger"

def checkStoreAndFwdFlagValid(store_and_fwd_flag):
    if store_and_fwd_flag == 'Y' or store_and_fwd_flag == 'N':
        return "Valid"
    else:
        return "Invalid"

def checkPaymentTypeValid(payment_type):
    try:
        int(payment_type)
        if int(payment_type) > 0 and int(payment_type) <= 6 :
            return "Valid"
        else:
            return "Invalid_NotWithinRange_"+payment_type
    except ValueError:
        return "Invalid_NotInteger_"+payment_type


def checkMtaTaxValid(mta_tax):
    try:
        num = float(mta_tax)
        if  num == 0.5 :
            return "Valid"
        elif num == 0:
            return "Valid_ZeroTax"
        elif num < 0:
            return "Invalid_NegativeMtaTax"
        else:
            return "Invalid_not_0.5_or_0"
    except ValueError:
        return "Invalid_NotFloat"

def checkImprovementSurchargeValid(amount):
    if amount:
        try:
            num = float(amount)
            if  num ==  0.3:
                return "Valid"
            elif num == 0:
                return "Valid_Zero_amount"
            else:
                return "Invalid_Negative_amount"
        except ValueError:
            return "Invalid_NotFloat"
    else:
        return "Invalid_Null"

#Check amount valid can be used for tolls, tip, total amount, fare and extra amount
def checkAmountValid(amount):
    if amount:
        try:
            num = float(amount)
            if  num > 0:
                return "Valid"
            elif num == 0:
                return "Valid_Zero_amount"
            else:
                return "Invalid_Negative_amount"
        except ValueError:
            return "Invalid_NotFloat"
    else:
        return "Invalid_Null"

def checkLongitude(f):
    r = (-74.259090,-73.700272)   
    try:
        ff = float(f)
        if ff > 180 or ff < -180:
            return "Invalid"
        elif  r[1] >ff> r[0]:
            return "Valid"
        else:
            return "Invalid_NotNYC"
    except ValueError:
        return "Invalid_NotFloat"


def checkLatitude(f):
    r = (40.477399,40.917577)  
    try:
        ff = float(f)
        if ff > 90 or ff < -90:
            return "Invalid"
        elif  r[1] >ff> r[0]:
            return "Valid"
        else:
            return "Outlier_NotNYC"
    except ValueError:
        return "Invalid_NotFloat"

def checkPickupAndDropoffDate(pickupdate, dropoffdate):
    pickupdate_date = datetime.datetime.strptime(pickupdate, '%Y-%m-%d %H:%M:%S')
    dropoffdate_date = datetime.datetime.strptime(dropoffdate, '%Y-%m-%d %H:%M:%S')
    if dropoffdate_date > pickupdate_date:
        return "Valid"
    elif dropoffdate_date == pickupdate_date:
        return "Valid_butEqual"
    else:
        return "Invalid"

def checkTotalFareVsFareAmount(fare_amount, total_fare):
    ff_fare_amount = float(fare_amount)
    ff_total_amount = float(total_fare)
    if ff_fare_amount < ff_total_amount:
        return "Valid"
    elif ff_fare_amount == ff_total_amount:
        return "Valid_butEqual"
    else:
        return "Invalid"

def checkGPS_TripDistance(pickup_long, pickup_lat, dropoff_long, dropoff_lat, trip_distance):
    ff_pickup_long = float(pickup_long)
    ff_pickup_lat = float(pickup_lat)
    ff_dropoff_long = float(dropoff_long)
    ff_dropoff_lat = float(dropoff_lat)
    ff_trip_distance = float(trip_distance)
    if ff_trip_distance > 0 and ff_pickup_long == ff_dropoff_long and ff_pickup_lat == ff_dropoff_lat:
        return "Invalid"
    else:
        return "Valid"

def getAllValidationFunctions():
    #['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'RatecodeID', 'store_and_fwd_flag', 'dropoff_longitude', 'dropoff_latitude', 'payment_type', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount']
    d = {}
    d['pickup_longitude'] = checkLongitude
    d['pickup_latitude'] = checkLatitude
    d['dropoff_longitude'] = checkLongitude
    d['dropoff_latitude'] = checkLatitude
    d['VendorID'] = checkVendorIDValid
    d['tpep_pickup_datetime'] = checkPickUpDateValid
    d['tpep_dropoff_datetime'] = checkDropoffDateValid
    d['passenger_count'] = checkPassengerCountValid
    d['trip_distance'] = checkTripDistanceValid
    d['RatecodeID'] = checkRateCodeIdValid
    d['store_and_fwd_flag'] = checkStoreAndFwdFlagValid
    d['payment_type'] = checkPaymentTypeValid
    d['fare_amount'] = checkAmountValid
    d['extra'] =checkAmountValid
    d['mta_tax'] = checkMtaTaxValid
    d['tip_amount'] = checkAmountValid
    d['tolls_amount'] = checkAmountValid
    d['improvement_surcharge'] = checkImprovementSurchargeValid
    d['total_amount'] = checkAmountValid


    return d
   






