import pandas as pd
import datetime as dt
from datetime import datetime, timedelta
from pyspark.sql import types as T
from pyspark.sql.functions import *
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, date_format, monotonically_increasing_id, mean
from pyspark.sql.functions import isnan, when, count, col, udf, dayofmonth, dayofweek, month, year, weekofyear


def create_dim_port(input_df):
    """Clean ports data"""
    return input_df.select(["code", "location", "state"]) \
                   .withColumnRenamed("code", "port_code") \
                   .withColumnRenamed("location", "city") \
                   .withColumnRenamed("state", "state_code") \
                   .dropna() \
        .dropDuplicates(["port_code", "city", "state_code"])


def create_dim_country(input_df):
    """Clean countries data"""
    return input_df.select(["code", "country"]) \
                   .withColumnRenamed("code", "country_code") \
                   .withColumnRenamed("country", "country_name") \
                   .dropna() \
        .dropDuplicates(["country_code", "country_name"])


def create_dim_visa(input_df):
    """Clean visas data"""
    return input_df.withColumn("visa_id", monotonically_increasing_id()) \
                   .select(["visa_id", "i94visa", "visatype", "visapost"]) \
                   .dropna() \
        .dropDuplicates(["i94visa", "visatype", "visapost"])


def create_dim_airport(input_df):
    """Clean airports data"""
    return input_df.select(["ident", "type", "iata_code", "name", "iso_country", "iso_region", "municipality", "gps_code", "coordinates", "elevation_ft"])\
        .dropDuplicates(["ident"])


def create_dim_time(input_df):
    """Clean time data"""
    get_isoformat_date = udf(lambda x: (datetime(1960, 1, 1).date(
    ) + timedelta(days=int(x))).isoformat() if x else None)

    df = input_df.select(["arrdate"])\
        .withColumn("date", get_isoformat_date("arrdate"))\
        .withColumn("day", dayofmonth("date"))\
        .withColumn("month", month("date"))\
        .withColumn("year", year("date"))\
        .withColumn("week", weekofyear("date"))\
        .withColumn("weekday", dayofweek("date"))\
        .select(["arrdate", "date", "day", "month", "year", "week", "weekday"])\
        .dropDuplicates(["arrdate"])

    return df


def create_dim_state(input_df):
    """Clean states data"""

    df = input_df.select(["State Code", "State", "Median Age", "Male Population", "Female Population", "Total Population", "Average Household Size",
                          "Foreign-born", "Race", "Count"])\
        .withColumnRenamed("State Code", "state_code")\
        .withColumnRenamed("State", "state")\
        .withColumnRenamed("Median Age", "median_age")\
        .withColumnRenamed("Male Population", "male_population")\
        .withColumnRenamed("Female Population", "female_population")\
        .withColumnRenamed("Total Population", "total_population")\
        .withColumnRenamed("Average Household Size", "average_household_size")\
        .withColumnRenamed("Foreign-born", "foreign_born")\
        .withColumnRenamed("Race", "race")\
        .withColumnRenamed("Count", "count")\

    df = df.groupBy(col("state_code"), col("state")).agg(
        round(mean('median_age'), 2).alias("median_age"),
        sum("total_population").alias("total_population"),
        sum("male_population").alias("male_population"),
        sum("female_population").alias("female_population"),
        sum("foreign_born").alias("foreign_born"),
        round(mean("average_household_size"), 2).alias(
            "average_household_size")).dropna()

    return df


def create_staging_temperature(input_df):
    """Clean temperature data"""
    df = input_df.select(["Country", "AverageTemperature", "AverageTemperatureUncertainty"])\
        .withColumnRenamed("Country", "country")\
        .withColumnRenamed("AverageTemperature", "average_temperature")\
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty")\

    df = df.groupBy(col("country")).agg(
        round(mean('average_temperature'), 2).alias("average_temperature"),
        round(mean("average_temperature_uncertainty"), 2).alias(
            "average_temperature_uncertainty")
    ).dropna()\
        .withColumn("temperature_id", monotonically_increasing_id()) \
        .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])

    return df

def create_dim_temperature(input_df):
    """Clean temperature data"""
    df = input_df.select(["Country", "Country Code", "AverageTemperature", "AverageTemperatureUncertainty"])\
        .withColumnRenamed("Country", "country")\
        .withColumnRenamed("AverageTemperature", "average_temperature")\
        .withColumnRenamed("AverageTemperatureUncertainty", "average_temperature_uncertainty")\

    df = df.groupBy(col("country")).agg(
        round(mean('average_temperature'), 2).alias("average_temperature"),
        round(mean("average_temperature_uncertainty"), 2).alias(
            "average_temperature_uncertainty")
    ).dropna()\
        .withColumn("temperature_id", monotonically_increasing_id()) \
        .select(["temperature_id", "country", "average_temperature", "average_temperature_uncertainty"])

    return df

def create_fact_immigration(spark, immigration_df, countries_df, states_df, ports_df, visas_df, temperature_df, airports_df, time_df):
    """Clean immigration data"""
    immigration_df.createOrReplaceTempView('staging_immigration_data')
    states_df.createOrReplaceTempView('staging_states_data')
    visas_df.createOrReplaceTempView('staging_visas_data')
    ports_df.createOrReplaceTempView('staging_ports_data')
    temperature_df.createOrReplaceTempView('staging_temperature_data')
    countries_df.createOrReplaceTempView('staging_countries_data')
    airports_df.createOrReplaceTempView('staging_airports_data')
    time_df.createOrReplaceTempView('staging_time_data')

    return spark.sql("""
    SELECT 
        DISTINCT si.cicid AS cicid,
        si.i94yr AS i94yr,
        si.i94mon AS i94mon,
        si.i94res AS i94res,
        si.i94mode AS i94mode,
        si.i94port AS i94port,
        si.i94cit AS i94cit, 
        si.i94addr AS i94addr,
        si.i94bir AS i94bir,
        si.occup AS occupation,
        si.gender AS gender,
        si.biryear AS birth_year,
        si.dtaddto AS entry_date,
        si.airline AS airline,
        si.admnum AS admission_number,
        si.fltno AS flight_number,
        si.visatype AS visa_type,
        si.arrdate AS arrival_date,
        si.depdate AS departure_date,
        st.temperature_id AS temperature_id,
        sv.visa_id as visa_id,
        ss.state_code as state_code,
        sp.port_code as port_code,
        sc.country_code as country_code,
        sa.ident as ident
    FROM staging_immigration_data si 
    LEFT JOIN staging_states_data ss ON si.i94addr = ss.state_code 
    LEFT JOIN staging_visas_data sv ON 
    (si.i94visa = sv.i94visa 
    AND si.visatype = sv.visatype
    AND si.visapost = sv.visapost)
    LEFT JOIN staging_ports_data sp ON sp.port_code = si.i94port 
    LEFT JOIN staging_temperature_data st ON si.i94res = st.country_code 
    LEFT JOIN staging_countries_data sc ON sc.country_code = si.i94res 
    LEFT JOIN staging_airports_data sa ON sa.ident = si.i94port 
    LEFT JOIN staging_time_data std ON std.arrdate = si.arrdate  
    """)