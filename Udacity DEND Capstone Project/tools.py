import pandas as pd
import numpy as np
import csv


def eliminate_column_missing_data(df):
    """
      Clean the data within the dataframe.

      :param df: dataframe
      :return: the cleaned dataframe 
    """
    print("Dropping missing data...")
    drop_columns = []
    percentage = 0
    for column in df:
        values = df[column].unique()
        if(True in pd.isnull(values)):
            percentage_missing = 100 * pd.isnull(df[column]).sum() / df.shape[0]
            if percentage_missing >= 95:
                drop_columns.append(column)
    if len(drop_columns) > 0:
        print(drop_columns)
    # Drop columns missing more than 95%:
    df = df.drop(columns=drop_columns)
    # drop rows where all elements are missing
    df = df.dropna(how='all')

    print("Cleaning complete!")
    return df


def drop_duplicate_rows(df):
    """
        Drop duplicate data within the dataframe.

        :param df: dataframe
        :return: the cleaned dataframe
    """
    print("Dropping duplicate data...")
    count = df.shape[0]
    df = df.drop_duplicates()
    print("Drops {} columns".format(count - df.shape[0]))
    print("Cleaning complete!")
    return df


def write_to_parquet(df, output, table_name):
    """
        Writes the dataframe as parquet file.

        :param df: dataframe to write
        :param output: output path where to write
        :param table_name: name of the table
    """
    path = output + table_name
    print("Writing table {} to {}".format(table_name, path))
    df.write.mode("overwrite").parquet(path)
    print("Writing completed")


def prepare_dictionaries():
    """
        Writes the dictionaries as csv files
    """
    country_info = ['code', 'country']
    country_dict = []
    with open("countries.txt") as fp:
        lines = fp.readlines()
        for line in lines:
            (a, b) = line.split('=')
            dict = {}
            dict['code'] = a.replace("'", '').strip()
            dict['country'] = b.replace("'", '').strip()
            country_dict.append(dict)

    with open('countries.csv', 'w') as csvfile:
        writer = csv.DictWriter(
            csvfile, fieldnames=country_info,  delimiter=',')
        writer.writeheader()
        writer.writerows(country_dict)

    # ports:
    port_info = ['code', 'location', 'state']
    port_dict = []
    with open("ports.txt") as fp:
        lines = fp.readlines()
        for line in lines:
            (a, b) = line.split('=')
            dict = {}
            dict['code'] = a.replace("'", '').strip()
            port = b.replace("'", '').strip()
            port_list = port.split(',')
            dict['location'] = port_list[0].strip()
            if len(port_list) > 1:
                dict['state'] = port_list[1].strip()
            elif port_list[0] == 'MARIPOSA AZ':
                dict['location'] = 'MARIPOSA'
                dict['state'] = 'AZ'
            port_dict.append(dict)

    with open('ports.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=port_info,  delimiter=',')
        writer.writeheader()
        writer.writerows(port_dict)

    # states:
    state_info = ['code', 'state']
    state_dict = []
    with open("states.txt") as fp:
        lines = fp.readlines()
        for line in lines:
            (a, b) = line.split('=')
            dict = {}
            dict['code'] = a.replace("'", '').strip()
            dict['state'] = b.replace("'", '').strip()
            state_dict.append(dict)

    with open('states.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=state_info,  delimiter=',')
        writer.writeheader()
        writer.writerows(state_dict)

    # visas:
    visa_info = ['code', 'category']
    visa_dict = []
    with open("visa.txt") as fp:
        lines = fp.readlines()
        for line in lines:
            (a, b) = line.split('=')
            dict = {}
            dict['code'] = a.replace("'", '').strip()
            dict['category'] = b.replace("'", '').strip()
            visa_dict.append(dict)

    with open('visas.csv', 'w') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=visa_info,  delimiter=',')
        writer.writeheader()
        writer.writerows(visa_dict)


def quality_check(fact_immigration: DataFrame,
                  dim_country: DataFrame,
                  dim_time: DataFrame,
                  dim_airport: DataFrame,
                  dim_temperature: DataFrame,
                  dim_visa: DataFrame,
                  dim_port: DataFrame,
                  dim_state: DataFrame,
                  sparkContext) -> None:
    """
    Performs two data quality checks against dataframes.
    1. Each table is not empty.
    2. Validate that immigrations fact table contains only valid values as per dimensional tables.
    """
    error_count = 0
    if fact_immigration.count() == 0:
        error_count += 1
        print("Invalid dataset. Immigrations fact is empty.")
    else:
        print("Data quality check passed for Immigrations fact with record_count: {} records.".format(
            fact_immigration.count()))
    if dim_time.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim time is empty.")
    else:
        print("Data quality check passed for Dim time with record_count: {} records.".format(
            dim_time.count()))
    if dim_airport.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim airport is empty.")
    else:
        print("Data quality check passed for Dim airport with record_count: {} records.".format(
            dim_airport.count()))
    if dim_temperature.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim temperature is empty.")
    else:
        print("Data quality check passed for Dim temperature with record_count: {} records.".format(
            dim_temperature.count()))
    if dim_port.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim port is empty.")
    else:
        print("Data quality check passed for Dim port with record_count: {} records.".format(
            dim_port.count()))
    if dim_visa.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim visa is empty.")
    else:
        print("Data quality check passed for Dim visa with record_count: {} records.".format(
            dim_visa.count()))
    if dim_state.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim state is empty.")
    else:
        print("Data quality check passed for Dim state with record_count: {} records.".format(
            dim_state.count()))
    if dim_country.count() == 0:
        error_count += 1
        print("Invalid dataset. Dim country is empty.")
    else:
        print("Data quality check passed for Dim country with record_count: {} records.".format(
            dim_country.count()))

    returnedImmi = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM fact_immigration WHERE cicid IS NULL""")
    if returnedImmi.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in cicid column!")

    returnedTime = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_time WHERE date IS NULL""")
    if returnedTime.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in date column!")

    returnedAirp = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_airport WHERE ident IS NULL""")
    if returnedAirp.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in ident column!")

    returnedTemp = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_temperature WHERE temperature_id IS NULL""")
    if returnedTemp.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in temperature_id!")

    returnedCountry = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_country WHERE country_code IS NULL""")
    if returnedCountry.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in code column!")

    returnedPort = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_port WHERE port_code IS NULL""")
    if returnedPort.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in port_code column!")

    returnedState = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_state WHERE state_code IS NULL""")
    if returnedState.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in state_code!")

    returnedVisa = sparkContext.sql(
        f"""SELECT COUNT(*) as nbr FROM dim_visa WHERE i94visa IS NULL""")
    if returnedVisa.head()[0] > 0:
        error_count += 1
        print(f"Data quality check failed! Found NULL values in i94visa!")

    if error_count == 0:
        print(f"All tables passed.")
    else:
        print(f"One or more tables failed.")