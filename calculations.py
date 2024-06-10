import os
import pandas as pd
import datetime
import numpy as np


class Calculations:
    def __init__(self, files):
        self.trips = self.produce_trips_table(files)
        self.daily_counts = self.calculate_daily_counts(self.get_trips())
        self.monthly_counts = self.calculate_monthly_counts(self.get_trips())

    def get_trips(self):
        return self.trips

    def get_daily_counts(self):
        return self.daily_counts

    def get_monthly_counts(self):
        return self.monthly_counts

    def produce_trips_table(self, files):
        df = [pd.read_csv(file) for file in files]
        trips = pd.concat(df, ignore_index=True)
        trips['Starttime'] = pd.to_datetime(trips['Starttime'])
        return trips

    def calculate_daily_counts(self, trips):
        trips['day'] = trips['Starttime'].dt.strftime('%m/%d/%Y')
        from_counts = trips.groupby(['day', 'From station id']).size().reset_index(name='fromCNT')
        from_counts.rename(columns={'From station id': 'station_id'}, inplace=True)
        to_counts = trips.groupby(['day', 'To station id']).size().reset_index(name='toCNT')
        to_counts.rename(columns={'To station id': 'station_id'}, inplace=True)
        daily_counts = pd.merge(from_counts, to_counts, how='outer', on=['day', 'station_id'])
        daily_counts.fillna(0, inplace=True)
        daily_counts['rebalCNT'] = abs(daily_counts['fromCNT'] - daily_counts['toCNT'])

        daily_counts['day'] = daily_counts['day'].astype(str)
        daily_counts['station_id'] = daily_counts['station_id'].astype(int)
        daily_counts['fromCNT'] = daily_counts['fromCNT'].astype(int)
        daily_counts['toCNT'] = daily_counts['toCNT'].astype(int)
        daily_counts['rebalCNT'] = daily_counts['rebalCNT'].astype(int)

        return daily_counts[['day', 'station_id', 'fromCNT', 'toCNT', 'rebalCNT']]

    def calculate_monthly_counts(self, trips):
        trips['month'] = trips['Starttime'].dt.strftime('%m/%Y')
        from_counts = trips.groupby(['month', 'From station id']).size().reset_index(name='fromCNT')
        from_counts.rename(columns={'From station id': 'station_id'}, inplace=True)
        to_counts = trips.groupby(['month', 'To station id']).size().reset_index(name='toCNT')
        to_counts.rename(columns={'To station id': 'station_id'}, inplace=True)
        monthly_counts = pd.merge(from_counts, to_counts, how='outer', on=['month', 'station_id'])
        monthly_counts.fillna(0, inplace=True)
        monthly_counts['rebalCNT'] = abs(monthly_counts['fromCNT'] - monthly_counts['toCNT'])

        monthly_counts['month'] = monthly_counts['month'].astype(str)
        monthly_counts['station_id'] = monthly_counts['station_id'].astype(int)
        monthly_counts['fromCNT'] = monthly_counts['fromCNT'].astype(int)
        monthly_counts['toCNT'] = monthly_counts['toCNT'].astype(int)
        monthly_counts['rebalCNT'] = monthly_counts['rebalCNT'].astype(int)

        return monthly_counts[['month', 'station_id', 'fromCNT', 'toCNT', 'rebalCNT']]


if __name__ == "__main__":
    calculations = Calculations(
        ['HealthyRideRentals2021-Q1.csv', 'HealthyRideRentals2021-Q2.csv', 'HealthyRideRentals2021-Q3.csv'])
    print("-------------- Trips Table ---------------")
    print(calculations.get_trips().head(10))
    print()
    print("-------------- Daily Counts ---------------")
    print(calculations.get_daily_counts().head(10))
    print()
    print("------------- Monthly Counts---------------")
    print(calculations.get_monthly_counts().head(10))
    print()