import holidays
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# bq_project = gbq["bq_project"]
# bq_dataset = gbq["bq_dataset"]
# bq_table = gbq["bq_dim_date_table"]
# gbq_key_path = gbq["bq_key_path"]

def get_special_event(row, event):
        end = None
        date_actual = row["date_actual"].date()
        year = row["year_actual"]
        if year < 2002:  # Wayfair establish year
            return False
        # Find Thanksgiving of the current year
        start = ([x[0] for x in holidays.UnitedStates(years=year).items() if x[1] == "Thanksgiving"][0])

        if event == "peak":
            end = datetime(year, 12, 31).date()
        elif event == "cyber5":
            end = (start + timedelta(days=4))

        if start <= date_actual <= end:
            return True
        else:
            return False

def model(dbt, session):
    
    dbt.config(
      materialized = 'table',
      submission_method = 'cluster',
      gcs_bucket = 'wf-ae-gat-model-storage-prod',
      dataproc_region = 'us-central1',
      dataproc_cluster_name = 'gat-dataproc-cluster',
      cluster_by = 'date_actual',
      schema = 'ops_reporting_core'
    )
    
    start_date = datetime(2021, 1, 1)
    end_date = datetime(2049, 1, 1)
    
    #df = pd.DataFrame()
    #df_dummy = pd.DataFrame(pd.date_range('1900-01-01', '1900-01-01').strftime('%Y%m%d'), columns=['date_integer'])
    df = pd.DataFrame(pd.date_range(start_date, end_date).strftime('%Y%m%d'), columns=['date_integer'])
    #df = df.append(df_dummy, ignore_index=True).astype(int)
    #df = df.append(df_dates, ignore_index=True).astype(int)

    # Dim Date attributes
    df['date_actual'] = pd.to_datetime(df['date_integer'], format='%Y%m%d')  # Unique
    df['year_actual'] = df['date_actual'].dt.year
    df['half_year_actual'] = df['date_actual'].dt.month.apply(lambda x: 1 if x < 7 else 2)
    df['half_year_name'] = df['date_actual'].dt.month.apply(lambda x: 'H1' if x < 7 else 'H2')
    df['year_quarter'] = df['date_actual'].dt.year.astype(str) + ['Q'] + df['date_actual'].dt.quarter.astype(str)
    df['quarter_actual'] = df['date_actual'].dt.quarter
    df['quarter_name'] = ['Q'] + df['date_actual'].dt.quarter.astype(str)
    df['year_month'] = df['date_actual'].dt.year.astype(str) + ['/'] + df['date_actual'].dt.strftime('%b')
    df['month_actual'] = df['date_actual'].dt.month
    df['month_name'] = df['date_actual'].dt.strftime('%B')
    df['month_name_abbreviated'] = df['date_actual'].dt.strftime('%b')
    df['week_of_year_monday'] =  df['date_actual'].dt.to_period('W-SUN').dt.week
    df['week_of_year_sunday'] =  df['date_actual'].dt.to_period('W-SAT').dt.week
    df['year_week_sunday'] = np.where(
        (df['date_actual'].dt.month == 1) & (df['week_of_year_sunday'] > 51),
        (df['date_actual'].dt.year-1).astype(str) + ['W'] + df['week_of_year_sunday'].astype(str).str.pad(2, fillchar='0'),
        df['date_actual'].dt.year.astype(str) + ['W'] + df['week_of_year_sunday'].astype(str).str.pad(2, fillchar='0')
        )
    df['year_week_sunday'] = np.where(
        (df['date_actual'].dt.month == 12) & (df['week_of_year_sunday'] == 1),
        (df['date_actual'].dt.year+1).astype(str) + ['W'] + df['week_of_year_sunday'].astype(str).str.pad(2, fillchar='0'),
        df['year_week_sunday']
    )
    df['year_week_monday'] = np.where(
        (df['date_actual'].dt.month == 1) & (df['week_of_year_monday'] > 51),
        (df['date_actual'].dt.year-1).astype(str) + ['W'] + df['week_of_year_monday'].astype(str).str.pad(2, fillchar='0'),
        df['date_actual'].dt.year.astype(str) + ['W'] + df['week_of_year_monday'].astype(str).str.pad(2, fillchar='0'))
    df['year_week_monday'] = np.where(
        (df['date_actual'].dt.month == 12) & (df['week_of_year_monday'] == 1),
        (df['date_actual'].dt.year+1).astype(str) + ['W'] + df['week_of_year_monday'].astype(str).str.pad(2, fillchar='0'),
        df['year_week_monday']
    )
    df['is_weekend'] = df['date_actual'].dt.dayofweek > 4
    df['day_of_week_sunday'] = df['date_actual'].dt.strftime('%w').astype(int)
    df['day_of_week_monday'] = df['day_of_week_sunday'].replace({0: 6, 1: 0, 2: 1, 3: 2, 4: 3, 5: 4, 6:5 })
    df['day_of_month'] = df['date_actual'].dt.day.astype(int)
    df['day_name'] = df['date_actual'].dt.strftime('%A')
    df['day_name_abbreviated'] = df['date_actual'].dt.strftime('%a')
    df['sunday_date'] = df['date_actual'].dt.date - pd.to_timedelta(df['day_of_week_sunday'], unit='d')
    df['is_peak'] = df.apply(get_special_event, axis=1, event="peak")
    df['is_cyber5'] = df.apply(get_special_event, axis=1, event="cyber5")
    df['date_actual'] = df['date_actual'].dt.date
    current_timestamp = datetime.utcnow()
    df['dwh_creation_timestamp'] = current_timestamp
    df['dwh_modified_timestamp'] = current_timestamp
    df.sort_values(by='date_actual',inplace=True, ascending=True)
    
    return df
