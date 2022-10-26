import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig, DbConfig
from util.db_simpleConnection import simple_try

def extract_times(db_con: Engine):
    #Dictionary for values
    times_dict = {
        "TIME_ID": [],
        "DAY_NAME": [],
        "DAY_INTEGER_IN_WEEK": [],
        "DAY_INTEGER_IN_MONTH": [],
        "CALENDAR_WEEK_INTEGER": [],
        "CALENDAR_MONTH_INTEGER": [],
        "CALENDAR_MONTH_DESC": [],
        "END_OF_CAL_MONTH": [],
        "CALENDAR_MONTH_NAME": [],
        "CALENDAR_QUARTER_DESC": [],
        "CALENDAR_YEAR": [],
    }
    times_csv = pd.read_csv(DataConfig.get_csv_path('times.csv'))
    
    #Process CSV Content
    if not times_csv.empty:
        for id,day_name,day_in_week, \
            day_in_month,cal_week,cal_month, \
            cal_month_desc,eoc_month, \
            cal_quarter,cal_year \
            in zip(
                times_csv['TIME_ID'],
                times_csv['DAY_NAME'],
                times_csv['DAY_NUMBER_IN_WEEK'],
                times_csv['DAY_NUMBER_IN_MONTH'],
                times_csv['CALENDAR_WEEK_NUMBER'],
                times_csv['CALENDAR_MONTH_NUMBER'],
                times_csv['CALENDAR_MONTH_DESC'],
                times_csv['END_OF_CAL_MONTH'],
                times_csv['CALENDAR_QUARTER_DESC'],
                times_csv['CALENDAR_YEAR']
            ):
            times_dict['TIME_ID'].append(id)
            times_dict['DAY_NAME'].append(day_name)
            times_dict['DAY_INTEGER_IN_WEEK'].append(day_in_week)
            times_dict['DAY_INTEGER_IN_MONTH'].append(day_in_month)
            times_dict['CALENDAR_WEEK_INTEGER'].append(cal_week)
            times_dict['CALENDAR_MONTH_INTEGER'].append(cal_month)
            times_dict['CALENDAR_MONTH_DESC'].append(cal_month_desc)
            times_dict['END_OF_CAL_MONTH'].append(eoc_month)
            times_dict['CALENDAR_MONTH_NAME'].append("")
            times_dict['CALENDAR_QUARTER_DESC'].append(cal_quarter)
            times_dict['CALENDAR_YEAR'].append(cal_year)
    if times_dict["TIME_ID"]:
        db_con.connect().execute(f'TRUNCATE TABLE TIMES_EXT')
        df_times = pd.DataFrame(times_dict)
        df_times.to_sql('TIMES_EXT', db_con, if_exists="append",index=False)