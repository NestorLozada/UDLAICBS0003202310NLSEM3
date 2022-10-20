import pandas as pd
from sqlalchemy.engine import Engine
from config import DataConfig, DbConfig
from utilsconnection.db_simpleConnection import simple_connection

def extract_times(db_con: Engine):
    #Dictionary for values
    times_dict = {
        "time_id": [],
        "day_name": [],
        "day_integer_in_week": [],
        "day_integer_in_month": [],
        "calendar_week_integer": [],
        "calendar_month_integer": [],
        "calendar_month_desc": [],
        "end_of_cal_month": [],
        "calendar_month_name": [],
        "calendar_quarter_desc": [],
        "calendar_year": [],
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
            times_dict['time_id'].append(id)
            times_dict['day_name'].append(day_name)
            times_dict['day_integer_in_week'].append(day_in_week)
            times_dict['day_integer_in_month'].append(day_in_month)
            times_dict['calendar_week_integer'].append(cal_week)
            times_dict['calendar_month_integer'].append(cal_month)
            times_dict['calendar_month_desc'].append(cal_month_desc)
            times_dict['end_of_cal_month'].append(eoc_month)
            times_dict['calendar_month_name'].append("")
            times_dict['calendar_quarter_desc'].append(cal_quarter)
            times_dict['calendar_year'].append(cal_year)
    if times_dict["time_id"]:
        db_con.connect().execute(f'TRUNCATE TABLE times')
        df_times = pd.DataFrame(times_dict)
        df_times.to_sql('times', db_con, if_exists="append",index=False)