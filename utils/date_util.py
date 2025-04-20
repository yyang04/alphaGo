from datetime import datetime, timedelta


def get_days_delta(dt, delta):
    date_obj = datetime.strptime(dt, '%Y%m%d')
    return (date_obj + timedelta(days=delta)).strftime('%Y%m%d')
