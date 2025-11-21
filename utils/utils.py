import calendar

def int_to_excel_col(n):
    result = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        result = chr(65 + remainder) + result
    return result

def excel_col_to_int(col):
    result = 0
    for char in col:
        result = result * 26 + (ord(char.upper()) - ord('A') + 1)
    return result

def next_excel_col(col):
    col_int = excel_col_to_int(col)
    next_col_int = col_int + 1
    return int_to_excel_col(next_col_int)

def get_week_of_month(year, month, day):
    month_calendar = calendar.monthcalendar(year, month)
    for week_number, week in enumerate(month_calendar, start=1):
        if day in week:
            return "Semana " + str(week_number)
        
def get_week_of_month_int(year, month, day):
    month_calendar = calendar.monthcalendar(year, month)
    for week_number, week in enumerate(month_calendar, start=1):
        if day in week:
            return week_number