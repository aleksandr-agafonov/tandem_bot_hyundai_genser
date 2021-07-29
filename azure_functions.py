import pyodbc


server = 'prsunvsu17.database.windows.net'
database = 'mybi-hpnqdjf'
username = 'owner-hpnqdjf'
password = 'VxBvU9dYXyyr'
driver_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password


# блок запросов за "вчера"
get_adcost_yesterday = open('get_adcost_yesterday.sql').read()
get_calls_yesterday = open('get_calls_yesterday.sql', 'rb').read()
get_target_calls_yesterday = open('get_target_calls_yesterday.sql', 'rb').read()

# блок запросов за "сегодня"
get_adcost_today = open('get_adcost_today.sql').read()
get_calls_today = open('get_calls_yesterday.sql').read()
get_target_calls_today = open('get_target_calls_today.sql').read()

# Блок запросов за "этот месяц"
get_adcost_current_month = open('get_adcost_current_month.sql').read()
get_calls_current_month = open('get_calls_current_month.sql').read()
get_target_calls_current_month = open('get_target_calls_current_month.sql').read()

# Блок запросов за "прошлый месяц"
get_adcost_previous_month = open('get_adcost_previous_month.sql').read()
get_calls_previous_month = open('get_calls_previous_month.sql').read()
get_target_calls_previous_month = open('get_target_calls_previous_month.sql').read()

# Блок запросов за "эту неделю"
get_adcost_current_week = open('get_adcost_current_week.sql').read()
get_calls_current_week = open('get_calls_current_week.sql').read()
get_target_calls_current_week = open('get_target_calls_current_week.sql').read()


# функция подключения и чтения из AZURE
def get_stat(adcost, calls, target_calls):

    cnxn = pyodbc.connect(driver_string)
    cursor = cnxn.cursor()

    try:
        # получаем расходы
        cursor.execute(adcost)
        adcost_row = cursor.fetchone()
        print(adcost_row)

            # получаем звонки
        cursor.execute(calls)
        calls_row = cursor.fetchone()
        print(calls_row)

            # получаем звонки ОП
        cursor.execute(target_calls)
        target_calls_row = cursor.fetchone()
        print(target_calls_row)

        cnxn.close()

        return adcost_row, calls_row, target_calls_row
    except:
        return 'error'


#print(get_stat(get_adcost_current_week.read() , get_calls_current_week.read(), get_target_calls_current_week.read()))