from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton


# общая клаиатура
show_yandex_add_button = InlineKeyboardButton('Конкуренты в Яндексе', callback_data='c_show_yandex_add')
switch_total_result = InlineKeyboardButton('Суммарные результаты', callback_data='c_switch_total_result')
switch_ppc_result = InlineKeyboardButton('Контекстная реклама', callback_data='c_switch_ppc_result')
switch_target_result = InlineKeyboardButton('Таргетированная реклама', callback_data='c_switch_target_result')

main_keyboard = InlineKeyboardMarkup(resize_keyboard=True)
main_keyboard.add(show_yandex_add_button)
main_keyboard.add(switch_total_result)
main_keyboard.add(switch_ppc_result)
main_keyboard.add(switch_target_result)


# клавиатура с тотал результатами
total_previous_month_button = InlineKeyboardButton('Статистика за прошлый месяц', callback_data='c_total_previous_month_stat')
total_current_month_button = InlineKeyboardButton('Статистика за текущий месяц', callback_data='c_total_current_month_stat')
total_current_week_button = InlineKeyboardButton('Статистика за неделю', callback_data='c_total_current_week_stat')
total_yesterday_button = InlineKeyboardButton('Статистика за вчера', callback_data='c_total_yesterday_stat')
total_today_button = InlineKeyboardButton('Статистика за сегодня', callback_data='c_total_today_stat')
get_back = InlineKeyboardButton('Вернуться назад', callback_data='c_get_back')

total_keyboard = InlineKeyboardMarkup(resize_keyboard=True)
total_keyboard.add(total_previous_month_button)
total_keyboard.add(total_current_month_button)
total_keyboard.add(total_current_week_button)
total_keyboard.add(total_yesterday_button)
total_keyboard.add(total_today_button)
total_keyboard.add(get_back)


# клавиатура контекст
ppc_previous_month_button = InlineKeyboardButton('Статистика за прошлый месяц', callback_data='c_ppc_previous_month_stat')
ppc_current_month_button = InlineKeyboardButton('Статистика за текущий месяц', callback_data='c_ppc_current_month_stat')
ppc_current_week_button = InlineKeyboardButton('Статистика за неделю', callback_data='c_ppc_current_week_stat')
ppc_yesterday_button = InlineKeyboardButton('Статистика за вчера', callback_data='c_ppc_yesterday_stat')
ppc_today_button = InlineKeyboardButton('Статистика за сегодня', callback_data='c_ppc_today_stat')

ppc_keyboard = InlineKeyboardMarkup(resize_keyboard=True)
ppc_keyboard.add(ppc_previous_month_button)
ppc_keyboard.add(ppc_current_month_button)
ppc_keyboard.add(ppc_current_week_button)
ppc_keyboard.add(ppc_yesterday_button )
ppc_keyboard.add(ppc_today_button)
ppc_keyboard.add(get_back)


# клаиатура таргетированная реклама
target_previous_month_button = InlineKeyboardButton('Статистика за прошлый месяц', callback_data='c_target_previous_month_stat')
target_current_month_button = InlineKeyboardButton('Статистика за текущий месяц', callback_data='c_target_current_month_stat')
target_current_week_button = InlineKeyboardButton('Статистика за неделю', callback_data='c_target_current_week_stat')
target_yesterday_button = InlineKeyboardButton('Статистика за вчера', callback_data='c_target_yesterday_stat')
target_today_button = InlineKeyboardButton('Статистика за сегодня', callback_data='c_target_today_stat')

target_keyboard = InlineKeyboardMarkup(resize_keyboard=True)
target_keyboard.add(target_previous_month_button)
target_keyboard.add(target_current_month_button)
target_keyboard.add(target_current_week_button)
target_keyboard.add(target_yesterday_button )
target_keyboard.add(target_today_button)
target_keyboard.add(get_back)
