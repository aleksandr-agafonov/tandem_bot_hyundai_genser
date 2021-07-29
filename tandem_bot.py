from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from azure_functions import get_stat  # функция для прогона запросов
from azure_functions import get_adcost_yesterday, get_calls_yesterday, get_target_calls_yesterday  # запросы за вчера
from azure_functions import get_adcost_today, get_calls_today, get_target_calls_today  # запросы за сегодня
from azure_functions import get_adcost_current_month, get_calls_current_month, get_target_calls_current_month  # запросы за этот месяц
from azure_functions import get_adcost_previous_month, get_calls_previous_month, get_target_calls_previous_month  # запросы за прошлый месяц
from azure_functions import get_adcost_current_week, get_calls_current_week, get_target_calls_current_week  # запросы за эту неделю
from states import Actions
import requests
from bs4 import BeautifulSoup
# import json


token = '1944607173:AAF6YHKCarXvdwB9fozgs-S1Ogz91CSdE14'
bot = Bot(token=token)
dp = Dispatcher(bot, storage=MemoryStorage())


# создаем клавиатуру
show_yandex_add_button = InlineKeyboardButton('Конкуренты в Яндексе', callback_data='c_show_yandex_add')
# screen_yandex_add_button = InlineKeyboardButton('Скрин выдачи в Яндексе', callback_data='c_screen_yandex_add')
# screen_google_add_button = InlineKeyboardButton('Скрин выдачи в Google', callback_data='c_screen_google_add')
get_previous_month_stat = InlineKeyboardButton('Статистика за прошлый месяц', callback_data='c_get_previous_month_stat')
get_current_month_stat = InlineKeyboardButton('Статистика за текущий месяц', callback_data='c_get_current_month_stat')
get_current_week_stat = InlineKeyboardButton('Статистика за неделю', callback_data='c_get_current_week_stat')
get_yesterday_stat = InlineKeyboardButton('Статистика за вчера', callback_data='c_get_yesterday_stat')
get_today_stat = InlineKeyboardButton('Статистика за сегодня', callback_data='c_get_today_stat')

keyboard = InlineKeyboardMarkup(resize_keyboard=True)
keyboard.add(show_yandex_add_button)
# keyboard.add(screen_yandex_add_button)
# keyboard.add(screen_google_add_button)
keyboard.add(get_previous_month_stat)
keyboard.add(get_current_month_stat)
keyboard.add(get_current_week_stat)
keyboard.add(get_yesterday_stat)
keyboard.add(get_today_stat)


# Приветственный блок
@dp.message_handler(commands=['start'])  # приветствуем и показываем клавиатуру
async def say_hello(message: types.Message):
    print(message)
    await message.answer('Привет! Чего изволите?', reply_markup=keyboard)
# Приветственный блок


# Блок запросов Яндекса
@dp.callback_query_handler(lambda c: c.data == 'c_show_yandex_add')
async def get_yandex_add_query(callback_query: types.CallbackQuery):
    await bot.answer_callback_query(callback_query.id)
    await bot.send_message(callback_query.from_user.id, 'Введи поисковой запрос для Yandex')
    await Actions.yandex_add_state.set()


@dp.message_handler(state=Actions.yandex_add_state)
async def get_yandex_add_text(message: types.message, state: FSMContext):
    print(message)
    if message.text != '/start':
        url = 'https://www.yandex.ru/search/ads?text='
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0'}
        search_query = message.text.replace(' ', '+')

        req = requests.get(url + search_query + '&lr=1', headers=headers, stream=True)
        soup = BeautifulSoup(req.content.decode('utf-8'), 'html.parser')

        try:
            for ad in soup.find_all('li', attrs={'class': 'serp-item'})[:3]:
                head = ad.find('div', attrs={'class': 'OrganicTitle-LinkText'}).text  # заголовок
                ad_text = ad.find('div', attrs={'class': 'Typo_text_m'}).text  # тексты
                domain = ad.find('div', attrs={'class': 'Organic-Path'}).find('b').text  # видимый домен

                await message.answer(head + '\n\n' + ad_text + '\n\n' + domain)

        except:
            await message.answer('По данному запросу нет рекламных объявлений')

        finally:
            await state.finish()
            await message.answer('Чего изволите?', reply_markup=keyboard)

    else:
        await state.finish()
        await message.answer('Чего изволите?', reply_markup=keyboard)
# Блок запросов Яндекса


# блок скринов Яндекса
# @dp.callback_query_handler(lambda c: c.data == 'c_screen_yandex_add')  # обрабатываем клик по кнопке и ставим состояние
# async def get_yandex_add_screenshot(callback_query: types.CallbackQuery):
#     await bot.answer_callback_query(callback_query.id)
#     await bot.send_message(callback_query.from_user.id, 'Введи поисковой запрос для Yandex для снятия скриншота')
#     await Actions.yandex_screen_state.set()
#
#
# @dp.message_handler(state=Actions.yandex_screen_state)
# async def get_yandex_screen(message: types.message, state: FSMContext):
#     if message.text != '/start':
#         search_query = message.text.replace(' ', '+')
#         yandex_ad_url = 'https://www.yandex.ru/search/ads?text=' + search_query + '&lr=1'
#
#         url = ' https://api.topvisor.com/v2/json/get/webScreens_2'
#         api_key = '43012ad5e875832a46fe'
#
#         headers = {
#             'User-Id': '291326',
#             'Authorization': 'bearer ' + api_key,
#             'Content-type': 'application/json'
#         }
#
#         params = {
#             'url': yandex_ad_url,
#             'w': 1000,
#             'h': 2350,
#         }
#
#         body = json.dumps(params)
#         req = requests.post(url, body, headers=headers)
#
#         with open('myfile.png', 'wb') as f:
#             f.write(req.content)
#
#         screen = open('myfile.png', 'rb')
#         await message.answer_document(screen)
#         await state.finish()
#         await message.answer('Чего изволите?', reply_markup=keyboard)
#     else:
#         await state.finish()
#         await message.answer('Чего изволите?', reply_markup=keyboard)
# # блок скринов Яндекса
#
#
# # блок скринов Goolge
# @dp.callback_query_handler(lambda c: c.data == 'c_screen_google_add')  # обрабатываем клик по кнопке и ставим состояние
# async def get_google_add_screenshot(callback_query: types.CallbackQuery):
#     await bot.answer_callback_query(callback_query.id)
#     await bot.send_message(callback_query.from_user.id, 'Введи поисковой запрос для Google для снятия скриншота')
#     await Actions.google_screen_state.set()
#
#
# @dp.message_handler(state=Actions.google_screen_state)
# async def get_google_screen(message: types.message, state: FSMContext):
#     if message.text != '/start':
#         search_query = message.text.replace(' ', '+')
#         google_ad_url = 'https://www.google.ru/search?q=' + search_query
#
#         url = ' https://api.topvisor.com/v2/json/get/webScreens_2'
#         api_key = '43012ad5e875832a46fe'
#
#         headers = {
#             'User-Id': '291326',
#             'Authorization': 'bearer ' + api_key,
#             'Content-type': 'application/json'
#         }
#
#         params = {
#             'url': google_ad_url,
#             'w': 1000,
#             'h': 2350,
#         }
#
#         body = json.dumps(params)
#         req = requests.post(url, body, headers=headers)
#
#         try:
#             with open('myfile.png', 'wb') as f:
#                 f.write(req.content)
#
#             screen = open('myfile.png', 'rb')
#             await message.answer_document(screen)
#             await state.finish()
#             await message.answer('Чего изволите?', reply_markup=keyboard)
#
#         except:
#             await message.answer('Возникла ошибка, попробуйте позже', reply_markup=keyboard)
#     else:
#         await state.finish()
#         await message.answer('Чего изволите?', reply_markup=keyboard)
# блок скринов Goolge


# запрашиваем стату за "вчера" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_yesterday_stat')
async def yesterday_stat(callback_query: types.CallbackQuery):
    print(callback_query)
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(get_adcost_yesterday.read(), get_calls_yesterday.read(), get_target_calls_yesterday.read())
        stat_date = message[0][0]
        adcost = message[0][1]
        calls = message[1][1]
        target_calls = message[2][2]
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на: ' + str(stat_date) + '\n' +
                               'Составляет: ' + str(round(adcost, 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(calls) + '\n' +
                               'CPL: ' + str(round(adcost / calls, 0)) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(target_calls) + '\n' +
                               'CPL ОП: ' + str(round(adcost / target_calls, 0)) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)
    except:
        await bot.send_message(callback_query.from_user.id, 'Возникли проблемы, попробуйте позже', reply_markup=keyboard)


# запрашиваем стату за "сегодня" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_today_stat')
async def today_stat(callback_query: types.CallbackQuery):
    print(callback_query)
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(get_adcost_today.read(), get_calls_today.read(), get_target_calls_today.read())
        stat_date = message[0][0]
        adcost = message[0][1]
        calls = message[1][1]

        # првоеряем что есть данные за сегодня
        if message[2] is None:
            last_update_hour = ''
            target_calls = 0
            cpl_op = str(0)
        else:
            last_update_hour = str(message[2][1]) + ' часов'
            target_calls = message[2][2]
            cpl_op = str(round(adcost / target_calls, 0))


        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на: ' + str(stat_date) + ', ' + str(last_update_hour) + '\n' +
                               'Составляет: ' + str(round(adcost, 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(calls) + '\n' +
                               'CPL: ' + str(round(adcost / calls, 0)) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(target_calls) + '\n' +
                               'CPL ОП: ' + cpl_op + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)
    except ValueError as e:
        print(e)
        await bot.send_message(callback_query.from_user.id, 'Возникли проблемы, попробуйте позже', reply_markup=keyboard)


# запрашиваем стату за текущий месяц из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_current_month_stat')
async def current_month_stat(callback_query: types.CallbackQuery):
    print(callback_query)
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(get_adcost_current_month.read(), get_calls_current_month.read(), get_target_calls_current_month.read())
        adcost = message[0][0]
        calls = message[1][0]
        target_calls = message[2][0]
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за этот месяц: ' + str(round(adcost, 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(calls) + '\n' +
                               'CPL: ' + str(round(adcost / calls, 0)) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(target_calls) + '\n' +
                               'CPL ОП: ' + str(round(adcost / target_calls, 0)) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)
    except:
        await bot.send_message(callback_query.from_user.id, 'Возникли проблемы, попробуйте позже', reply_markup=keyboard)


# запрашиваем стату за прошлый месяц из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_previous_month_stat')
async def previous_month_stat(callback_query: types.CallbackQuery):
    print(callback_query)
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(get_adcost_previous_month.read(), get_calls_previous_month.read(), get_target_calls_previous_month.read())
        adcost = message[0][0]
        calls = message[1][0]
        target_calls = message[2][0]
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за прошлый месяц: ' + str(round(adcost, 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(calls) + '\n' +
                               'CPL: ' + str(round(adcost / calls, 0)) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(target_calls) + '\n' +
                               'CPL ОП: ' + str(round(adcost / target_calls, 0)) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)
    except:
        await bot.send_message(callback_query.from_user.id, 'Возникли проблемы, попробуйте позже', reply_markup=keyboard)


# запрашиваем стату за текущую неделю
@dp.callback_query_handler(lambda c: c.data == 'c_get_current_week_stat')
async def current_week_stat(callback_query: types.CallbackQuery):
    print(callback_query)
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(get_adcost_current_week.read(), get_calls_current_week.read(), get_target_calls_current_week.read())
        adcost = message[0][0]
        calls = message[1][0]
        target_calls = message[2][0]
        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за эту неделю: ' + str(round(adcost, 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(calls) + '\n' +
                               'CPL: ' + str(round(adcost / calls, 0)) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(target_calls) + '\n' +
                               'CPL ОП: ' + str(round(adcost / target_calls, 0)) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)
    except:
        await bot.send_message(callback_query.from_user.id, 'Возникли проблемы, попробуйте позже', reply_markup=keyboard)


# запрос IP адресс - скрытый функционал
@dp.message_handler(commands=['ip'])
async def get_my_ip(callback_query: types.CallbackQuery):
    req = requests.get('https://api.myip.com/')
    print(req.content.decode('utf-8'))
    await bot.send_message(callback_query.from_user.id, req.json()['ip'])
    await bot.send_message(callback_query.from_user.id, 'Чего изволите?', reply_markup=keyboard)


executor.start_polling(dp)
