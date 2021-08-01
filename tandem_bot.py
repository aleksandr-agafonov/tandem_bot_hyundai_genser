from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from states import Actions
from parser_yandex_function import parse_yandex_moscow
from azure_functions import get_stat  # функция для прогона запросов
import requests


# token = '1938283222:AAEe7C80RbtpAjW7BVBzt6qISW8VnzIpg0A'  # токен тестового бота
token = '1944607173:AAF6YHKCarXvdwB9fozgs-S1Ogz91CSdE14'  # токен боевого бота
bot = Bot(token=token)
dp = Dispatcher(bot, storage=MemoryStorage())


# SQL запросы
yesterday_stat = open('yesterday_stat.sql').read()
today_stat = open('today_stat.sql').read()
current_week_stat = open('current_week_stat.sql').read()
current_month_stat = open('current_month_stat.sql').read()
previous_month_stat = open('previous_month_stat.sql').read()


# создаем клавиатуру
show_yandex_add_button = InlineKeyboardButton('Конкуренты в Яндексе', callback_data='c_show_yandex_add')
get_previous_month_stat = InlineKeyboardButton('Статистика за прошлый месяц', callback_data='c_get_previous_month_stat')
get_current_month_stat = InlineKeyboardButton('Статистика за текущий месяц', callback_data='c_get_current_month_stat')
get_current_week_stat = InlineKeyboardButton('Статистика за неделю', callback_data='c_get_current_week_stat')
get_yesterday_stat = InlineKeyboardButton('Статистика за вчера', callback_data='c_get_yesterday_stat')
get_today_stat = InlineKeyboardButton('Статистика за сегодня', callback_data='c_get_today_stat')

keyboard = InlineKeyboardMarkup(resize_keyboard=True)
keyboard.add(show_yandex_add_button)
keyboard.add(get_previous_month_stat)
keyboard.add(get_current_month_stat)
keyboard.add(get_current_week_stat)
keyboard.add(get_yesterday_stat)
keyboard.add(get_today_stat)


# Приветственный блок
@dp.message_handler(commands=['start'])  # приветствуем и показываем клавиатуру
async def say_hello(message: types.Message):
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
    if message.text != '/start':
        yandex_parse_result = parse_yandex_moscow(message.text)

        try:
            for result in yandex_parse_result:
                head = result['head']
                ad_text = result['ad_text']
                domain = result['domain']

                await message.answer(head + '\n\n' + ad_text + '\n\n' + domain)

        except Exception as e:
            print(e)
            await message.answer('По данному запросу нет рекламных объявлений')

        finally:
            await state.finish()
            await message.answer('Чего изволите?', reply_markup=keyboard)

    else:
        await state.finish()
        await message.answer('Чего изволите?', reply_markup=keyboard)
# Блок запросов Яндекса


# запрашиваем стату за "вчера" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_yesterday_stat')
async def get_yesterday_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(yesterday_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на: ' + str(message['date']) + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Чего изволите?',
                               reply_markup=keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=keyboard)


# запрашиваем стату за "сегодня" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_today_stat')
async def get_today_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(today_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на: ' + str(message['date']) + ' ' + str(message['max_hour']) + ' часов' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Чего изволите?',
                               reply_markup=keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=keyboard)


# запрашиваем стату за "эту неделю" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_current_week_stat')
async def get_current_week_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(current_week_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на: ' + str(message['date']) + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Чего изволите?',
                               reply_markup=keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=keyboard)


# запрашиваем стату за "этот месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_current_month_stat')
async def get_current_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(current_month_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за этот месяц' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Чего изволите?',
                               reply_markup=keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=keyboard)


# запрашиваем стату за "прошлый месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_get_previous_month_stat')
async def get_previous_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(previous_month_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за прошлый месяц' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Чего изволите?',
                               reply_markup=keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=keyboard)


# запрос IP адресс - скрытый функционал
@dp.message_handler(commands=['ip'])
async def get_my_ip(callback_query: types.CallbackQuery):
    req = requests.get('http://ip-api.com/json/')
    await bot.send_message(callback_query.from_user.id, req.json())


executor.start_polling(dp)
