from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from states import Actions
from parser_yandex_function import parse_yandex_moscow
from azure_functions import get_stat  # функция для прогона запросов
from keyboards import main_keyboard, total_keyboard, ppc_keyboard, target_keyboard  # ипорт клавиатур для меню и подменю


token = '1938283222:AAEe7C80RbtpAjW7BVBzt6qISW8VnzIpg0A'  # токен тестового бота
# token = '1944607173:AAF6YHKCarXvdwB9fozgs-S1Ogz91CSdE14'  # токен боевого бота
bot = Bot(token=token)
dp = Dispatcher(bot, storage=MemoryStorage())


# SQL запросы тотал
total_yesterday_stat = open('total_sql\total_yesterday_stat.sql').read()
total_today_stat = open(r'total_sql\total_today_stat.sql').read()
total_current_week_stat = open(r'total_sql\total_current_week_stat.sql').read()
total_current_month_stat = open(r'total_sql\total_current_month_stat.sql').read()
total_previous_month_stat = open(r'total_sql\total_previous_month_stat.sql').read()

# SQL запросы по контексту
ppc_yesterday_stat = open(r'ppc_sql\ppc_yesterday_stat.sql').read()
ppc_today_stat = open(r'ppc_sql\ppc_today_stat.sql').read()
ppc_current_week_stat = open(r'ppc_sql\ppc_current_week_stat.sql').read()
ppc_current_month_stat = open(r'ppc_sql\ppc_current_month_stat.sql').read()
ppc_previous_month_stat = open(r'ppc_sql\ppc_previous_month_stat.sql').read()

# SQL запросы по таргетированной рекламе
target_yesterday_stat = open(r'target_sql\target_yesterday_stat.sql').read()
target_today_stat = open(r'target_sql\target_today_stat.sql').read()
target_current_week_stat = open(r'target_sql\target_current_week_stat.sql').read()
target_current_month_stat = open(r'target_sql\target_current_month_stat.sql').read()
target_previous_month_stat = open(r'target_sql\target_previous_month_stat.sql').read()


# Приветственный блок
@dp.message_handler(commands=['start'], state='*')  # приветствуем и показываем клавиатуру
async def say_hello(message: types.Message, state: FSMContext):
    await state.finish()
    await message.answer('Выберете пункт меню', reply_markup=main_keyboard)
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
                await state.finish()

        except Exception as e:
            print(e)
            await message.answer('По данному запросу нет рекламных объявлений')
            await state.finish()

        finally:
            await message.answer('Выберете пункт меню', reply_markup=main_keyboard)
            await state.finish()

    else:
        await message.answer('Выберете пункт меню', reply_markup=main_keyboard)
        await state.finish()
# Блок запросов Яндекса


# сброс состояния при нажатии кнопки "вернуться назад в подменю"
@dp.callback_query_handler(lambda c: c.data == 'c_get_back', state='*')
async def reset_state(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(callback_query.from_user.id, 'Выберете пункт меню', reply_markup=main_keyboard)
    await state.finish()


# пререходим к клавиатуре TOTAL
@dp.callback_query_handler(lambda c: c.data == 'c_switch_total_result')
async def show_total_result_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(callback_query.from_user.id, 'Результаты по всем каналам', reply_markup=total_keyboard)
    await Actions.total_ad_state.set()


# запрашиваем тотал стату за "вчера" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_total_yesterday_stat', state=Actions.total_ad_state)
async def get_total_yesterday_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(total_yesterday_stat)

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
                               'Результаты по всем каналам',
                               reply_markup=total_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=total_keyboard)


# запрашиваем тоал стату за "сегодня" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_total_today_stat', state=Actions.total_ad_state)
async def get_total_today_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(total_today_stat)

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
                               'Результаты по всем каналам',
                               reply_markup=total_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=total_keyboard)


# запрашиваем тотал стату за "эту неделю" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_total_current_week_stat', state=Actions.total_ad_state)
async def get_total_current_week_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(total_current_week_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за эту неделю' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Результаты по всем каналам',
                               reply_markup=total_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=total_keyboard)


# запрашиваем тотал стату за "этот месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_total_current_month_stat', state=Actions.total_ad_state)
async def get_total_current_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(total_current_month_stat)

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
                               'Результаты по всем каналам',
                               reply_markup=total_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=total_keyboard)


# запрашиваем тотал стату за "прошлый месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_total_previous_month_stat', state=Actions.total_ad_state)
async def get_total_previous_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(total_previous_month_stat)

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
                               'Результаты по всем каналам',
                               reply_markup=total_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=total_keyboard)


# пререходим к клавиатуре по контексту
@dp.callback_query_handler(lambda c: c.data == 'c_switch_ppc_result')
async def show_ppc_result_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(callback_query.from_user.id, 'Результаты по контекстной рекламе', reply_markup=ppc_keyboard)
    await Actions.ppc_ad_state.set()


# запрашиваем стату по контекста за "вчера" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_ppc_yesterday_stat', state=Actions.ppc_ad_state)
async def get_ppc_yesterday_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(ppc_yesterday_stat)

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
                               'Результаты по контекстной рекламе',
                               reply_markup=ppc_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=ppc_keyboard)


# запрашиваем стату по контекста за "сегодня" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_ppc_today_stat', state=Actions.ppc_ad_state)
async def get_ppc_today_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(ppc_today_stat)

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
                               'Результаты по контекстной рекламе',
                               reply_markup=ppc_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=ppc_keyboard)


# запрашиваем стату по контексту за "эту неделю" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_ppc_current_week_stat', state=Actions.ppc_ad_state)
async def get_ppc_current_week_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(ppc_current_week_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за эту неделю' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Результаты по контекстной рекламе',
                               reply_markup=ppc_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=ppc_keyboard)


# статистика по контексту за текущий месяц
@dp.callback_query_handler(lambda c: c.data == 'c_ppc_current_month_stat', state=Actions.ppc_ad_state)
async def get_ppc_current_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(ppc_current_month_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за текущий месяц' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Результаты по контекстной рекламе',
                               reply_markup=ppc_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=ppc_keyboard)


# статистика из zure за прошлый месяц по контексту
@dp.callback_query_handler(lambda c: c.data == 'c_ppc_previous_month_stat', state=Actions.ppc_ad_state)
async def get_ppc_previous_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(ppc_previous_month_stat)

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
                               'Результаты по контекстной рекламе',
                               reply_markup=ppc_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=ppc_keyboard)


# переход к клавиатуре таргетированной рекламе
@dp.callback_query_handler(lambda c: c.data == 'c_switch_target_result')
async def show_target_result_menu(callback_query: types.CallbackQuery, state: FSMContext):
    await bot.send_message(callback_query.from_user.id,
                           'Результаты по таргетированной рекламе', reply_markup=target_keyboard)
    await Actions.target_ad_state.set()


# запрашиваем стату по таргетированной рекламе за "вчера" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_target_yesterday_stat', state=Actions.target_ad_state)
async def get_target_yesterday_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(target_yesterday_stat)

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
                               'Результаты по таргетированной рекламе',
                               reply_markup=target_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=target_keyboard)


# запрашиваем стату по таргетированной рекламе за "сегодня" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_target_today_stat', state=Actions.target_ad_state)
async def get_target_today_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(target_today_stat)

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
                               'Результаты по таргетированной рекламе',
                               reply_markup=target_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=target_keyboard)


# запрашиваем стату по таргетированной рекламе за "эту неделю" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_target_current_week_stat', state=Actions.target_ad_state)
async def get_target_current_week_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(target_current_week_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход за эту неделю' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Результаты по контекстной рекламе',
                               reply_markup=target_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=target_keyboard)


# запрашиваем стату по таргетированной рекламе за "текущий месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_target_current_month_stat', state=Actions.target_ad_state)
async def get_target_current_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(target_current_month_stat)

        await bot.answer_callback_query(callback_query.id)
        await bot.send_message(callback_query.from_user.id,
                               'Расход на текущий месяц' + '\n' +
                               'Составляет: ' + str(round(message['adcost'], 0)) + ' руб.' + '\n' +
                               'Всего уникальных звонков: ' + str(message['unique_calls']) + '\n' +
                               'CPL: ' + str(message['unique_calls_cpl']) + ' руб.' + '\n' +
                               'Звонки ОП: ' + str(message['target_calls']) + '\n' +
                               'CPL ОП: ' + str(message['target_calls_cpl']) + ' руб.'
                               )
        await bot.send_message(callback_query.from_user.id,
                               'Результаты по таргетированной рекламе',
                               reply_markup=target_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=target_keyboard)


# запрашиваем стату по таргетированной рекламе за "прошлый месяц" из AZURE
@dp.callback_query_handler(lambda c: c.data == 'c_target_previous_month_stat', state=Actions.target_ad_state)
async def get_target_previous_month_stat(callback_query: types.CallbackQuery):
    await bot.send_message(callback_query.from_user.id, 'Собираю информацию, подождите немного')

    try:
        # разбираем содержимое функции
        message = get_stat(target_previous_month_stat)

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
                               'Результаты по таргетированной рекламе',
                               reply_markup=target_keyboard)
    except Exception as e:
        print(e)
        await bot.send_message(callback_query.from_user.id,
                               'Возникли проблемы, попробуйте позже',
                               reply_markup=target_keyboard)


executor.start_polling(dp, skip_updates=True)
