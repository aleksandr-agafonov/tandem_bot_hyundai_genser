from aiogram.dispatcher.filters.state import StatesGroup, State

class Actions(StatesGroup):
    yandex_add_state = State()
    total_ad_state = State()
    ppc_ad_state = State()
    target_ad_state = State()

