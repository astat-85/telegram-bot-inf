"""
Обработчики для раздела "Личные данные"
"""
import logging
from datetime import datetime
from aiogram import Router, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from database.profile_db import ProfileDB
from utils.gender import detect_gender_by_name
from utils.date_parser import parse_birthday
from keyboards.profile import (
    get_profile_menu_keyboard,
    get_edit_profile_keyboard,
    get_city_choice_keyboard,
    get_skip_keyboard,
    get_back_keyboard,
    get_accounts_management_keyboard,
    get_link_account_keyboard,
    get_confirm_unlink_keyboard,
    get_unlink_success_keyboard,
    get_no_accounts_to_link_keyboard
)
from cities.city_db import CityDatabase

logger = logging.getLogger(__name__)

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
_check_subscription_func = None
profile_db = None
db = None
city_db = CityDatabase()

# Состояния FSM для заполнения профиля
class ProfileForm(StatesGroup):
    waiting_for_name = State()          # Ожидание ФИО
    waiting_for_gender_choice = State() # Ожидание выбора пола (если имя неоднозначное)
    waiting_for_city = State()           # Ожидание города
    waiting_for_birthday = State()       # Ожидание даты рождения
    waiting_for_confirm = State()        # Подтверждение данных
    waiting_for_account_link = State()   # Ожидание выбора привязки аккаунта

# Роутер для профиля
router = Router()

def format_timezone_offset(tzid: str) -> str:
    """Преобразует название часового пояса в смещение относительно Москвы"""
    timezone_offsets = {
        'Europe/Kaliningrad': 'MSK-1 (UTC+2)',
        'Europe/Moscow': 'MSK (UTC+3)',
        'Europe/Volgograd': 'MSK (UTC+3)',
        'Europe/Kirov': 'MSK (UTC+3)',
        'Europe/Astrakhan': 'MSK+1 (UTC+4)',
        'Europe/Samara': 'MSK+1 (UTC+4)',
        'Europe/Saratov': 'MSK+1 (UTC+4)',
        'Europe/Ulyanovsk': 'MSK+1 (UTC+4)',
        'Asia/Yekaterinburg': 'MSK+2 (UTC+5)',
        'Asia/Omsk': 'MSK+3 (UTC+6)',
        'Asia/Novosibirsk': 'MSK+4 (UTC+7)',
        'Asia/Barnaul': 'MSK+4 (UTC+7)',
        'Asia/Tomsk': 'MSK+4 (UTC+7)',
        'Asia/Novokuznetsk': 'MSK+4 (UTC+7)',
        'Asia/Krasnoyarsk': 'MSK+4 (UTC+7)',
        'Asia/Irkutsk': 'MSK+5 (UTC+8)',
        'Asia/Chita': 'MSK+6 (UTC+9)',
        'Asia/Yakutsk': 'MSK+6 (UTC+9)',
        'Asia/Khandyga': 'MSK+6 (UTC+9)',
        'Asia/Vladivostok': 'MSK+7 (UTC+10)',
        'Asia/Ust-Nera': 'MSK+7 (UTC+10)',
        'Asia/Magadan': 'MSK+8 (UTC+11)',
        'Asia/Sakhalin': 'MSK+8 (UTC+11)',
        'Asia/Srednekolymsk': 'MSK+8 (UTC+11)',
        'Asia/Kamchatka': 'MSK+9 (UTC+12)',
        'Asia/Anadyr': 'MSK+9 (UTC+12)'
    }
    if tzid in timezone_offsets:
        return timezone_offsets[tzid]
    
    if 'Asia/Yekaterinburg' in tzid:
        return 'MSK+2 (UTC+5)'
    elif 'Asia/Omsk' in tzid:
        return 'MSK+3 (UTC+6)'
    elif 'Asia/Novosibirsk' in tzid:
        return 'MSK+4 (UTC+7)'
    elif 'Asia/Krasnoyarsk' in tzid:
        return 'MSK+4 (UTC+7)'
    elif 'Asia/Irkutsk' in tzid:
        return 'MSK+5 (UTC+8)'
    elif 'Asia/Yakutsk' in tzid:
        return 'MSK+6 (UTC+9)'
    elif 'Asia/Vladivostok' in tzid:
        return 'MSK+7 (UTC+10)'
    elif 'Asia/Magadan' in tzid:
        return 'MSK+8 (UTC+11)'
    elif 'Asia/Kamchatka' in tzid:
        return 'MSK+9 (UTC+12)'
    
    return tzid.replace('Europe/', '').replace('Asia/', '')

async def check_subscription_wrapper(user_id: int) -> bool:
    """Обертка для проверки подписки"""
    global _check_subscription_func
    if _check_subscription_func is None:
        print("⚠️ Функция проверки подписки не установлена, разрешаем доступ")
        return True
    return await _check_subscription_func(user_id)

def format_profile(profile: dict, linked_accounts: list = None) -> str:
    """Форматирует данные профиля для вывода с привязанными никами"""
    full_name = profile['first_name']
    if profile.get('last_name'):
        full_name = f"{profile['last_name']} {full_name}"
    if profile.get('middle_name'):
        full_name += f" {profile['middle_name']}"
    
    gender_text = "👨 Мужской" if profile['gender'] == 'male' else "👩 Женский" if profile['gender'] == 'female' else "—"
    
    if profile.get('birth_day') and profile.get('birth_month'):
        birth = f"{profile['birth_day']:02d}.{profile['birth_month']:02d}"
        if profile.get('birth_year'):
            birth += f".{profile['birth_year']}"
            age = datetime.now().year - profile['birth_year']
            birth += f" ({age} лет)"
    else:
        birth = "—"
    
    location = profile.get('city', '—')
    if profile.get('region'):
        location += f", {profile['region']}"
    
    timezone = profile.get('timezone', 'Europe/Moscow')
    timezone_display = format_timezone_offset(timezone)
    
    text = (
        f"👤 <b>Мой профиль</b>\n\n"
        f"<b>Имя:</b> {full_name}\n"
        f"<b>Пол:</b> {gender_text}\n"
        f"<b>Дата рождения:</b> {birth}\n"
        f"<b>Город:</b> {location}\n"
        f"<b>Часовой пояс:</b> {timezone_display}\n"
    )
    
    # Привязанные ники
    if linked_accounts:
        nicks = [acc.get('game_nickname', '?') for acc in linked_accounts]
        text += f"\n<b>🎮 Игровые ники:</b> {' / '.join(nicks)}\n"
    else:
        text += f"\n<b>🎮 Игровые ники:</b> не привязаны\n"
    
    if profile.get('location_manually_set'):
        text += "\n<i>✅ Город указан вручную</i>\n"
    else:
        text += "\n<i>⏰ Часовой пояс: МСК (по умолчанию)</i>\n"
    
    return text

@router.message(Command("profile"))
@router.message(F.text == "👤 Мой профиль")
async def cmd_profile(message: Message):
    """Показ профиля или предложение заполнить"""
    global profile_db
    user_id = message.from_user.id
    
    if profile_db is None:
        await message.answer("❌ Ошибка инициализации профиля. Попробуйте позже.")
        return
    
    if not await check_subscription_wrapper(user_id):
        await message.answer(
            "❌ Для доступа к профилю нужно быть подписчиком группы",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        return
    
    profile = profile_db.get_profile(user_id)
    
    if not profile:
        text = (
            "👤 <b>Личное дело</b>\n\n"
            "Сведения необходимы:\n"
            "• Для идентификации личного состава\n"
            "• Определения часового пояса (чтоб не будить среди ночи)\n"
            "• Знать возрастной состав союза (кто тут старый краб)\n"
            "• Напомнить Вам о Вашем ДР (чтоб не забыли налить)"
        )
        await message.answer(
            text,
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
    else:
        # Получаем привязанные аккаунты
        linked_accounts = profile_db.get_linked_accounts(user_id)
        text = format_profile(profile, linked_accounts)
        await message.answer(
            text,
            reply_markup=get_profile_menu_keyboard(has_profile=True, has_accounts=bool(linked_accounts)),
            parse_mode="HTML"
        )

@router.callback_query(F.data == "profile_fill")
async def start_profile_fill(callback: CallbackQuery, state: FSMContext):
    """Начало заполнения профиля"""
    await callback.answer()
    await callback.message.edit_text(
        "📝 <b>Заполнение профиля</b>\n\n"
        "Введите ваше <b>имя</b> (обязательно) и, если хотите, "
        "фамилию и отчество.\n\n"
        "Примеры:\n"
        "• <i>Иван</i>\n"
        "• <i>Иван Петров</i>\n"
        "• <i>Иван Сергеевич Петров</i>"
    )
    await callback.message.answer(
        "📝 Введите Имя или ФИО:",
        reply_markup=get_back_keyboard()
    )
    await state.set_state(ProfileForm.waiting_for_name)

@router.message(ProfileForm.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    """Обработка введенного ФИО"""
    text = message.text.strip()
    if text == "⬅️ Назад":
        data = await state.get_data()
        edit_mode = data.get('edit_mode', False)
        
        if edit_mode:
            await state.clear()
            await cmd_profile(message)
        else:
            await state.clear()
            await message.answer(
                "❌ Заполнение отменено",
                reply_markup=get_profile_menu_keyboard(has_profile=False)
            )
        return
    
    parts = text.split()
    
    data = {}
    if len(parts) == 1:
        data['first_name'] = parts[0].capitalize()
        data['last_name'] = None
        data['middle_name'] = None
    elif len(parts) == 2:
        data['first_name'] = parts[0].capitalize()
        data['last_name'] = parts[1].capitalize()
        data['middle_name'] = None
    elif len(parts) >= 3:
        data['first_name'] = parts[0].capitalize()
        data['middle_name'] = parts[1].capitalize()
        data['last_name'] = ' '.join(parts[2:]).capitalize()
    else:
        await message.answer("❌ Слишком мало данных. Введите хотя бы имя.")
        return
    
    gender = detect_gender_by_name(data['first_name'])
    
    if gender == 'male':
        data['gender'] = 'male'
        gender_text = "мужской"
        await message.answer(f"✅ Определен пол: {gender_text}")
        
        await state.update_data(profile_data=data)
        
        await message.answer(
            "🏰 <b>Город</b>\n\n"
            "Введите ваш город (необязательно).\n"
            "Если укажете город, часовой пояс определится автоматически.\n\n"
            "Или нажмите <b>⏭ Пропустить</b>.",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_city)
        
    elif gender == 'female':
        data['gender'] = 'female'
        gender_text = "женский"
        await message.answer(f"✅ Определен пол: {gender_text}")
        
        await state.update_data(profile_data=data)
        
        await message.answer(
            "🏰 <b>Город</b>\n\n"
            "Введите ваш город (необязательно).\n"
            "Если укажете город, часовой пояс определится автоматически.\n\n"
            "Или нажмите <b>⏭ Пропустить</b>.",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_city)
        
    else:
        await state.update_data(profile_data=data)
        
        gender_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="gender_male")],
            [InlineKeyboardButton(text="👩 Женский", callback_data="gender_female")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="gender_cancel")]
        ])
        
        await message.answer(
            f"❓ Не удалось однозначно определить пол для имени '{data['first_name']}'.\n"
            f"Пожалуйста, укажите ваш пол:",
            reply_markup=gender_kb
        )
        await state.set_state(ProfileForm.waiting_for_gender_choice)

@router.callback_query(F.data.startswith("gender_"))
async def gender_choice_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора пола"""
    await callback.answer()
    choice = callback.data.replace("gender_", "")
    
    if choice == "cancel":
        data = await state.get_data()
        edit_mode = data.get('edit_mode', False)
        
        await callback.message.delete()
        
        if edit_mode:
            await state.clear()
            await cmd_profile(callback.message)
        else:
            await state.clear()
            await callback.message.answer(
                "❌ Заполнение отменено",
                reply_markup=get_profile_menu_keyboard(has_profile=False)
            )
        return
    
    state_data = await state.get_data()
    profile_data = state_data.get('profile_data', {})
    
    if choice == "male":
        profile_data['gender'] = 'male'
        gender_text = "мужской"
    elif choice == "female":
        profile_data['gender'] = 'female'
        gender_text = "женский"
    else:
        await callback.message.delete()
        await callback.message.answer(
            "❌ Неверный выбор",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        await state.clear()
        return
    
    await state.update_data(profile_data=profile_data)
    
    await callback.message.delete()
    await callback.message.answer(f"✅ Выбран пол: {gender_text}")
    
    await callback.message.answer(
        "🏰 <b>Город</b>\n\n"
        "Введите ваш город (необязательно).\n"
        "Если укажете город, часовой пояс определится автоматически.\n\n"
        "Или нажмите <b>⏭ Пропустить</b>.",
        reply_markup=get_skip_keyboard()
    )
    await state.set_state(ProfileForm.waiting_for_city)

# ... (остальные функции продолжаются в том же духе - все пробелы удалены)
