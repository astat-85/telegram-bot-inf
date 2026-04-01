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
    waiting_for_name = State()
    waiting_for_gender_choice = State()
    waiting_for_city = State()
    waiting_for_birthday = State()
    waiting_for_confirm = State()
    waiting_for_account_link = State()

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
    return tzid.replace('Europe/', '').replace('Asia/', '')

async def check_subscription_wrapper(user_id: int) -> bool:
    """Обертка для проверки подписки"""
    global _check_subscription_func
    if _check_subscription_func is None:
        return True
    return await _check_subscription_func(user_id)

def format_profile(profile: dict, linked_accounts: list = None) -> str:
    """Форматирует данные профиля для вывода"""
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
    
    if linked_accounts:
        nicks = [acc.get('game_nickname', '?') for acc in linked_accounts]
        text += f"\n<b>🎮 Игровые ники:</b> {' / '.join(nicks)}\n"
    else:
        text += f"\n<b>🎮 Игровые ники:</b> не привязаны\n"
    
    return text

@router.message(Command("profile"))
@router.message(F.text == "👤 Мой профиль")
async def cmd_profile(message: Message):
    """Показ профиля или предложение заполнить"""
    global profile_db
    user_id = message.from_user.id
    
    if profile_db is None:
        await message.answer("❌ Ошибка инициализации профиля")
        return
    
    profile = profile_db.get_profile(user_id)
    
    if not profile:
        text = (
            "👤 <b>Личное дело</b>\n\n"
            "Сведения необходимы:\n"
            "• Для идентификации личного состава\n"
            "• Определения часового пояса\n"
            "• Напомнить о Вашем ДР"
        )
        await message.answer(text, reply_markup=get_profile_menu_keyboard(has_profile=False))
    else:
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
        "Введите ваше <b>имя</b> (обязательно) и, если хотите, фамилию и отчество.\n\n"
        "Примеры:\n"
        "• Иван\n"
        "• Иван Петров\n"
        "• Иван Сергеевич Петров"
    )
    await callback.message.answer("📝 Введите Имя или ФИО:", reply_markup=get_back_keyboard())
    await state.set_state(ProfileForm.waiting_for_name)

@router.message(ProfileForm.waiting_for_name)
async def process_name(message: Message, state: FSMContext):
    """Обработка введенного ФИО"""
    text = message.text.strip()
    if text == "⬅️ Назад":
        await state.clear()
        await cmd_profile(message)
        return
    
    parts = text.split()
    data = {}
    if len(parts) == 1:
        data['first_name'] = parts[0].capitalize()
    elif len(parts) == 2:
        data['first_name'] = parts[0].capitalize()
        data['last_name'] = parts[1].capitalize()
    elif len(parts) >= 3:
        data['first_name'] = parts[0].capitalize()
        data['middle_name'] = parts[1].capitalize()
        data['last_name'] = ' '.join(parts[2:]).capitalize()
    
    gender = detect_gender_by_name(data['first_name'])
    if gender:
        data['gender'] = gender
        await message.answer(f"✅ Определен пол: {'мужской' if gender == 'male' else 'женский'}")
    else:
        gender_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="gender_male")],
            [InlineKeyboardButton(text="👩 Женский", callback_data="gender_female")]
        ])
        await message.answer(f"❓ Не удалось определить пол. Выберите:", reply_markup=gender_kb)
        await state.update_data(profile_data=data)
        await state.set_state(ProfileForm.waiting_for_gender_choice)
        return
    
    await state.update_data(profile_data=data)
    await message.answer(
        "🏰 <b>Город</b>\n\n"
        "Введите ваш город (необязательно).\n"
        "Или нажмите <b>⏭ Пропустить</b>.",
        reply_markup=get_skip_keyboard()
    )
    await state.set_state(ProfileForm.waiting_for_city)

@router.callback_query(F.data.startswith("gender_"))
async def gender_choice_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора пола"""
    await callback.answer()
    choice = callback.data.replace("gender_", "")
    state_data = await state.get_data()
    profile_data = state_data.get('profile_data', {})
    profile_data['gender'] = 'male' if choice == 'male' else 'female'
    await state.update_data(profile_data=profile_data)
    await callback.message.delete()
    await callback.message.answer(
        "🏰 <b>Город</b>\n\n"
        "Введите ваш город (необязательно).\n"
        "Или нажмите <b>⏭ Пропустить</b>.",
        reply_markup=get_skip_keyboard()
    )
    await state.set_state(ProfileForm.waiting_for_city)

@router.message(ProfileForm.waiting_for_city)
async def process_city(message: Message, state: FSMContext):
    """Обработка введенного города"""
    text = message.text.strip()
    
    if text == "⏭ Пропустить":
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        profile_data['timezone'] = 'Europe/Moscow'
        profile_data['location_manually_set'] = False
        await state.update_data(profile_data=profile_data)
        await message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения:\n"
            "• ДДММ (1503)\n"
            "• ДДММГГГГ (15031990)\n"
            "• ДД.ММ.ГГГГ (15.03.1990)",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        return
    
    if text == "⬅️ Назад":
        await state.clear()
        await cmd_profile(message)
        return
    
    # ===== ПОИСК ГОРОДА =====
    print(f"🔍 Поиск города: '{text}'")
    cities = city_db.search(text)
    print(f"🔍 Найдено городов: {len(cities)}")
    
    if not cities:
        await message.answer(
            f"❌ Город '{text}' не найден.\n\n"
            "Проверьте написание или нажмите <b>⏭ Пропустить</b>.",
            reply_markup=get_skip_keyboard()
        )
        return
    
    if len(cities) == 1:
        city = cities[0]
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        profile_data['city'] = city['name']
        profile_data['region'] = city['region']['name']
        profile_data['timezone'] = city['timezone']['tzid']
        profile_data['location_manually_set'] = True
        await state.update_data(profile_data=profile_data)
        await message.answer(f"✅ Город: {city['name']}, {city['region']['name']}")
        await message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения:\n"
            "• ДДММ (1503)\n"
            "• ДДММГГГГ (15031990)\n"
            "• ДД.ММ.ГГГГ (15.03.1990)",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
    else:
        await message.answer("🔍 Найдено несколько городов. Уточните:", reply_markup=get_city_choice_keyboard(cities))

@router.callback_query(F.data.startswith("city_"))
async def city_choice_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора города из списка"""
    await callback.answer()
    data = callback.data
    
    if data == "city_retry":
        await callback.message.delete()
        await callback.message.answer("🏰 Введите название города:", reply_markup=get_back_keyboard())
        return
    
    if data == "city_skip":
        state_data = await state.get_data()
        profile_data = state_data.get('profile_data', {})
        profile_data['timezone'] = 'Europe/Moscow'
        await state.update_data(profile_data=profile_data)
        await callback.message.delete()
        await callback.message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения:",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        return
    
    if data.startswith("city_select_"):
        city_id_str = data.replace("city_select_", "")
        all_cities = city_db.get_all_cities()
        selected_city = None
        for city in all_cities:
            if str(city.get('id', '')) == city_id_str:
                selected_city = city
                break
        
        if selected_city:
            state_data = await state.get_data()
            profile_data = state_data.get('profile_data', {})
            profile_data['city'] = selected_city['name']
            profile_data['region'] = selected_city['region']['name']
            profile_data['timezone'] = selected_city['timezone']['tzid']
            await state.update_data(profile_data=profile_data)
            await callback.message.delete()
            await callback.message.answer(f"✅ Город: {selected_city['name']}")
            await callback.message.answer(
                "📅 <b>Дата рождения</b>\n\n"
                "Введите дату рождения:",
                reply_markup=get_skip_keyboard()
            )
            await state.set_state(ProfileForm.waiting_for_birthday)

@router.message(ProfileForm.waiting_for_birthday)
async def process_birthday(message: Message, state: FSMContext):
    """Обработка введенной даты рождения"""
    global profile_db
    text = message.text.strip()
    
    if text == "⏭ Пропустить":
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        profile_db.save_profile(user_id, username, profile_data)
        profile = profile_db.get_profile(user_id)
        linked_accounts = profile_db.get_linked_accounts(user_id)
        await message.answer(
            "✅ <b>Профиль сохранен!</b>\n\n" + format_profile(profile, linked_accounts),
            reply_markup=get_profile_menu_keyboard(has_profile=True, has_accounts=bool(linked_accounts)),
            parse_mode="HTML"
        )
        await state.clear()
        return
    
    if text == "⬅️ Назад":
        await state.clear()
        await cmd_profile(message)
        return
    
    parsed = parse_birthday(text)
    if not parsed:
        await message.answer(
            "❌ Неверный формат даты.\n\n"
            "Примеры:\n"
            "• ДДММ (1503)\n"
            "• ДДММГГГГ (15031990)\n"
            "• ДД.ММ.ГГГГ (15.03.1990)",
            reply_markup=get_skip_keyboard()
        )
        return
    
    day, month, year = parsed
    data = await state.get_data()
    profile_data = data.get('profile_data', {})
    profile_data['birth_day'] = day
    profile_data['birth_month'] = month
    if year:
        profile_data['birth_year'] = year
    
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    profile_db.save_profile(user_id, username, profile_data)
    profile = profile_db.get_profile(user_id)
    linked_accounts = profile_db.get_linked_accounts(user_id)
    
    date_str = f"{day:02d}.{month:02d}"
    if year:
        date_str += f".{year}"
    
    await message.answer(
        f"✅ <b>Профиль сохранен!</b>\n\n"
        f"📅 Дата рождения: {date_str}\n\n" + format_profile(profile, linked_accounts),
        reply_markup=get_profile_menu_keyboard(has_profile=True, has_accounts=bool(linked_accounts)),
        parse_mode="HTML"
    )
    await state.clear()

@router.callback_query(F.data == "profile_view")
async def profile_view(callback: CallbackQuery):
    """Просмотр профиля"""
    await callback.answer()
    global profile_db
    profile = profile_db.get_profile(callback.from_user.id)
    if profile:
        linked_accounts = profile_db.get_linked_accounts(callback.from_user.id)
        await callback.message.edit_text(
            format_profile(profile, linked_accounts),
            reply_markup=get_profile_menu_keyboard(has_profile=True, has_accounts=bool(linked_accounts)),
            parse_mode="HTML"
        )
    else:
        await callback.message.edit_text("👤 Профиль не найден", reply_markup=get_profile_menu_keyboard(has_profile=False))

@router.callback_query(F.data == "profile_edit")
async def profile_edit(callback: CallbackQuery, state: FSMContext):
    """Редактирование профиля"""
    await callback.answer()
    await callback.message.edit_text(
        "✏️ <b>Редактирование профиля</b>\n\n"
        "Что хотите изменить?",
        reply_markup=get_edit_profile_keyboard()
    )

@router.callback_query(F.data.startswith("edit_"))
async def edit_field_choice(callback: CallbackQuery, state: FSMContext):
    """Выбор поля для редактирования"""
    await callback.answer()
    global profile_db
    field = callback.data.replace("edit_", "")
    
    if field == "name":
        await callback.message.edit_text("✏️ <b>Редактирование имени</b>\n\n" "Введите новое имя или ФИО:")
        await callback.message.answer("📝 Введите новое имя:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_name)
        await state.update_data(edit_mode=True)
    elif field == "city":
        await callback.message.edit_text("🏰 <b>Редактирование города</b>\n\n" "Введите ваш город:")
        await callback.message.answer("📝 Введите новый город:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_city)
        await state.update_data(edit_mode=True)
    elif field == "gender":
        logger.info(f"User {callback.from_user.id} clicked Edit Gender")
        markup = InlineKeyboardMarkup(row_width=2)
        markup.add(
            InlineKeyboardButton("Мужской", callback_data="set_gender_male"),
            InlineKeyboardButton("Женский", callback_data="set_gender_female")
        )
        markup.add(InlineKeyboardButton("❌ Отмена", callback_data="profile_edit"))
        
        await callback.message.edit_text("👤 <b>Выберите пол:</b>", reply_markup=markup)
        # Состояние не меняем, так как выбор мгновенный через callback
    elif field == "birthday":
        await callback.message.edit_text("📅 <b>Редактирование даты рождения</b>\n\n" "Введите новую дату:")
        await callback.message.answer("📝 Введите новую дату:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_birthday)
        await state.update_data(edit_mode=True)
    elif field == "back":
        await profile_view(callback)

@router.callback_query(F.data.startswith("set_gender_"))
async def process_set_gender(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора пола при редактировании"""
    try:
        logger.info(f"User {callback.from_user.id} selected gender: {callback.data}")
        await callback.answer()
        global profile_db
        
        if profile_db is None:
            await callback.answer("❌ Ошибка БД", show_alert=True)
            return

        gender = 'male' if 'male' in callback.data else 'female'
        user_id = callback.from_user.id
        username = callback.from_user.username or f"user_{user_id}"
        
        # Получаем текущий профиль
        current_profile = profile_db.get_profile(user_id)
        if not current_profile:
            await callback.answer("❌ Профиль не найден", show_alert=True)
            return
        
        # Обновляем поле gender
        current_profile['gender'] = gender
        
        # Сохраняем
        save_result = profile_db.save_profile(user_id, username, current_profile)
        if not save_result:
            logger.error("Failed to save profile gender")
            await callback.answer("❌ Ошибка сохранения", show_alert=True)
            return
        
        logger.info(f"Gender saved successfully for user {user_id}")
        
        await callback.message.edit_text(
            f"✅ Пол установлен: {'Мужской' if gender == 'male' else 'Женский'}",
            reply_markup=get_edit_profile_keyboard()
        )
    except Exception as e:
        logger.error(f"Error in process_set_gender: {e}", exc_info=True)
        await callback.answer(f"❌ Ошибка: {e}", show_alert=True)


@router.callback_query(F.data == "profile_accounts")
async def profile_accounts(callback: CallbackQuery, state: FSMContext):
    """Показывает привязанные ники"""
    await callback.answer()
    global profile_db
    user_id = callback.from_user.id
    profile = profile_db.get_profile(user_id)
    if not profile:
        await callback.message.edit_text("❌ Сначала заполните профиль", reply_markup=get_profile_menu_keyboard(has_profile=False))
        return
    linked_accounts = profile_db.get_linked_accounts(user_id)
    if linked_accounts:
        text = "🎮 <b>Ваши игровые ники</b>\n\n"
        for i, acc in enumerate(linked_accounts, 1):
            nickname = acc.get('game_nickname', '?')
            text += f"{i}. {nickname}\n"
        await callback.message.edit_text(text, reply_markup=get_accounts_management_keyboard(linked_accounts))
    else:
        await callback.message.edit_text(
            "🎮 <b>У вас нет привязанных ников</b>",
            reply_markup=InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="➕ Привязать", callback_data="link_existing_account")],
                [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_view")]
            ])
        )
