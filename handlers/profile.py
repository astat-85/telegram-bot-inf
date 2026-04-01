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
from aiogram.exceptions import TelegramBadRequest

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
    # Сбрасываем флаг редактирования при начале нового заполнения
    await state.update_data(edit_mode=False)
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
    """Обработка выбора пола (Универсальная: и для заполнения, и для редактирования)"""
    await callback.answer()
    state_data = await state.get_data()
    edit_mode = state_data.get('edit_mode', False)
    choice = callback.data.replace("gender_", "")
    gender_val = 'male' if choice == 'male' else 'female'
    
    if edit_mode:
        # === РЕЖИМ РЕДАКТИРОВАНИЯ ===
        logger.info(f"User {callback.from_user.id} editing gender to {gender_val}")
        global profile_db
        user_id = callback.from_user.id
        username = callback.from_user.username or f"user_{user_id}"
        
        current_profile = profile_db.get_profile(user_id)
        if not current_profile:
            await callback.answer("❌ Профиль не найден", show_alert=True)
            return
        
        current_profile['gender'] = gender_val
        profile_db.save_profile(user_id, username, current_profile)
        
        gender_text = "Мужской" if gender_val == 'male' else "Женский"
        try:
            await callback.message.edit_text(
                f"✅ Пол установлен: {gender_text}",
                reply_markup=get_edit_profile_keyboard()
            )
        except TelegramBadRequest:
            pass # Игнорируем ошибку, если сообщение не изменилось
        await state.clear()
    else:
        # === РЕЖИМ ЗАПОЛНЕНИЯ ===
        profile_data = state_data.get('profile_data', {})
        profile_data['gender'] = gender_val
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
    """Обработка введенного города (Первичное заполнение и Редактирование)"""
    text = message.text.strip()
    state_data = await state.get_data()
    edit_mode = state_data.get('edit_mode', False)
    
    if text == "⏭ Пропустить":
        data = await state.get_data()
        profile_data = data.get('profile_data', {})
        
        # Если режим редактирования - сохраняем сразу и выходим
        if edit_mode:
            global profile_db
            user_id = message.from_user.id
            username = message.from_user.username or f"user_{user_id}"
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                current_profile['city'] = None
                current_profile['region'] = None
                current_profile['timezone'] = 'Europe/Moscow'
                current_profile['location_manually_set'] = False
                profile_db.save_profile(user_id, username, current_profile)
            
            await message.answer("✅ Город сброшен.", reply_markup=get_edit_profile_keyboard())
            await state.clear()
            return

        # Логика первичного заполнения
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
    logger.info(f"🔍 Поиск города: '{text}'")
    cities = city_db.search(text)
    logger.info(f"🔍 Найдено городов: {len(cities)}")
    
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
        
        if edit_mode:
            # РЕЖИМ РЕДАКТИРОВАНИЯ: Сохраняем и возвращаем в меню
            user_id = message.from_user.id
            username = message.from_user.username or f"user_{user_id}"
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                current_profile['city'] = city['name']
                current_profile['region'] = city['region']['name']
                current_profile['timezone'] = city['timezone']['tzid']
                current_profile['location_manually_set'] = True
                profile_db.save_profile(user_id, username, current_profile)
            
            await message.answer(f"✅ Город обновлен: {city['name']}", reply_markup=get_edit_profile_keyboard())
            await state.clear()
        else:
            # РЕЖИМ ЗАПОЛНЕНИЯ: Переходим дальше
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
    state_data = await state.get_data()
    edit_mode = state_data.get('edit_mode', False)

    if data == "city_retry":
        await callback.message.delete()
        await callback.message.answer("🏰 Введите название города:", reply_markup=get_back_keyboard())
        return
    
    if data == "city_skip":
        if edit_mode:
            global profile_db
            user_id = callback.from_user.id
            username = callback.from_user.username or f"user_{user_id}"
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                current_profile['city'] = None
                current_profile['region'] = None
                current_profile['timezone'] = 'Europe/Moscow'
                profile_db.save_profile(user_id, username, current_profile)
            try:
                await callback.message.edit_text("✅ Город сброшен.", reply_markup=get_edit_profile_keyboard())
            except TelegramBadRequest:
                pass
            await state.clear()
            return
        
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
            if edit_mode:
                # РЕЖИМ РЕДАКТИРОВАНИЯ
                user_id = callback.from_user.id
                username = callback.from_user.username or f"user_{user_id}"
                current_profile = profile_db.get_profile(user_id)
                if current_profile:
                    current_profile['city'] = selected_city['name']
                    current_profile['region'] = selected_city['region']['name']
                    current_profile['timezone'] = selected_city['timezone']['tzid']
                    profile_db.save_profile(user_id, username, current_profile)
                
                try:
                    await callback.message.edit_text(
                        f"✅ Город обновлен: {selected_city['name']}", 
                        reply_markup=get_edit_profile_keyboard()
                    )
                except TelegramBadRequest:
                    pass
                await state.clear()
            else:
                # РЕЖИМ ЗАПОЛНЕНИЯ
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
    state_data = await state.get_data()
    edit_mode = state_data.get('edit_mode', False)
    
    if text == "⏭ Пропустить":
        if edit_mode:
            user_id = message.from_user.id
            username = message.from_user.username or f"user_{user_id}"
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                current_profile['birth_day'] = None
                current_profile['birth_month'] = None
                current_profile['birth_year'] = None
                profile_db.save_profile(user_id, username, current_profile)
            await message.answer("✅ Дата рождения сброшена.", reply_markup=get_edit_profile_keyboard())
            await state.clear()
            return

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
    
    if edit_mode:
        # РЕЖИМ РЕДАКТИРОВАНИЯ
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        current_profile = profile_db.get_profile(user_id)
        if current_profile:
            current_profile['birth_day'] = day
            current_profile['birth_month'] = month
            if year:
                current_profile['birth_year'] = year
            profile_db.save_profile(user_id, username, current_profile)
        
        date_str = f"{day:02d}.{month:02d}"
        if year:
            date_str += f".{year}"
        await message.answer(f"✅ Дата рождения обновлена: {date_str}", reply_markup=get_edit_profile_keyboard())
        await state.clear()
    else:
        # РЕЖИМ ЗАПОЛНЕНИЯ
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
    # Устанавливаем флаг редактирования
    await state.update_data(edit_mode=True)
    try:
        await callback.message.edit_text(
            "✏️ <b>Редактирование профиля</b>\n\n"
            "Что хотите изменить?",
            reply_markup=get_edit_profile_keyboard()
        )
    except TelegramBadRequest:
        pass

@router.callback_query(F.data.startswith("edit_"))
async def edit_field_choice(callback: CallbackQuery, state: FSMContext):
    """Выбор поля для редактирования"""
    await callback.answer()
    field = callback.data.replace("edit_", "")
    
    # Флаг уже установлен в profile_edit, но продублируем для надежности
    await state.update_data(edit_mode=True)

    if field == "name":
        try:
            await callback.message.edit_text("✏️ <b>Редактирование имени</b>\n\nВведите новое имя или ФИО:")
        except TelegramBadRequest:
            pass
        await callback.message.answer("📝 Введите новое имя:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_name)
    elif field == "city":
        try:
            await callback.message.edit_text("🏰 <b>Редактирование города</b>\n\nВведите ваш город:")
        except TelegramBadRequest:
            pass
        await callback.message.answer("📝 Введите новый город:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_city)
    elif field == "gender":
        logger.info(f"User {callback.from_user.id} clicked Edit Gender")
        # Для пола мы не меняем состояние FSM, а просто показываем кнопки.
        # Обработка произойдет в gender_choice_callback, который проверит флаг edit_mode.
        try:
            markup = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Мужской", callback_data="gender_male"),
                 InlineKeyboardButton(text="Женский", callback_data="gender_female")],
                [InlineKeyboardButton(text="❌ Отмена", callback_data="profile_edit")]
            ])
            await callback.message.edit_text("👤 <b>Выберите пол:</b>", reply_markup=markup)
        except TelegramBadRequest:
            pass
    elif field == "birthday":
        try:
            await callback.message.edit_text("📅 <b>Редактирование даты рождения</b>\n\nВведите новую дату (ДДММ или ДД.ММ.ГГГГ):")
        except TelegramBadRequest:
            pass
        await callback.message.answer("📝 Введите новую дату:", reply_markup=get_back_keyboard())
        await state.set_state(ProfileForm.waiting_for_birthday)
    elif field == "back":
        await profile_view(callback)

# Этот обработчик больше не нужен, так как логика перенесена в gender_choice_callback
# Но оставим его пустым или удалим, чтобы не было конфликтов. 
# Лучше удалить, чтобы не дублировалось.
# @router.callback_query(F.data.startswith("set_gender_")) ... <- УДАЛЕНО

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
