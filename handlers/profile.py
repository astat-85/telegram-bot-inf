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
    get_back_keyboard
)
from cities.city_db import CityDatabase

logger = logging.getLogger(__name__)

# ========== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ==========
_check_subscription_func = None
profile_db = None  # Будет установлено из main.py
city_db = CityDatabase()

# Состояния FSM для заполнения профиля
class ProfileForm(StatesGroup):
    waiting_for_name = State()          # Ожидание ФИО
    waiting_for_gender_choice = State() # Ожидание выбора пола (если имя неоднозначное)
    waiting_for_city = State()           # Ожидание города
    waiting_for_birthday = State()       # Ожидание даты рождения
    waiting_for_confirm = State()        # Подтверждение данных

# Роутер для профиля
router = Router()

@router.callback_query()
def format_timezone_offset(tzid: str) -> str:
    """
    Преобразует название часового пояса в смещение относительно Москвы
    """
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
        text = format_profile(profile)
        await message.answer(
            text,
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )

def format_profile(profile: dict) -> str:
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
        f"<b>Часовой пояс:</b> {timezone_display}\n\n"
    )
    
    if profile.get('location_manually_set'):
        text += "<i>✅ Город указан вручную</i>\n"
    else:
        text += "<i>⏰ Часовой пояс: МСК (по умолчанию)</i>\n"
    
    return text

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
        
        edit_mode = data.get('edit_mode', False)
        
        if edit_mode:
            user_id = message.from_user.id
            username = message.from_user.username or f"user_{user_id}"
            
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                for key, value in profile_data.items():
                    if value is not None:
                        current_profile[key] = value
                profile_db.save_profile(user_id, username, current_profile)
                profile = profile_db.get_profile(user_id)
            else:
                profile_db.save_profile(user_id, username, profile_data)
                profile = profile_db.get_profile(user_id)
            
            await message.answer(
                "✅ <b>Профиль обновлен!</b>\n\n" + format_profile(profile),
                reply_markup=get_profile_menu_keyboard(has_profile=True),
                parse_mode="HTML"
            )
            await state.clear()
        else:
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
    
    cities = city_db.search(text)
    
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
        
        await message.answer(
            f"✅ Город: {city['name']}, {city['region']['name']}\n"
            f"🕒 Часовой пояс: {format_timezone_offset(city['timezone']['tzid'])}"
        )
        
        edit_mode = data.get('edit_mode', False)
        
        if edit_mode:
            user_id = message.from_user.id
            username = message.from_user.username or f"user_{user_id}"
            
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                for key, value in profile_data.items():
                    if value is not None:
                        current_profile[key] = value
                profile_db.save_profile(user_id, username, current_profile)
                profile = profile_db.get_profile(user_id)
            else:
                profile_db.save_profile(user_id, username, profile_data)
                profile = profile_db.get_profile(user_id)
            
            await message.answer(
                "✅ <b>Профиль обновлен!</b>\n\n" + format_profile(profile),
                reply_markup=get_profile_menu_keyboard(has_profile=True),
                parse_mode="HTML"
            )
            await state.clear()
        else:
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
        await message.answer(
            "🔍 Найдено несколько городов. Уточните:",
            reply_markup=get_city_choice_keyboard(cities)
        )

@router.callback_query(F.data.startswith("city_"))
async def city_choice_callback(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора города из списка"""
    global profile_db
    
    await callback.answer()
    
    data = callback.data
    print(f"🔍 ПОЛУЧЕН CALLBACK: '{data}'")
    
    if data == "city_retry":
        await callback.message.delete()
        await callback.message.answer(
            "🏰 Введите название города:",
            reply_markup=get_back_keyboard()
        )
        return
    
    if data == "city_skip":
        state_data = await state.get_data()
        profile_data = state_data.get('profile_data', {})
        profile_data['timezone'] = 'Europe/Moscow'
        profile_data['location_manually_set'] = False
        
        await state.update_data(profile_data=profile_data)
        
        await callback.message.delete()
        await callback.message.answer(
            "📅 <b>Дата рождения</b>\n\n"
            "Введите дату рождения:\n"
            "• ДДММ (1503)\n"
            "• ДДММГГГГ (15031990)\n"
            "• ДД.ММ.ГГГГ (15.03.1990)",
            reply_markup=get_skip_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        return
    
    if data.startswith("city_select_"):
        try:
            city_id_str = data.replace("city_select_", "")
            print(f"🔍 ВЫБРАН ГОРОД С ID: '{city_id_str}'")
            
            all_cities = city_db.get_all_cities()
            print(f"📊 ВСЕГО ГОРОДОВ: {len(all_cities)}")
            
            selected_city = None
            
            for city in all_cities:
                if (city.get('id') == city_id_str or 
                    city.get('okato') == city_id_str or 
                    city.get('oktmo') == city_id_str or
                    city.get('guid') == city_id_str):
                    selected_city = city
                    print(f"✅ НАЙДЕН ГОРОД: {city.get('name')}")
                    break
            
            if not selected_city:
                for city in all_cities:
                    city_id = city.get('id', '')
                    if city_id and city_id.endswith(city_id_str):
                        selected_city = city
                        print(f"✅ НАЙДЕН ГОРОД: {city.get('name')} по окончанию ID")
                        break
            
            if selected_city:
                city_name = selected_city.get('name', '')
                region_name = selected_city.get('region', {}).get('name', '')
                timezone = selected_city.get('timezone', {}).get('tzid', 'Europe/Moscow')
                
                print(f"✅ СОХРАНЯЕМ: {city_name}, {region_name}, пояс: {timezone}")
                
                state_data = await state.get_data()
                profile_data = state_data.get('profile_data', {})
                profile_data['city'] = city_name
                profile_data['region'] = region_name
                profile_data['timezone'] = timezone
                profile_data['location_manually_set'] = True
                
                await state.update_data(profile_data=profile_data)
                
                await callback.message.delete()
                
                edit_mode = state_data.get('edit_mode', False)
                
                if edit_mode:
                    user_id = callback.from_user.id
                    username = callback.from_user.username or f"user_{user_id}"
                    
                    current_profile = profile_db.get_profile(user_id)
                    if current_profile:
                        for key, value in profile_data.items():
                            if value is not None:
                                current_profile[key] = value
                        profile_db.save_profile(user_id, username, current_profile)
                    else:
                        profile_db.save_profile(user_id, username, profile_data)
                    
                    profile = profile_db.get_profile(user_id)
                    await callback.message.answer(
                        f"✅ Город: {city_name}, {region_name}\n"
                        f"🕒 Часовой пояс: {format_timezone_offset(timezone)}\n\n"
                        f"✅ <b>Профиль обновлен!</b>\n\n" + format_profile(profile),
                        reply_markup=get_profile_menu_keyboard(has_profile=True),
                        parse_mode="HTML"
                    )
                    await state.clear()
                else:
                    await callback.message.answer(
                        f"✅ Город: {city_name}, {region_name}\n"
                        f"🕒 Часовой пояс: {format_timezone_offset(timezone)}\n\n"
                        f"📅 Теперь введите дату рождения:",
                        reply_markup=get_skip_keyboard()
                    )
                    await state.set_state(ProfileForm.waiting_for_birthday)
                return
            else:
                print(f"❌ ГОРОД НЕ НАЙДЕН")
                
                await callback.message.delete()
                await callback.message.answer(
                    "❌ Город не найден.\n"
                    "🏰 Введите название вручную:",
                    reply_markup=get_back_keyboard()
                )
                return
                
        except Exception as e:
            print(f"⚠️ ОШИБКА: {e}")
            await callback.message.delete()
            await callback.message.answer(
                "❌ Ошибка при выборе города.\n"
                "🏰 Введите название вручную:",
                reply_markup=get_back_keyboard()
            )
            return
    
    city_part = data[5:]
    
    last_underscore = city_part.rfind('_')
    if last_underscore == -1:
        city_name = city_part.replace('_', ' ')
        region = ""
    else:
        region = city_part[last_underscore + 1:]
        city_name = city_part[:last_underscore].replace('_', ' ')
    
    print(f"🔍 Поиск города по названию: '{city_name}' (регион: '{region}')")
    
    if region:
        city = city_db.get_city_by_name_and_region(city_name, region)
    else:
        cities = city_db.search(city_name)
        city = cities[0] if cities else None
    
    if city:
        state_data = await state.get_data()
        profile_data = state_data.get('profile_data', {})
        profile_data['city'] = city['name']
        profile_data['region'] = city['region']['name']
        profile_data['timezone'] = city['timezone']['tzid']
        profile_data['location_manually_set'] = True
        
        await state.update_data(profile_data=profile_data)
        
        await callback.message.delete()
        
        edit_mode = state_data.get('edit_mode', False)
        
        if edit_mode:
            user_id = callback.from_user.id
            username = callback.from_user.username or f"user_{user_id}"
            
            current_profile = profile_db.get_profile(user_id)
            if current_profile:
                for key, value in profile_data.items():
                    if value is not None:
                        current_profile[key] = value
                profile_db.save_profile(user_id, username, current_profile)
            else:
                profile_db.save_profile(user_id, username, profile_data)
            
            profile = profile_db.get_profile(user_id)
            await callback.message.answer(
                f"✅ Город: {city['name']}, {city['region']['name']}\n"
                f"🕒 Часовой пояс: {format_timezone_offset(city['timezone']['tzid'])}\n\n"
                f"✅ <b>Профиль обновлен!</b>\n\n" + format_profile(profile),
                reply_markup=get_profile_menu_keyboard(has_profile=True),
                parse_mode="HTML"
            )
            await state.clear()
        else:
            await callback.message.answer(
                f"✅ Город: {city['name']}, {city['region']['name']}\n"
                f"🕒 Часовой пояс: {format_timezone_offset(city['timezone']['tzid'])}\n\n"
                f"📅 Теперь введите дату рождения:",
                reply_markup=get_skip_keyboard()
            )
            await state.set_state(ProfileForm.waiting_for_birthday)
    else:
        print(f"❌ Город не найден: {city_name}")
        await callback.message.delete()
        await callback.message.answer(
            f"❌ Город '{city_name}' не найден.\n"
            f"🏰 Введите название вручную:",
            reply_markup=get_back_keyboard()
        )

@router.message(ProfileForm.waiting_for_birthday)
async def process_birthday(message: Message, state: FSMContext):
    """Обработка введенной даты рождения"""
    global profile_db
    
    text = message.text.strip()
    
    if text == "⏭ Пропустить":
        data = await state.get_data()
        edit_mode = data.get('edit_mode', False)
        user_id = message.from_user.id
        username = message.from_user.username or f"user_{user_id}"
        
        if edit_mode:
            # При пропуске в режиме редактирования - просто показываем профиль
            profile = profile_db.get_profile(user_id)
            if profile:
                await message.answer(
                    "✅ <b>Профиль</b>\n\n" + format_profile(profile),
                    reply_markup=get_profile_menu_keyboard(has_profile=True),
                    parse_mode="HTML"
                )
            else:
                await message.answer(
                    "❌ Профиль не найден",
                    reply_markup=get_profile_menu_keyboard(has_profile=False)
                )
        else:
            # При пропуске в новом профиле - сохраняем без даты
            profile_data = data.get('profile_data', {})
            profile_db.save_profile(user_id, username, profile_data)
            profile = profile_db.get_profile(user_id)
            await message.answer(
                "✅ <b>Профиль сохранен!</b>\n\n" + format_profile(profile),
                reply_markup=get_profile_menu_keyboard(has_profile=True),
                parse_mode="HTML"
            )
        
        await state.clear()
        return
    
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
    
    # Парсим дату
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
    edit_mode = data.get('edit_mode', False)
    user_id = message.from_user.id
    username = message.from_user.username or f"user_{user_id}"
    
    if edit_mode:
        # РЕЖИМ РЕДАКТИРОВАНИЯ - получаем текущий профиль из БД
        current_profile = profile_db.get_profile(user_id)
        
        if not current_profile:
            await message.answer("❌ Профиль не найден")
            await state.clear()
            return
        
        # Обновляем дату
        current_profile['birth_day'] = day
        current_profile['birth_month'] = month
        if year:
            current_profile['birth_year'] = year
        else:
            # Если год не указан - удаляем его
            if 'birth_year' in current_profile:
                del current_profile['birth_year']
        
        # Сохраняем
        profile_db.save_profile(user_id, username, current_profile)
        
        # Получаем обновленный профиль
        updated_profile = profile_db.get_profile(user_id)
        
        date_str = f"{day:02d}.{month:02d}"
        if year:
            date_str += f".{year}"
        
        await message.answer(
            f"✅ <b>Дата рождения обновлена: {date_str}</b>\n\n" + format_profile(updated_profile),
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )
        await state.clear()
        
    else:
        # НОВЫЙ ПРОФИЛЬ - сохраняем в FSM
        profile_data['birth_day'] = day
        profile_data['birth_month'] = month
        if year:
            profile_data['birth_year'] = year
        else:
            # Если год не указан - удаляем из данных
            if 'birth_year' in profile_data:
                del profile_data['birth_year']
        
        await state.update_data(profile_data=profile_data)
        
        date_str = f"{day:02d}.{month:02d}"
        if year:
            date_str += f".{year}"
        
        # Сохраняем полный профиль
        profile_db.save_profile(user_id, username, profile_data)
        profile = profile_db.get_profile(user_id)
        
        await message.answer(
            f"✅ Дата рождения: {date_str}\n\n"
            f"Профиль успешно заполнен!",
            reply_markup=get_profile_menu_keyboard(has_profile=True)
        )
        await state.clear()

@router.callback_query(F.data == "profile_view")
async def profile_view(callback: CallbackQuery):
    """Просмотр профиля"""
    await callback.answer()
    
    global profile_db
    
    if profile_db is None:
        await callback.message.edit_text(
            "❌ Ошибка инициализации профиля. Попробуйте позже.",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        return
    
    profile = profile_db.get_profile(callback.from_user.id)
    
    if profile:
        await callback.message.edit_text(
            format_profile(profile),
            reply_markup=get_profile_menu_keyboard(has_profile=True),
            parse_mode="HTML"
        )
    else:
        await callback.message.edit_text(
            "👤 Профиль не найден",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )

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
    
    if profile_db is None:
        await callback.message.edit_text(
            "❌ Ошибка инициализации профиля. Попробуйте позже.",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        return
    
    field = callback.data.replace("edit_", "")
    
    if field == "name":
        await callback.message.edit_text(
            "✏️ <b>Редактирование имени</b>\n\n"
            "Введите ваше <b>имя</b> (обязательно).\n"
            "Фамилию и отчество можно добавить по желанию."
        )
        await callback.message.answer(
            "📝 Введите новое имя или ФИО:",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_name)
        await state.update_data(edit_mode=True)
    
    elif field == "gender":
        gender_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👨 Мужской", callback_data="edit_gender_male")],
            [InlineKeyboardButton(text="👩 Женский", callback_data="edit_gender_female")],
            [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_edit")]
        ])
        
        await callback.message.edit_text(
            "⚥ <b>Редактирование пола</b>\n\n"
            "Выберите ваш пол:",
            reply_markup=gender_kb
        )
    
    elif field == "city":
        await callback.message.edit_text(
            "🏰 <b>Редактирование города</b>\n\n"
            "Введите ваш город:"
        )
        await callback.message.answer(
            "📝 Введите новый город:",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_city)
        await state.update_data(edit_mode=True)
    
    elif field == "birthday":
        await callback.message.edit_text(
            "📅 <b>Редактирование даты рождения</b>\n\n"
            "Введите дату рождения в формате:\n"
            "• ДДММ (1503) - год останется старый\n"
            "• ДДММГГГГ (15031990) - год обновится"
        )
        await callback.message.answer(
            "📝 Введите новую дату рождения:",
            reply_markup=get_back_keyboard()
        )
        await state.set_state(ProfileForm.waiting_for_birthday)
        await state.update_data(edit_mode=True)
    
    elif field == "back":
        await profile_view(callback)

@router.callback_query(F.data.startswith("edit_gender_"))
async def edit_gender_choice(callback: CallbackQuery, state: FSMContext):
    """Обработка выбора пола при редактировании"""
    await callback.answer("🔄 Обрабатываю...")
    
    print(f"\n🔴🔴🔴 edit_gender_choice ВЫЗВАНА! 🔴🔴🔴")
    print(f"🔴 callback.data = '{callback.data}'")
    print(f"🔴 user_id = {callback.from_user.id}")
    print("🔴" * 30)
    
    await callback.answer()
    
    global profile_db
    
    choice = callback.data.replace("edit_gender_", "")
    
    if choice == "male":
        gender = 'male'
        gender_text = "мужской"
    elif choice == "female":
        gender = 'female'
        gender_text = "женский"
    else:
        await callback.message.edit_text(
            "❌ Неверный выбор",
            reply_markup=get_profile_menu_keyboard(has_profile=True)
        )
        return
    
    user_id = callback.from_user.id
    username = callback.from_user.username or f"user_{user_id}"
    
    # ПОЛУЧАЕМ ТЕКУЩИЙ ПРОФИЛЬ ИЗ БД
    profile = profile_db.get_profile(user_id)
    
    if not profile:
        await callback.message.edit_text(
            "❌ Профиль не найден",
            reply_markup=get_profile_menu_keyboard(has_profile=False)
        )
        return
    
    # Обновляем пол
    profile['gender'] = gender
    
    # Сохраняем
    profile_db.save_profile(user_id, username, profile)
    
    # Получаем обновленный профиль
    updated_profile = profile_db.get_profile(user_id)
    
    await callback.message.edit_text(
        f"✅ Пол изменён на: {gender_text}\n\n" + format_profile(updated_profile),
        reply_markup=get_profile_menu_keyboard(has_profile=True),
        parse_mode="HTML"
    )
