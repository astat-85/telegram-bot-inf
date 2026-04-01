"""
Клавиатуры для раздела профиля
"""
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)


def get_profile_menu_keyboard(has_profile: bool = False) -> InlineKeyboardMarkup:
    """
    Меню профиля
    Args:
        has_profile: есть ли профиль у пользователя
    """
    if has_profile:
        buttons = [
            [InlineKeyboardButton(text="👤 Просмотр профиля", callback_data="profile_view")],
            [InlineKeyboardButton(text="✏️ Редактировать", callback_data="profile_edit")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]
    else:
        buttons = [
            [InlineKeyboardButton(text="📝 Заполнить профиль", callback_data="profile_fill")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_edit_profile_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора поля редактирования
    """
    buttons = [
        [InlineKeyboardButton(text="👤 ФИО", callback_data="edit_name")],
        [InlineKeyboardButton(text="⚥ Пол", callback_data="edit_gender")],
        [InlineKeyboardButton(text="🏰 Город", callback_data="edit_city")],
        [InlineKeyboardButton(text="📅 Дата рождения", callback_data="edit_birthday")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_view")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_city_choice_keyboard(cities: list) -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора города из нескольких вариантов
    """
    buttons = []
    for city in cities[:10]:  # максимум 10 вариантов
        city_name = city.get('name', '')
        region_name = city.get('region', {}).get('name', '')
        if region_name:
            text = f"{city_name} ({region_name})"
        else:
            text = city_name
        
        # Используем ID города для callback
        city_id = city.get('id', '')
        if city_id:
            callback = f"city_select_{city_id}"
        else:
            # Если нет ID, используем упрощённый вариант
            simple_name = city_name.replace(' ', '_').replace('-', '_')
            simple_region = region_name.replace(' ', '_').replace('-', '_')
            callback = f"city_{simple_name}_{simple_region}"
        
        buttons.append([InlineKeyboardButton(text=text, callback_data=callback)])

    buttons.append([InlineKeyboardButton(text="🔄 Ввести заново", callback_data="city_retry")])
    buttons.append([InlineKeyboardButton(text="⏭ Пропустить (МСК)", callback_data="city_skip")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_skip_keyboard() -> ReplyKeyboardMarkup:
    """
    Reply клавиатура с кнопками пропуска
    """
    buttons = [
        [KeyboardButton(text="⏭ Пропустить")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)


def get_back_keyboard() -> ReplyKeyboardMarkup:
    """
    Reply клавиатура с кнопкой назад
    """
    buttons = [
        [KeyboardButton(text="⬅️ Назад")]
    ]
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)
