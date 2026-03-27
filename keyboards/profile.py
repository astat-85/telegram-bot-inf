"""
Клавиатуры для раздела профиля
"""
from aiogram.types import (
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)


def get_profile_menu_keyboard(has_profile: bool = False, has_accounts: bool = False) -> InlineKeyboardMarkup:
    """
    Меню профиля
    Args:
        has_profile: есть ли профиль у пользователя
        has_accounts: есть ли привязанные аккаунты
    """
    if has_profile:
        buttons = [
            [InlineKeyboardButton(text="👤 Просмотр профиля", callback_data="profile_view")],
        ]
        
        # Кнопка управления никами показывается всегда, если есть профиль
        buttons.append([InlineKeyboardButton(text="🎮 Мои ники", callback_data="profile_accounts")])
        
        buttons.append([InlineKeyboardButton(text="✏️ Редактировать", callback_data="profile_edit")])
        buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")])
    else:
        buttons = [
            [InlineKeyboardButton(text="📝 Заполнить профиль", callback_data="profile_fill")],
            [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
        ]

    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_accounts_management_keyboard(accounts: list, page: int = 1, total_pages: int = 1) -> InlineKeyboardMarkup:
    """
    Клавиатура для управления привязанными никами
    Args:
        accounts: список привязанных аккаунтов
        page: текущая страница
        total_pages: всего страниц
    """
    buttons = []

    # Показываем аккаунты с кнопками отвязки
    for acc in accounts:
        nickname = acc.get('game_nickname', '?')
        acc_id = acc.get('id')
        
        # Добавляем предупреждение если аккаунт неполный
        warning = ""
        required_fields = ['power', 'bm', 'pl1', 'pl2', 'pl3', 'dragon', 'buffs_stands', 'buffs_research']
        is_incomplete = False
        for field in required_fields:
            value = acc.get(field, '')
            if not value or value == '—' or value == '':
                is_incomplete = True
                break
        
        if is_incomplete:
            warning = "⚠️ "
        
        buttons.append([
            InlineKeyboardButton(
                text=f"🗑️ {warning}{nickname}", 
                callback_data=f"unlink_account_{acc_id}"
            )
        ])

    # Навигация по страницам (если больше 5 аккаунтов)
    if len(accounts) > 5 and total_pages > 1:
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="◀️", callback_data=f"accounts_page_{page-1}"))
        nav_buttons.append(InlineKeyboardButton(text=f"{page}/{total_pages}", callback_data="noop"))
        if page < total_pages:
            nav_buttons.append(InlineKeyboardButton(text="▶️", callback_data=f"accounts_page_{page+1}"))
        if nav_buttons:
            buttons.append(nav_buttons)

    # Кнопки действий
    buttons.append([InlineKeyboardButton(text="➕ Привязать новый ник", callback_data="link_new_account")])
    buttons.append([InlineKeyboardButton(text="📝 Создать новый аккаунт", callback_data="new_account_from_profile")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_view")])

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


def get_link_account_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура для выбора привязки аккаунта
    """
    buttons = [
        [
            InlineKeyboardButton(text="✅ Да, привязать", callback_data="link_yes"),
            InlineKeyboardButton(text="❌ Нет, не привязывать", callback_data="link_no")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_confirm_unlink_keyboard(account_id: int, nickname: str) -> InlineKeyboardMarkup:
    """
    Клавиатура подтверждения отвязки аккаунта
    """
    buttons = [
        [
            InlineKeyboardButton(text="✅ Да, отвязать", callback_data=f"confirm_unlink_{account_id}"),
            InlineKeyboardButton(text="❌ Нет", callback_data="profile_accounts")
        ]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_unlink_success_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура после успешной отвязки
    """
    buttons = [
        [InlineKeyboardButton(text="🎮 Мои ники", callback_data="profile_accounts")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="menu")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def get_no_accounts_to_link_keyboard() -> InlineKeyboardMarkup:
    """
    Клавиатура когда нет аккаунтов для привязки
    """
    buttons = [
        [InlineKeyboardButton(text="➕ Создать новый аккаунт", callback_data="new_account")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="profile_view")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)
