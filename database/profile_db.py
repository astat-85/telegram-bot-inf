"""
Работа с таблицей профилей пользователей
"""
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, List
from pathlib import Path
import os


# ========== ФУНКЦИЯ RETRY ==========
def retry_on_db_lock(max_retries=3, delay=0.1):
    """Декоратор для повторных попыток при блокировке БД"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except sqlite3.OperationalError as e:
                    if 'database is locked' in str(e) and attempt < max_retries - 1:
                        time.sleep(delay * (attempt + 1))
                        continue
                    raise
            return func(*args, **kwargs)
        return wrapper
    return decorator


class ProfileDB:
    def __init__(self, db=None):
        """
        Инициализация с возможностью передачи готового Database объекта
        """
        self.db = db
        self.lock = threading.RLock()
        if db:
            self._create_tables()
            self.init_default_data()
    
    def _create_tables(self):
        """Создает таблицу профилей, если её нет"""
        with self.lock:
            self.db._execute('''
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT NOT NULL,
                last_name TEXT,
                middle_name TEXT,
                gender TEXT CHECK (gender IN ('male', 'female', NULL)),
                birth_day INTEGER CHECK(birth_day BETWEEN 1 AND 31),
                birth_month INTEGER CHECK(birth_month BETWEEN 1 AND 12),
                birth_year INTEGER CHECK(birth_year > 1900),
                city TEXT,
                region TEXT,
                timezone TEXT DEFAULT 'Europe/Moscow',
                location_manually_set BOOLEAN DEFAULT 0,
                is_active BOOLEAN DEFAULT 1,
                last_active TIMESTAMP,
                archived_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.db._execute("PRAGMA table_info(user_profiles)")
            columns = [col[1] for col in self.db.cursor.fetchall()]
            
            if 'is_active' not in columns:
                self.db._execute("ALTER TABLE user_profiles ADD COLUMN is_active BOOLEAN DEFAULT 1")
            if 'last_active' not in columns:
                self.db._execute("ALTER TABLE user_profiles ADD COLUMN last_active TIMESTAMP")
            if 'archived_at' not in columns:
                self.db._execute("ALTER TABLE user_profiles ADD COLUMN archived_at TIMESTAMP")
            
            self.db._execute('''
            CREATE TABLE IF NOT EXISTS user_account_links (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                profile_user_id INTEGER NOT NULL,
                game_account_id INTEGER NOT NULL,
                linked_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (profile_user_id) REFERENCES user_profiles(user_id) ON DELETE CASCADE,
                FOREIGN KEY (game_account_id) REFERENCES users(id) ON DELETE CASCADE,
                UNIQUE(profile_user_id, game_account_id)
            )
            ''')
            
            self.db._execute('''
            CREATE TABLE IF NOT EXISTS birthday_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                template_text TEXT NOT NULL,
                is_default BOOLEAN DEFAULT 0,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.db._execute('''
            CREATE TABLE IF NOT EXISTS birthday_settings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                responsible_user_id INTEGER,
                group_chat_id INTEGER,
                notification_3day BOOLEAN DEFAULT 1,
                notification_1day BOOLEAN DEFAULT 1,
                notification_day BOOLEAN DEFAULT 1,
                use_gpt BOOLEAN DEFAULT 0,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ''')
            
            self.db._execute('CREATE INDEX IF NOT EXISTS idx_profile_city ON user_profiles(city)')
            self.db._execute('CREATE INDEX IF NOT EXISTS idx_profile_active ON user_profiles(is_active, last_active)')
            self.db._execute('CREATE INDEX IF NOT EXISTS idx_profile_birthday ON user_profiles(birth_day, birth_month)')
            self.db._execute('CREATE INDEX IF NOT EXISTS idx_account_links_profile ON user_account_links(profile_user_id)')
            self.db._execute('CREATE INDEX IF NOT EXISTS idx_account_links_account ON user_account_links(game_account_id)')
            
            self.db.conn.commit()
    
    @retry_on_db_lock()
    def save_profile(self, user_id: int, username: str,  Dict[str, Any]) -> bool:
        """Сохраняет или обновляет профиль пользователя"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute("SELECT user_id FROM user_profiles WHERE user_id = ?", (user_id,))
                exists = self.db.cursor.fetchone()
                
                if exists:
                    query = '''
                    UPDATE user_profiles SET
                        username = ?,
                        first_name = ?,
                        last_name = ?,
                        middle_name = ?,
                        gender = ?,
                        birth_day = ?,
                        birth_month = ?,
                        birth_year = ?,
                        city = ?,
                        region = ?,
                        timezone = ?,
                        location_manually_set = ?,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                    '''
                    
                    self.db._execute(query, (
                        username,
                        data.get('first_name'),
                        data.get('last_name'),
                        data.get('middle_name'),
                        data.get('gender'),
                        data.get('birth_day'),
                        data.get('birth_month'),
                        data.get('birth_year'),
                        data.get('city'),
                        data.get('region'),
                        data.get('timezone', 'Europe/Moscow'),
                        data.get('location_manually_set', False),
                        user_id
                    ))
                else:
                    query = '''
                    INSERT INTO user_profiles (
                        user_id, username, first_name, last_name, middle_name,
                        gender, birth_day, birth_month, birth_year,
                        city, region, timezone, location_manually_set,
                        last_active
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                    '''
                    
                    self.db._execute(query, (
                        user_id,
                        username,
                        data.get('first_name'),
                        data.get('last_name'),
                        data.get('middle_name'),
                        data.get('gender'),
                        data.get('birth_day'),
                        data.get('birth_month'),
                        data.get('birth_year'),
                        data.get('city'),
                        data.get('region'),
                        data.get('timezone', 'Europe/Moscow'),
                        data.get('location_manually_set', False)
                    ))
                
                self.db.conn.commit()
                return True
                
            except Exception as e:
                print(f"❌ Ошибка сохранения профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def get_profile(self, user_id: int) -> Optional[Dict[str, Any]]:
        """Получает профиль пользователя"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute("SELECT * FROM user_profiles WHERE user_id = ?", (user_id,))
                row = self.db.cursor.fetchone()
                return dict(row) if row else None
            except Exception as e:
                print(f"❌ Ошибка получения профиля: {e}")
                return None
    
    @retry_on_db_lock()
    def update_last_active(self, user_id: int) -> bool:
        """Обновляет время последней активности пользователя"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute(
                    "UPDATE user_profiles SET last_active = CURRENT_TIMESTAMP WHERE user_id = ?",
                    (user_id,)
                )
                self.db.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка обновления активности: {e}")
                return False
    
    @retry_on_db_lock()
    def get_inactive_profiles(self, days: int = 30) -> List[Dict[str, Any]]:
        """Получает профили, неактивные более N дней"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    SELECT * FROM user_profiles 
                    WHERE is_active = 1 
                    AND last_active < datetime('now', ?)
                    ORDER BY last_active ASC
                ''', (f'-{days} days',))
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения неактивных профилей: {e}")
                return []
    
    @retry_on_db_lock()
    def archive_profile(self, user_id: int) -> bool:
        """Архивирует профиль (помечает как неактивный)"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    UPDATE user_profiles 
                    SET is_active = 0, archived_at = CURRENT_TIMESTAMP 
                    WHERE user_id = ?
                ''', (user_id,))
                self.db.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка архивации профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def restore_profile(self, user_id: int) -> bool:
        """Восстанавливает профиль из архива"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    UPDATE user_profiles 
                    SET is_active = 1, archived_at = NULL, last_active = CURRENT_TIMESTAMP
                    WHERE user_id = ?
                ''', (user_id,))
                self.db.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка восстановления профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def get_archived_profiles(self) -> List[Dict[str, Any]]:
        """Получает все архивированные профили"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    SELECT * FROM user_profiles 
                    WHERE is_active = 0 
                    ORDER BY archived_at DESC
                ''')
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения архивных профилей: {e}")
                return []
    
    @retry_on_db_lock()
    def delete_profile(self, user_id: int) -> bool:
        """Удаляет профиль пользователя"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute("DELETE FROM user_profiles WHERE user_id = ?", (user_id,))
                self.db.conn.commit()
                return self.db.cursor.rowcount > 0
            except Exception as e:
                print(f"❌ Ошибка удаления профиля: {e}")
                return False
    
    @retry_on_db_lock()
    def link_account(self, profile_user_id: int, game_account_id: int) -> bool:
        """Привязывает игровой аккаунт к профилю"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    INSERT OR IGNORE INTO user_account_links (profile_user_id, game_account_id)
                    VALUES (?, ?)
                ''', (profile_user_id, game_account_id))
                self.db.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка привязки аккаунта: {e}")
                return False
    
    @retry_on_db_lock()
    def unlink_account(self, profile_user_id: int, game_account_id: int) -> bool:
        """Отвязывает игровой аккаунт от профиля"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    DELETE FROM user_account_links 
                    WHERE profile_user_id = ? AND game_account_id = ?
                ''', (profile_user_id, game_account_id))
                self.db.conn.commit()
                return self.db.cursor.rowcount > 0
            except Exception as e:
                print(f"❌ Ошибка отвязки аккаунта: {e}")
                return False
    
    @retry_on_db_lock()
    def get_linked_accounts(self, profile_user_id: int) -> List[Dict[str, Any]]:
        """Получает все привязанные аккаунты для профиля"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    SELECT u.*, l.linked_at
                    FROM users u
                    INNER JOIN user_account_links l ON u.id = l.game_account_id
                    WHERE l.profile_user_id = ?
                    ORDER BY u.updated_at DESC
                ''', (profile_user_id,))
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения привязанных аккаунтов: {e}")
                return []
    
    @retry_on_db_lock()
    def get_profile_by_account(self, game_account_id: int) -> Optional[Dict[str, Any]]:
        """Получает профиль по ID игрового аккаунта"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    SELECT p.* 
                    FROM user_profiles p
                    INNER JOIN user_account_links l ON p.user_id = l.profile_user_id
                    WHERE l.game_account_id = ?
                ''', (game_account_id,))
                row = self.db.cursor.fetchone()
                return dict(row) if row else None
            except Exception as e:
                print(f"❌ Ошибка получения профиля по аккаунту: {e}")
                return None
    
    @retry_on_db_lock()
    def get_profiles_with_birthday_in_days(self, days: int) -> List[Dict[str, Any]]:
        """Получает профили, у которых ДР будет через N дней"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    SELECT * FROM user_profiles 
                    WHERE is_active = 1
                    AND birth_day IS NOT NULL 
                    AND birth_month IS NOT NULL
                ''')
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения профилей с ДР: {e}")
                return []
    
    @retry_on_db_lock()
    def add_birthday_template(self, template_text: str, is_default: bool = False) -> Optional[int]:
        """Добавляет шаблон поздравления"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                if is_default:
                    self.db._execute("UPDATE birthday_templates SET is_default = 0")
                
                self.db._execute('''
                    INSERT INTO birthday_templates (template_text, is_default)
                    VALUES (?, ?)
                ''', (template_text, is_default))
                self.db.conn.commit()
                return self.db.cursor.lastrowid
            except Exception as e:
                print(f"❌ Ошибка добавления шаблона: {e}")
                return None
    
    @retry_on_db_lock()
    def get_birthday_templates(self, only_default: bool = False) -> List[Dict[str, Any]]:
        """Получает шаблоны поздравлений"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                if only_default:
                    self.db._execute("SELECT * FROM birthday_templates WHERE is_default = 1")
                else:
                    self.db._execute("SELECT * FROM birthday_templates ORDER BY is_default DESC, id DESC")
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения шаблонов: {e}")
                return []
    
    @retry_on_db_lock()
    def save_birthday_settings(self, responsible_user_id: int, group_chat_id: int = None, 
                                 notification_3day: bool = True, notification_1day: bool = True,
                                notification_day: bool = True, use_gpt: bool = False) -> bool:
        """Сохраняет настройки уведомлений о ДР"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute('''
                    INSERT OR REPLACE INTO birthday_settings 
                    (id, responsible_user_id, group_chat_id, notification_3day, 
                     notification_1day, notification_day, use_gpt, updated_at)
                    VALUES (1, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (responsible_user_id, group_chat_id, notification_3day, 
                      notification_1day, notification_day, use_gpt))
                self.db.conn.commit()
                return True
            except Exception as e:
                print(f"❌ Ошибка сохранения настроек: {e}")
                return False
    
    @retry_on_db_lock()
    def get_birthday_settings(self) -> Optional[Dict[str, Any]]:
        """Получает настройки уведомлений о ДР"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute("SELECT * FROM birthday_settings WHERE id = 1")
                row = self.db.cursor.fetchone()
                return dict(row) if row else None
            except Exception as e:
                print(f"❌ Ошибка получения настроек: {e}")
                return None
    
    def get_stats(self) -> Dict[str, Any]:
        """Статистика по профилям"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                self.db._execute("SELECT COUNT(*) FROM user_profiles")
                total = self.db.cursor.fetchone()[0]
                
                self.db._execute("SELECT COUNT(*) FROM user_profiles WHERE is_active = 1")
                active = self.db.cursor.fetchone()[0]
                
                self.db._execute("SELECT COUNT(*) FROM user_profiles WHERE is_active = 0")
                archived = self.db.cursor.fetchone()[0]
                
                self.db._execute("SELECT COUNT(*) FROM user_profiles WHERE city IS NOT NULL AND is_active = 1")
                with_city = self.db.cursor.fetchone()[0]
                
                self.db._execute("SELECT COUNT(*) FROM user_account_links")
                linked_accounts = self.db.cursor.fetchone()[0]
                
                return {
                    'total_profiles': total,
                    'active_profiles': active,
                    'archived_profiles': archived,
                    'with_city': with_city,
                    'linked_accounts': linked_accounts,
                    'percent_with_city': round(with_city / active * 100, 1) if active else 0,
                    'percent_active': round(active / total * 100, 1) if total else 0
                }
            except Exception as e:
                print(f"❌ Ошибка получения статистики профилей: {e}")
                return {
                    'total_profiles': 0, 
                    'active_profiles': 0,
                    'archived_profiles': 0,
                    'with_city': 0, 
                    'linked_accounts': 0,
                    'percent_with_city': 0,
                    'percent_active': 0
                }
    
    @retry_on_db_lock()
    def get_all_profiles(self, include_inactive: bool = False) -> List[Dict[str, Any]]:
        """Получает все профили (для админов)"""
        if not self.db:
            raise ValueError("Database object not provided")
        
        with self.lock:
            try:
                if include_inactive:
                    self.db._execute("SELECT * FROM user_profiles ORDER BY updated_at DESC")
                else:
                    self.db._execute("SELECT * FROM user_profiles WHERE is_active = 1 ORDER BY updated_at DESC")
                return [dict(row) for row in self.db.cursor.fetchall()]
            except Exception as e:
                print(f"❌ Ошибка получения всех профилей: {e}")
                return []
    
    def init_default_data(self):
        """Инициализирует дефолтные шаблоны и настройки"""
        with self.lock:
            try:
                self.db._execute("SELECT COUNT(*) FROM birthday_templates")
                count = self.db.cursor.fetchone()[0]
                
                if count == 0:
                    default_templates = [
                        "🎉 {name}, с днём рождения! Желаем здоровья, счастья и побед! 🏆",
                        "🥳 {name}, поздравляем! Пусть всё получается, а удача всегда будет на твоей стороне! 🍀",
                        "🎂 {name}, с днём рождения! Новых достижений и ярких побед! ⚡️"
                    ]
                    
                    for template in default_templates:
                        self.db._execute(
                            "INSERT INTO birthday_templates (template_text, is_default) VALUES (?, 1)",
                            (template,)
                        )
                    self.db.conn.commit()
                    print("✅ Добавлены дефолтные шаблоны поздравлений")
                
                self.db._execute("SELECT COUNT(*) FROM birthday_settings")
                count = self.db.cursor.fetchone()[0]
                
                if count == 0:
                    self.db._execute("""
                        INSERT INTO birthday_settings 
                        (id, responsible_user_id, group_chat_id, notification_3day, notification_1day, notification_day, use_gpt)
                        VALUES (1, NULL, NULL, 1, 1, 1, 0)
                    """)
                    self.db.conn.commit()
                    print("✅ Добавлены дефолтные настройки уведомлений")
                    
            except Exception as e:
                print(f"⚠️ Ошибка инициализации дефолтных данных: {e}")
