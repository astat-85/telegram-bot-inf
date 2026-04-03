import sqlite3
import os

DB_PATH = "/app/data/users_data.db"

# Создаем директорию если нет
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

# Проверяем существует ли таблица users
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='users'")
if not cursor.fetchone():
    print("Таблица users не найдена, создаём новую структуру...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            game_nickname TEXT,
            power TEXT DEFAULT '',
            bm TEXT DEFAULT '',
            pl1 TEXT DEFAULT '',
            pl2 TEXT DEFAULT '',
            pl3 TEXT DEFAULT '',
            dragon TEXT DEFAULT '',
            buffs_stands TEXT DEFAULT '',
            buffs_research TEXT DEFAULT '',
            acceleration_buff TEXT DEFAULT '',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE INDEX IF NOT EXISTS idx_user_id ON users(user_id)
    ''')
else:
    # Проверяем есть ли столбец acceleration_buff
    cursor.execute("PRAGMA table_info(users)")
    columns = [col[1] for col in cursor.fetchall()]

    if 'acceleration_buff' not in columns:
        print("Добавляем столбец acceleration_buff...")
        cursor.execute("ALTER TABLE users ADD COLUMN acceleration_buff TEXT DEFAULT ''")
        print("✅ Столбец acceleration_buff добавлен")
    else:
        print("✅ Столбец acceleration_buff уже существует")

# Проверяем таблицу user_profiles
cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='user_profiles'")
if not cursor.fetchone():
    print("Создаём таблицу user_profiles...")
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_profiles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL UNIQUE,
            first_name TEXT,
            last_name TEXT,
            middle_name TEXT,
            city TEXT,
            region TEXT,
            timezone TEXT,
            birth_day INTEGER,
            birth_month INTEGER,
            birth_year INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        )
    ''')
    print("✅ Таблица user_profiles создана")
else:
    print("✅ Таблица user_profiles уже существует")

conn.commit()
conn.close()
print("✅ Миграция завершена успешно")
