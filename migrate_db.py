#!/usr/bin/env python3
"""
Скрипт миграции базы данных: добавляет столбец acceleration_buff
"""
import sqlite3
import os

DB_PATH = "/app/data/users_data.db"

def migrate_database():
    if not os.path.exists(DB_PATH):
        print(f"❌ База данных не найдена: {DB_PATH}")
        return False

    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    try:
        # Проверяем структуру таблицы users
        cursor.execute("PRAGMA table_info(users)")
        columns = [col[1] for col in cursor.fetchall()]

        if "acceleration_buff" in columns:
            print("✅ Столбец acceleration_buff уже существует")
            return True

        # Добавляем столбец acceleration_buff
        print("➕ Добавление столбца acceleration_buff...")
        cursor.execute("""
            ALTER TABLE users
            ADD COLUMN acceleration_buff TEXT DEFAULT ''
        """)
        conn.commit()
        print("✅ Столбец acceleration_buff успешно добавлен")

        # Проверяем результат
        cursor.execute("PRAGMA table_info(users)")
        new_columns = [col[1] for col in cursor.fetchall()]

        if "acceleration_buff" in new_columns:
            print("✅ Проверка подтверждена: столбец существует")
            return True
        else:
            print("❌ Ошибка: столбец не был добавлен")
            return False

    except sqlite3.Error as e:
        print(f"❌ Ошибка SQLite: {e}")
        return False
    finally:
        conn.close()

if __name__ == "__main__":
    print("🔧 Запуск миграции базы данных...")
    success = migrate_database()
    if success:
        print("🎉 Миграция завершена успешно!")
    else:
        print("💥 Миграция завершилась с ошибкой!")
