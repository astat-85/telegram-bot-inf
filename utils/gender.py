"""
Определение пола по имени из JSON-файла
"""
import json
from pathlib import Path
from typing import Tuple, List, Optional


def load_names() -> Tuple[List[str], List[str], List[str]]:
    """Загружаем имена из JSON"""
    json_path = Path(__file__).parent.parent / "data" / "russian_names.json"
    
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            male_names = [name.lower().strip() for name in data.get('male', [])]
            female_names = [name.lower().strip() for name in data.get('female', [])]
            
            # Отдельно выделяем unisex-имена (которые могут быть и мужскими и женскими)
            unisex_names = ['саша', 'женя', 'валя', 'паша', 'мишель', 'никита']
            
            print(f"✅ Загружено {len(male_names)} мужских и {len(female_names)} женских имён из JSON")
            return male_names, female_names, unisex_names
        except Exception as e:
            print(f"⚠️ Ошибка загрузки JSON: {e}")
    
    # Если файла нет - возвращаем пустые списки
    print("⚠️ Файл с именами не найден, пол определяться не будет")
    return [], [], ['саша', 'женя', 'валя', 'паша']


# Загружаем имена при импорте модуля
MALE_NAMES, FEMALE_NAMES, UNISEX_NAMES = load_names()


def detect_gender_by_name(name: str) -> Optional[str]:
    """
    Определяет пол по имени из JSON-файла
    Возвращает 'male', 'female' или None (для unisex-имён)
    """
    if not name or len(name) < 2:
        return None
    
    name_lower = name.lower().strip()
    
    # 1️⃣ Проверяем, не является ли имя унисекс
    if name_lower in UNISEX_NAMES:
        print(f"⚠️ Унисекс имя: {name} - требуется ручной выбор")
        return None
    
    # 2️⃣ Проверяем по мужскому словарю
    if name_lower in MALE_NAMES:
        print(f"✅ Словарь: {name} -> мужской")
        return 'male'
    
    # 3️⃣ Проверяем по женскому словарю
    if name_lower in FEMALE_NAMES:
        print(f"✅ Словарь: {name} -> женский")
        return 'female'
    
    # 4️⃣ Если не нашли в словаре - возвращаем None
    print(f"⚠️ Имя '{name}' не найдено в словаре, требуется ручной выбор")
    return None
