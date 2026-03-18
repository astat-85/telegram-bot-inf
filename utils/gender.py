"""
Определение пола по имени из JSON-файла
"""
import json
import os
from pathlib import Path
import pymorphy2
import inspect

# ПАТЧ ДЛЯ СОВМЕСТИМОСТИ С PYTHON 3.11
if not hasattr(inspect, 'getargspec'):
    def getargspec_patch(func):
        spec = inspect.getfullargspec(func)
        return spec
    inspect.getargspec = getargspec_patch

# Загружаем имена из JSON
def load_names():
    json_path = Path(__file__).parent.parent / "data" / "russian_names.json"
    if json_path.exists():
        try:
            with open(json_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                male_names = [name.lower() for name in data.get('male', [])]
                female_names = [name.lower() for name in data.get('female', [])]
                print(f"✅ Загружено {len(male_names)} мужских и {len(female_names)} женских имён из JSON")
                return male_names, female_names
        except Exception as e:
            print(f"⚠️ Ошибка загрузки JSON: {e}")
    
    # Если файла нет - возвращаем базовый набор
    print("⚠️ Файл с именами не найден, использую базовый словарь")
    return (
        ['александр', 'алексей', 'андрей', 'антон', 'дмитрий', 'евгений', 'юрий', 'юра'],
        ['анна', 'елена', 'ольга', 'татьяна', 'юлия', 'мария']
    )

MALE_NAMES, FEMALE_NAMES = load_names()

# Pymorphy2 как запасной вариант
try:
    morph = pymorphy2.MorphAnalyzer(lang='ru')
    PYMORPHY_AVAILABLE = True
except Exception as e:
    print(f"⚠️ Ошибка загрузки pymorphy2: {e}")
    PYMORPHY_AVAILABLE = False
    morph = None

def detect_gender_by_name(name: str) -> str | None:
    """
    Определяет пол по имени
    Возвращает 'male', 'female' или None
    """
    if not name or len(name) < 2:
        return None
    
    name_lower = name.lower().strip()
    
    # 1️⃣ Проверяем по загруженному словарю
    if name_lower in MALE_NAMES:
        print(f"✅ Словарь: {name} -> мужской")
        return 'male'
    if name_lower in FEMALE_NAMES:
        print(f"✅ Словарь: {name} -> женский")
        return 'female'
    
    # 2️⃣ Запасной вариант: pymorphy2
    if PYMORPHY_AVAILABLE and morph:
        try:
            parsed = morph.parse(name_lower)[0]
            if 'masc' in parsed.tag:
                print(f"✅ pymorphy2: {name} -> мужской")
                return 'male'
            elif 'femn' in parsed.tag:
                print(f"✅ pymorphy2: {name} -> женский")
                return 'female'
        except:
            pass
    
    # 3️⃣ Самый запасной вариант - по окончанию
    if name_lower.endswith(('а', 'я')):
        print(f"⚠️ По окончанию: {name} -> женский")
        return 'female'
    else:
        print(f"⚠️ По окончанию (умолчание): {name} -> мужской")
        return 'male'
