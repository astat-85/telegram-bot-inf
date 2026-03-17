"""
Определение пола по имени через names-dataset и pymorphy2
"""
import pymorphy2
import inspect
from names_dataset import NameDataset

# ПАТЧ ДЛЯ СОВМЕСТИМОСТИ С PYTHON 3.11
if not hasattr(inspect, 'getargspec'):
    def getargspec_patch(func):
        spec = inspect.getfullargspec(func)
        return spec
    inspect.getargspec = getargspec_patch

# Загружаем базу имён (первый раз скачается ~50мб)
try:
    nd = NameDataset()
    NAMES_DB_AVAILABLE = True
    print("✅ База имён загружена")
except Exception as e:
    print(f"⚠️ Ошибка загрузки базы имён: {e}")
    NAMES_DB_AVAILABLE = False
    nd = None

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
    
    name_clean = name.strip().capitalize()
    
    # 1️⃣ Сначала проверяем по базе имён
    if NAMES_DB_AVAILABLE and nd:
        try:
            # В новой версии names-dataset используется другой метод
            result = nd.search(name_clean)
            if result and 'first_name' in result:
                gender = result['first_name'].get('gender')
                if gender == 'male':
                    print(f"✅ База имён: {name} -> мужской")
                    return 'male'
                elif gender == 'female':
                    print(f"✅ База имён: {name} -> женский")
                    return 'female'
        except Exception as e:
            print(f"⚠️ Ошибка поиска в базе имён: {e}")
    
    # 2️⃣ Запасной вариант: pymorphy2
    if PYMORPHY_AVAILABLE and morph:
        try:
            parsed = morph.parse(name.lower())[0]
            if 'masc' in parsed.tag:
                print(f"✅ pymorphy2: {name} -> мужской")
                return 'male'
            elif 'femn' in parsed.tag:
                print(f"✅ pymorphy2: {name} -> женский")
                return 'female'
        except:
            pass
    
    # 3️⃣ Самый запасной вариант - по окончанию
    if name.lower().endswith(('а', 'я')):
        print(f"⚠️ По окончанию: {name} -> женский")
        return 'female'
    else:
        print(f"⚠️ По окончанию (умолчание): {name} -> мужской")
        return 'male'
