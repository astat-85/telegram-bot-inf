"""
Определение пола по имени через pymorphy2
"""
import pymorphy2
import inspect
import functools

# ПАТЧ ДЛЯ СОВМЕСТИМОСТИ С PYTHON 3.11
if not hasattr(inspect, 'getargspec'):
    def getargspec_patch(func):
        """
        Замена устаревшему inspect.getargspec
        для совместимости с Python 3.11+
        """
        spec = inspect.getfullargspec(func)
        # Возвращаем объект, похожий на старый ArgSpec
        return spec  # Просто возвращаем fullargspec, он работает так же

    inspect.getargspec = getargspec_patch

# Теперь создаем анализатор
try:
    morph = pymorphy2.MorphAnalyzer(lang='ru')
    print("✅ pymorphy2 успешно загружен")
except Exception as e:
    print(f"⚠️ Ошибка загрузки pymorphy2: {e}")
    # Заглушка на случай ошибки
    class DummyMorph:
        def parse(self, word):
            class DummyParse:
                tag = []
                def __init__(self):
                    self.tag = []
            return [DummyParse()]
    morph = DummyMorph()

def detect_gender_by_name(name: str) -> str | None:
    """
    Определяет пол по имени
    Возвращает 'male', 'female' или None, если не удалось определить
    """
    if not name or len(name) < 2:
        return None
    
    try:
        parsed = morph.parse(name)[0]
        
        # Проверяем, что это имя
        if hasattr(parsed, 'tag') and 'Name' not in str(parsed.tag):
            return None
        
        if 'masc' in str(parsed.tag):
            return 'male'
        elif 'femn' in str(parsed.tag):
            return 'female'
    except Exception as e:
        print(f"⚠️ Ошибка определения пола: {e}")
        # Запасной вариант - определяем по окончанию
        if name.endswith(('а', 'я')):
            return 'female'
        else:
            return 'male'
    
    return None
