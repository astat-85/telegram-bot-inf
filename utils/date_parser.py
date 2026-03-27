"""
Парсинг даты рождения из текста
"""
import re
from datetime import datetime
from typing import Optional, Tuple


def parse_birthday(text: str) -> Optional[Tuple[int, int, Optional[int]]]:
    """
    Парсит дату рождения из текста
    Поддерживает форматы:
    - ДДММ (например 1503)
    - ДДММГГ (например 150390)
    - ДДММГГГГ (например 15031990)
    - ДД.ММ
    - ДД.ММ.ГГГГ
    - ДД.ММ.ГГ
    - ДД месяц
    - ДД месяц ГГГГ
    - ДД месяц ГГ
    Возвращает (день, месяц, год) или None
    Год может быть None, если не указан
    """
    text = text.strip().replace(' ', '')

    # Паттерн для формата ДДММ, ДДММГГ, ДДММГГГГ
    match = re.match(r'^(\d{2})(\d{2})(\d{2,4})?$', text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_str = match.group(3)
        
        # Валидация дня и месяца
        if not (1 <= day <= 31) or not (1 <= month <= 12):
            return None
        
        if year_str:
            if len(year_str) == 2:
                # ДДММГГ - определяем век
                year = int(year_str)
                current_year = datetime.now().year
                current_century = current_year // 100
                
                # Если год больше текущего двухзначного года - это прошлый век
                if year > current_year % 100:
                    year = (current_century - 1) * 100 + year
                else:
                    year = current_century * 100 + year
            else:
                year = int(year_str)
            
            # Проверка на разумный возраст (10-90 лет)
            age = datetime.now().year - year
            if age < 10 or age > 90:
                return None
        else:
            year = None
        
        return (day, month, year)

    # Формат с точками ДД.ММ или ДД.ММ.ГГГГ
    match = re.match(r'^(\d{1,2})[./](\d{1,2})(?:[./](\d{2,4}))?$', text)
    if match:
        day = int(match.group(1))
        month = int(match.group(2))
        year_str = match.group(3)
        
        if not (1 <= day <= 31) or not (1 <= month <= 12):
            return None
        
        if year_str:
            if len(year_str) == 2:
                year = int(year_str)
                current_year = datetime.now().year
                current_century = current_year // 100
                if year > current_year % 100:
                    year = (current_century - 1) * 100 + year
                else:
                    year = current_century * 100 + year
            else:
                year = int(year_str)
            
            age = datetime.now().year - year
            if age < 10 or age > 90:
                return None
        else:
            year = None
        
        return (day, month, year)

    return None
