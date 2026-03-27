"""
Парсинг даты рождения из текста
"""
import re
from datetime import datetime
from typing import Optional, Tuple


def parse_birthday(text: str) -> Optional[Tuple[int, int, Optional[int]]]:
    """
    Парсит дату рождения из текста
    Возвращает (день, месяц, год) или None
    """
    text = text.strip().replace(' ', '')

    # Паттерн для формата ДДММ, ДДММГГ, ДДММГГГГ
    match = re.match(r'^(\d{2})(\d{2})(\d{2,4})?$', text)
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
