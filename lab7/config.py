# -*- coding: utf-8 -*-
"""Конфигурация дашборда сквозной аналитики"""

import os

# Telegram
TG_CHANNEL = 'terra_danza'

# Яндекс.Метрика
YANDEX_TOKEN = 'токен_Яндекс.Метрики'
YANDEX_COUNTER_ID = '108240878'

# База данных
DB_PATH = 'analytics.db'

# Настройки
DEFAULT_DAYS = 30
CACHE_DIR = 'cache'

# UTM-параметры для демо
UTM_SOURCES = {
    'telegram': 'Telegram-канал',
    'vk': 'VK-сообщество',
    'direct': 'Прямой переход'
}

os.makedirs(CACHE_DIR, exist_ok=True)
