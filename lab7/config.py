# -*- coding: utf-8 -*-
"""Конфигурация дашборда сквозной аналитики"""

import os

# Telegram
TG_CHANNEL = 'terra_danza'

# Яндекс.Метрика
YANDEX_TOKEN = 'y0__xCB2N-8BhiEuj8g0Y-h7xb8ph-77dNTlolmOlJ9Pjlae_ayLg'
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
    'instagram': 'Instagram',
    'direct': 'Прямой переход'
}

os.makedirs(CACHE_DIR, exist_ok=True)
