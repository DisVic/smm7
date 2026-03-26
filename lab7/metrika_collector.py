# -*- coding: utf-8 -*-
"""Сбор данных из Яндекс.Метрики через API"""

import requests
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import time

from config import YANDEX_TOKEN, YANDEX_COUNTER_ID


class YandexMetrikaAPI:
    """Клиент для работы с API Яндекс.Метрики"""
    
    BASE_URL = 'https://api-metrika.yandex.net/management/v1'
    STATS_URL = 'https://api-metrika.yandex.net/stat/v1/data'
    
    def __init__(self, token: str, counter_id: str):
        self.token = token
        self.counter_id = counter_id
        self.headers = {
            'Authorization': f'OAuth {token}',
            'Content-Type': 'application/x-yametrika+json'
        }
    
    def _request(self, url: str, params: Dict = None) -> Optional[Dict]:
        """Выполнение запроса к API"""
        try:
            response = requests.get(url, headers=self.headers, params=params, timeout=30)
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 403:
                print(f"Ошибка доступа к Яндекс.Метрике. Проверьте токен и права.")
                return None
            elif response.status_code == 404:
                print(f"Счётчик {self.counter_id} не найден.")
                return None
            else:
                print(f"Ошибка API Метрики: {response.status_code}")
                return None
        except Exception as e:
            print(f"Ошибка запроса к Метрике: {e}")
            return None
    
    def get_counter_info(self) -> Optional[Dict]:
        """Получение информации о счётчике"""
        url = f"{self.BASE_URL}/counter/{self.counter_id}"
        data = self._request(url)
        
        if data and 'counter' in data:
            counter = data['counter']
            return {
                'id': counter.get('id'),
                'name': counter.get('name'),
                'site': counter.get('site'),
                'status': counter.get('status'),
                'created': counter.get('create_time')
            }
        return None
    
    def get_traffic_by_source(self, date_from: str, date_to: str) -> List[Dict]:
        """Получение трафика по источникам"""
        params = {
            'ids': self.counter_id,
            'metrics': 'ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate,ym:s:avgVisitDurationSeconds',
            'dimensions': 'ym:s:lastTrafficSource',
            'date1': date_from,
            'date2': date_to,
            'limit': 100
        }
        
        data = self._request(self.STATS_URL, params)
        
        if not data or 'data' not in data:
            return []
        
        results = []
        for item in data['data']:
            dimensions = item.get('dimensions', [])
            metrics = item.get('metrics', [])
            
            if dimensions and metrics:
                results.append({
                    'source': dimensions[0].get('name', 'Unknown'),
                    'users': int(metrics[0]) if metrics[0] else 0,
                    'visits': int(metrics[1]) if metrics[1] else 0,
                    'pageviews': int(metrics[2]) if metrics[2] else 0,
                    'bounce_rate': round(metrics[3], 2) if metrics[3] else 0,
                    'avg_duration': round(metrics[4], 2) if metrics[4] else 0
                })
        
        return results
    
    def get_traffic_by_utm(self, date_from: str, date_to: str) -> List[Dict]:
        """Получение трафика по UTM-меткам"""
        params = {
            'ids': self.counter_id,
            'metrics': 'ym:s:users,ym:s:visits,ym:s:pageviews',
            'dimensions': 'ym:s:lastUTMSource,ym:s:lastUTMMedium,ym:s:lastUTMCampaign',
            'date1': date_from,
            'date2': date_to,
            'limit': 100
        }
        
        data = self._request(self.STATS_URL, params)
        
        if not data or 'data' not in data:
            return []
        
        results = []
        for item in data['data']:
            dimensions = item.get('dimensions', [])
            metrics = item.get('metrics', [])
            
            if dimensions and metrics:
                results.append({
                    'utm_source': dimensions[0].get('name', ''),
                    'utm_medium': dimensions[1].get('name', ''),
                    'utm_campaign': dimensions[2].get('name', ''),
                    'users': int(metrics[0]) if metrics[0] else 0,
                    'visits': int(metrics[1]) if metrics[1] else 0,
                    'pageviews': int(metrics[2]) if metrics[2] else 0
                })
        
        return results
    
    def get_goals(self) -> List[Dict]:
        """Получение списка целей"""
        url = f"{self.BASE_URL}/counter/{self.counter_id}/goals"
        data = self._request(url)
        
        if not data or 'goals' not in data:
            return []
        
        return [{'id': g['id'], 'name': g['name'], 'type': g.get('type', '')} for g in data['goals']]
    
    def get_conversions(self, date_from: str, date_to: str) -> List[Dict]:
        """Получение конверсий по источникам"""
        # Сначала получаем список целей
        goals = self.get_goals()
        
        if not goals:
            return []
        
        # Агрегируем конверсии по источникам для всех целей
        source_conversions = {}
        
        for goal in goals:
            goal_id = goal['id']
            params = {
                'ids': self.counter_id,
                'metrics': f'ym:s:users,ym:s:visits,ym:s:goal{goal_id}reaches',
                'dimensions': 'ym:s:lastTrafficSource',
                'date1': date_from,
                'date2': date_to,
                'limit': 100
            }
            
            data = self._request(self.STATS_URL, params)
            
            if not data or 'data' not in data:
                continue
            
            for item in data['data']:
                dimensions = item.get('dimensions', [])
                metrics = item.get('metrics', [])
                
                if dimensions and metrics:
                    source = dimensions[0].get('name', 'Unknown')
                    users = int(metrics[0]) if metrics[0] else 0
                    conversions = int(metrics[2]) if len(metrics) > 2 and metrics[2] else 0
                    
                    if source not in source_conversions:
                        source_conversions[source] = {
                            'source': source,
                            'users': users,
                            'visits': int(metrics[1]) if metrics[1] else 0,
                            'conversions': 0
                        }
                    if conversions > 0:
                        source_conversions[source]['conversions'] += conversions
        
        # Вычисляем conversion_rate
        results = []
        for source_data in source_conversions.values():
            users = source_data['users']
            conversions = source_data['conversions']
            source_data['conversion_rate'] = round(conversions / users * 100, 2) if users > 0 else 0
            results.append(source_data)
        
        return results
    
    def get_daily_stats(self, date_from: str, date_to: str) -> List[Dict]:
        """Получение статистики по дням"""
        params = {
            'ids': self.counter_id,
            'metrics': 'ym:s:users,ym:s:visits,ym:s:pageviews,ym:s:bounceRate',
            'dimensions': 'ym:s:date',
            'date1': date_from,
            'date2': date_to,
            'limit': 1000
        }
        
        data = self._request(self.STATS_URL, params)
        
        if not data or 'data' not in data:
            return []
        
        results = []
        for item in data['data']:
            dimensions = item.get('dimensions', [])
            metrics = item.get('metrics', [])
            
            if dimensions and metrics:
                results.append({
                    'date': dimensions[0].get('name', ''),
                    'users': int(metrics[0]) if metrics[0] else 0,
                    'visits': int(metrics[1]) if metrics[1] else 0,
                    'pageviews': int(metrics[2]) if metrics[2] else 0,
                    'bounce_rate': round(metrics[3], 2) if metrics[3] else 0
                })
        
        return results


def get_metrika_data(days_back: int = 30) -> Dict:
    """Получение всех данных из Метрики за период"""
    api = YandexMetrikaAPI(YANDEX_TOKEN, YANDEX_COUNTER_ID)
    
    date_to = datetime.now().strftime('%Y-%m-%d')
    date_from = (datetime.now() - timedelta(days=days_back)).strftime('%Y-%m-%d')
    
    result = {
        'counter_info': None,
        'traffic_by_source': [],
        'traffic_by_utm': [],
        'conversions': [],
        'daily_stats': [],
        'goals': []
    }
    
    # Информация о счётчике
    result['counter_info'] = api.get_counter_info()
    
    # Трафик по источникам
    result['traffic_by_source'] = api.get_traffic_by_source(date_from, date_to)
    
    # Трафик по UTM
    result['traffic_by_utm'] = api.get_traffic_by_utm(date_from, date_to)
    
    # Конверсии
    result['conversions'] = api.get_conversions(date_from, date_to)
    
    # Статистика по дням
    result['daily_stats'] = api.get_daily_stats(date_from, date_to)
    
    # Цели
    result['goals'] = api.get_goals()
    
    return result


def get_demo_metrika_data(days_back: int = 30) -> Dict:
    """Генерация демо-данных Метрики (если API недоступен)"""
    import random
    
    date_to = datetime.now()
    dates = [(date_to - timedelta(days=i)).strftime('%Y-%m-%d') for i in range(days_back)]
    
    # Демо трафик по источникам
    sources = [
        {'source': 'Переходы из поисковых систем', 'users': random.randint(50, 200), 'visits': random.randint(60, 250), 'pageviews': random.randint(150, 600), 'bounce_rate': random.uniform(30, 60), 'avg_duration': random.randint(60, 180)},
        {'source': 'Прямые заходы', 'users': random.randint(30, 100), 'visits': random.randint(35, 120), 'pageviews': random.randint(80, 300), 'bounce_rate': random.uniform(25, 50), 'avg_duration': random.randint(90, 200)},
        {'source': 'Внутренние переходы', 'users': random.randint(10, 50), 'visits': random.randint(12, 60), 'pageviews': random.randint(30, 150), 'bounce_rate': random.uniform(20, 40), 'avg_duration': random.randint(120, 250)},
        {'source': 'Переходы по ссылкам на сайтах', 'users': random.randint(20, 80), 'visits': random.randint(25, 100), 'pageviews': random.randint(60, 250), 'bounce_rate': random.uniform(35, 55), 'avg_duration': random.randint(50, 150)},
        {'source': 'Рекламные переходы', 'users': random.randint(5, 30), 'visits': random.randint(6, 35), 'pageviews': random.randint(15, 100), 'bounce_rate': random.uniform(40, 70), 'avg_duration': random.randint(30, 100)},
    ]
    
    # Демо UTM
    utm_data = [
        {'utm_source': 'telegram', 'utm_medium': 'social', 'utm_campaign': 'brand_awareness', 'users': 45, 'visits': 52, 'pageviews': 120},
        {'utm_source': 'vk', 'utm_medium': 'social', 'utm_campaign': 'spring_sale', 'users': 32, 'visits': 38, 'pageviews': 85},
        {'utm_source': 'yandex', 'utm_medium': 'cpc', 'utm_campaign': 'dance_courses', 'users': 28, 'visits': 35, 'pageviews': 95},
    ]
    
    # Демо конверсии
    conversions = [
        {'source': 'Переходы из поисковых систем', 'users': 150, 'visits': 180, 'conversions': 12, 'conversion_rate': 8.0},
        {'source': 'Прямые заходы', 'users': 60, 'visits': 75, 'conversions': 5, 'conversion_rate': 8.3},
        {'source': 'Переходы по ссылкам на сайтах', 'users': 50, 'visits': 62, 'conversions': 8, 'conversion_rate': 16.0},
        {'source': 'Рекламные переходы', 'users': 18, 'visits': 22, 'conversions': 3, 'conversion_rate': 16.7},
    ]
    
    # Демо статистика по дням
    daily = []
    for date in reversed(dates):
        daily.append({
            'date': date,
            'users': random.randint(15, 60),
            'visits': random.randint(20, 80),
            'pageviews': random.randint(50, 200),
            'bounce_rate': round(random.uniform(30, 55), 2)
        })
    
    return {
        'counter_info': {'id': YANDEX_COUNTER_ID, 'name': 'Terra Danza Demo', 'site': 'terradanza.ru', 'status': 'Active'},
        'traffic_by_source': sources,
        'traffic_by_utm': utm_data,
        'conversions': conversions,
        'daily_stats': daily,
        'goals': [{'id': 1, 'name': 'Заявка на пробное занятие', 'type': 'action'}],
        'is_demo': True
    }
