# -*- coding: utf-8 -*-
"""Расчёт агрегированных метрик для сквозной аналитики"""

from typing import Dict, List
from datetime import datetime, date


def calculate_err(total_reactions: int, total_views: int, members: int) -> Dict:
    """
    Расчёт ERR (Engagement Rate by Reach)
    ERR = (реакции / просмотры) * 100%
    """
    if total_views == 0:
        return {'err': 0, 'err_views': 0}
    
    err = (total_reactions / total_views) * 100
    err_views = (total_views / members * 100) if members > 0 else 0
    
    return {
        'err': round(err, 2),
        'err_views': round(err_views, 2)
    }


def calculate_conversion_rate(conversions: int, visitors: int) -> float:
    """
    Расчёт конверсии
    CR = (конверсии / посетители) * 100%
    """
    if visitors == 0:
        return 0.0
    return round((conversions / visitors) * 100, 2)


def calculate_smm_traffic_share(smm_visits: int, total_visits: int) -> float:
    """
    Доля трафика из SMM в общем объёме
    """
    if total_visits == 0:
        return 0.0
    return round((smm_visits / total_visits) * 100, 2)


def calculate_funnel(data: Dict) -> List[Dict]:
    """
    Построение воронки конверсий
    Показы -> Клики -> Сессии -> Конверсии
    """
    funnel = []
    
    # Telegram воронка
    tg_metrics = data.get('tg_metrics', {})
    members = data.get('tg_members', 0)
    
    # Показы (просмотры)
    views = tg_metrics.get('total_views', 0)
    funnel.append({'stage': 'Просмотры (TG)', 'value': views, 'type': 'telegram'})
    
    # Реакции
    reactions = tg_metrics.get('total_reactions', 0)
    funnel.append({'stage': 'Реакции', 'value': reactions, 'type': 'telegram'})
    
    # Метрика воронка
    metrika = data.get('metrika', {})
    traffic = metrika.get('traffic_by_source', [])
    
    # Всего визитов
    total_visits = sum(t.get('visits', 0) for t in traffic)
    
    # Визиты из соцсетей
    social_sources = ['Переходы по ссылкам на сайтах', 'Рекламные переходы']
    smm_visits = sum(t.get('visits', 0) for t in traffic if any(s in t.get('source', '') for s in ['ссыл', 'соц', 'social']))
    
    funnel.append({'stage': 'Визиты на сайт', 'value': total_visits, 'type': 'metrika'})
    funnel.append({'stage': 'Из соцсетей', 'value': smm_visits, 'type': 'metrika'})
    
    # Конверсии
    conversions = metrika.get('conversions', [])
    total_conversions = sum(c.get('conversions', 0) for c in conversions)
    funnel.append({'stage': 'Конверсии', 'value': total_conversions, 'type': 'conversion'})
    
    return funnel


def aggregate_metrics(tg_data: Dict, metrika_data: Dict) -> Dict:
    """
    Агрегация всех метрик в единый отчёт
    """
    result = {
        'telegram': {},
        'metrika': {},
        'cross_channel': {},
        'funnel': []
    }
    
    # Telegram метрики
    if tg_data:
        info = tg_data.get('info', {})
        metrics = tg_data.get('metrics', {})
        
        result['telegram'] = {
            'channel_name': info.get('name', ''),
            'username': info.get('username', ''),
            'members': info.get('members', 0),
            'total_posts': metrics.get('total_posts', 0),
            'total_views': metrics.get('total_views', 0),
            'total_reactions': metrics.get('total_reactions', 0),
            'avg_views': metrics.get('avg_views', 0),
            'avg_reactions': metrics.get('avg_reactions', 0),
            'er': metrics.get('er', 0),
            'err': metrics.get('err', 0),
            'posts_with_links': metrics.get('posts_with_links', 0),
            'link_rate': metrics.get('link_rate', 0)
        }
    
    # Метрика метрики
    if metrika_data:
        traffic = metrika_data.get('traffic_by_source', [])
        utm = metrika_data.get('traffic_by_utm', [])
        conversions = metrika_data.get('conversions', [])
        daily = metrika_data.get('daily_stats', [])
        
        total_users = sum(t.get('users', 0) for t in traffic)
        total_visits = sum(t.get('visits', 0) for t in traffic)
        total_pageviews = sum(t.get('pageviews', 0) for t in traffic)
        total_conversions = sum(c.get('conversions', 0) for c in conversions)
        
        # Трафик из Telegram
        tg_traffic = [u for u in utm if u.get('utm_source', '').lower() == 'telegram']
        tg_users = sum(u.get('users', 0) for u in tg_traffic)
        tg_visits = sum(u.get('visits', 0) for u in tg_traffic)
        
        # Трафик из соцсетей
        social_traffic = [u for u in utm if u.get('utm_medium', '').lower() in ['social', 'cpc', 'referral']]
        social_users = sum(u.get('users', 0) for u in social_traffic)
        
        result['metrika'] = {
            'total_users': total_users,
            'total_visits': total_visits,
            'total_pageviews': total_pageviews,
            'total_conversions': total_conversions,
            'avg_bounce_rate': round(sum(t.get('bounce_rate', 0) for t in traffic) / len(traffic), 2) if traffic else 0,
            'traffic_by_source': traffic,
            'traffic_by_utm': utm,
            'conversions': conversions,
            'daily_stats': daily,
            'is_demo': metrika_data.get('is_demo', False)
        }
        
        result['cross_channel'] = {
            'tg_users_on_site': tg_users,
            'tg_visits_on_site': tg_visits,
            'social_users_on_site': social_users,
            'smm_traffic_share': calculate_smm_traffic_share(social_users, total_users),
            'conversion_rate': calculate_conversion_rate(total_conversions, total_users)
        }
    
    # Воронка
    result['funnel'] = calculate_funnel({
        'tg_metrics': result.get('telegram', {}),
        'tg_members': result.get('telegram', {}).get('members', 0),
        'metrika': result.get('metrika', {})
    })
    
    return result


def get_top_posts(posts: List[Dict], metric: str = 'views', limit: int = 5) -> List[Dict]:
    """
    Получение топ постов по метрике
    """
    if not posts:
        return []
    
    sorted_posts = sorted(posts, key=lambda x: x.get(metric, 0), reverse=True)
    
    top = []
    for post in sorted_posts[:limit]:
        # Обработка даты (может быть datetime, date или строкой)
        date_val = post.get('date')
        if isinstance(date_val, datetime):
            date_str = date_val.strftime('%Y-%m-%d')
        elif isinstance(date_val, date):
            date_str = date_val.isoformat()
        elif date_val:
            date_str = str(date_val)[:10]
        else:
            date_str = ''
        
        top.append({
            'post_id': post.get('post_id', 0),
            'date': date_str,
            'text': post.get('text', '')[:100] + '...' if len(post.get('text', '')) > 100 else post.get('text', ''),
            'views': post.get('views', 0),
            'reactions': post.get('reactions', 0),
            'er': round(post.get('reactions', 0) / post.get('views', 1) * 100, 2) if post.get('views', 0) > 0 else 0
        })
    
    return top


def compare_periods(current: Dict, previous: Dict) -> Dict:
    """
    Сравнение метрик между периодами
    """
    def calc_change(curr_val, prev_val):
        if prev_val == 0:
            return 0 if curr_val == 0 else 100
        return round((curr_val - prev_val) / prev_val * 100, 1)
    
    comparison = {}
    
    # Telegram сравнение
    if current.get('telegram') and previous.get('telegram'):
        curr_tg = current['telegram']
        prev_tg = previous['telegram']
        
        comparison['telegram'] = {
            'members_change': calc_change(curr_tg.get('members', 0), prev_tg.get('members', 0)),
            'views_change': calc_change(curr_tg.get('total_views', 0), prev_tg.get('total_views', 0)),
            'er_change': calc_change(curr_tg.get('er', 0), prev_tg.get('er', 0))
        }
    
    # Метрика сравнение
    if current.get('metrika') and previous.get('metrika'):
        curr_m = current['metrika']
        prev_m = previous['metrika']
        
        comparison['metrika'] = {
            'users_change': calc_change(curr_m.get('total_users', 0), prev_m.get('total_users', 0)),
            'visits_change': calc_change(curr_m.get('total_visits', 0), prev_m.get('total_visits', 0)),
            'conversions_change': calc_change(curr_m.get('total_conversions', 0), prev_m.get('total_conversions', 0))
        }
    
    return comparison
