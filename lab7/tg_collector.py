# -*- coding: utf-8 -*-
"""Сбор данных из Telegram через веб-скрапинг"""

import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
from typing import Dict, List, Optional

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0'
}


def parse_number(s: str) -> int:
    """Парсинг числа с K/M суффиксами"""
    s = s.strip().replace(' ', '').replace(',', '')
    if not s:
        return 0
    try:
        if 'K' in s or 'k' in s:
            return int(float(s.replace('K', '').replace('k', '')) * 1000)
        elif 'М' in s or 'M' in s:
            return int(float(s.replace('М', '').replace('M', '')) * 1000000)
        else:
            return int(s)
    except:
        return 0


def get_tg_channel_info(username: str) -> Optional[Dict]:
    """Получение информации о канале"""
    url = f"https://t.me/{username}"
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, 'html.parser')
        
        title = soup.find('div', class_='tgme_page_title')
        name = title.get_text(strip=True) if title else username
        
        description = ''
        desc_elem = soup.find('div', class_='tgme_page_description')
        if desc_elem:
            description = desc_elem.get_text(strip=True)
        
        members = 0
        extra = soup.find('div', class_='tgme_page_extra')
        if extra:
            text = extra.get_text()
            match = re.search(r'(\d[\d\s,KkМм]*)\s*(?:members|подписчик|subscribers)', text, re.I)
            if match:
                members = parse_number(match.group(1))
        
        return {
            'username': username,
            'name': name,
            'description': description,
            'members': members
        }
        
    except Exception as e:
        print(f"Ошибка получения информации о канале {username}: {e}")
        return None


def get_tg_posts(username: str, limit: int = 100) -> List[Dict]:
    """Получение списка постов с метриками"""
    url = f"https://t.me/s/{username}"
    
    posts = []
    
    try:
        r = requests.get(url, headers=HEADERS, timeout=15)
        r.encoding = 'utf-8'
        soup = BeautifulSoup(r.text, 'html.parser')
        
        post_elements = soup.find_all('div', class_='tgme_widget_message')
        
        for post in post_elements[:limit]:
            post_data = {}
            
            # ID поста
            link = post.find('a', class_='tgme_widget_message_date')
            if link:
                href = link.get('href', '')
                match = re.search(r'/(\d+)$', href)
                if match:
                    post_data['post_id'] = int(match.group(1))
            
            # Дата
            time_elem = post.find('time')
            if time_elem:
                datetime_attr = time_elem.get('datetime', '')
                if datetime_attr:
                    try:
                        post_data['date'] = datetime.fromisoformat(datetime_attr.replace('Z', '+00:00'))
                    except:
                        post_data['date'] = None
            
            # Текст
            text_elem = post.find('div', class_='tgme_widget_message_text')
            if text_elem:
                post_data['text'] = text_elem.get_text(strip=True)[:500]
            else:
                post_data['text'] = ''
            
            # Просмотры
            views_elem = post.find('span', class_='tgme_widget_message_views')
            if views_elem:
                post_data['views'] = parse_number(views_elem.get_text())
            else:
                post_data['views'] = 0
            
            # Реакции
            reactions = 0
            reaction_spans = post.find_all('span', class_='tgme_reaction')
            for span in reaction_spans:
                text = span.get_text()
                nums = re.findall(r'\d+', text)
                if nums:
                    reactions += int(nums[-1])
            post_data['reactions'] = reactions
            
            # Есть ли ссылка в посте
            links = post.find_all('a')
            post_data['has_link'] = len([a for a in links if 't.me' not in a.get('href', '')]) > 0
            
            if post_data.get('post_id'):
                posts.append(post_data)
        
        return posts
        
    except Exception as e:
        print(f"Ошибка получения постов {username}: {e}")
        return []


def calculate_tg_metrics(posts: List[Dict], members: int, days_back: int = 30) -> Dict:
    """Расчёт метрик канала"""
    if not posts or members == 0:
        return {
            'total_posts': 0,
            'total_views': 0,
            'total_reactions': 0,
            'avg_views': 0,
            'avg_reactions': 0,
            'er': 0,
            'err': 0,
            'posts_with_links': 0,
            'link_rate': 0
        }
    
    cutoff = datetime.now(tz=posts[0]['date'].tzinfo) - timedelta(days=days_back) if posts and posts[0].get('date') else None
    
    filtered_posts = []
    if cutoff:
        for p in posts:
            if p.get('date') and p['date'] >= cutoff:
                filtered_posts.append(p)
    else:
        filtered_posts = posts
    
    if not filtered_posts:
        filtered_posts = posts
    
    total_views = sum(p.get('views', 0) for p in filtered_posts)
    total_reactions = sum(p.get('reactions', 0) for p in filtered_posts)
    posts_with_links = sum(1 for p in filtered_posts if p.get('has_link'))
    
    count = len(filtered_posts)
    avg_views = total_views / count if count > 0 else 0
    avg_reactions = total_reactions / count if count > 0 else 0
    
    # ERR = средние просмотры / подписчики * 100%
    err = (avg_views / members * 100) if members > 0 else 0
    
    # ER = средние реакции / подписчики * 100%
    er = (avg_reactions / members * 100) if members > 0 else 0
    
    return {
        'total_posts': count,
        'total_views': total_views,
        'total_reactions': total_reactions,
        'avg_views': round(avg_views, 1),
        'avg_reactions': round(avg_reactions, 1),
        'er': round(er, 2),
        'err': round(err, 2),
        'posts_with_links': posts_with_links,
        'link_rate': round(posts_with_links / count * 100, 1) if count > 0 else 0
    }


def get_tg_full_data(username: str, days_back: int = 30) -> Dict:
    """Полные данные Telegram-канала"""
    info = get_tg_channel_info(username)
    if not info:
        return None
    
    posts = get_tg_posts(username, limit=100)
    metrics = calculate_tg_metrics(posts, info['members'], days_back)
    
    return {
        'info': info,
        'posts': posts,
        'metrics': metrics
    }
