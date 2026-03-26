# -*- coding: utf-8 -*-
"""Модуль базы данных SQLite для кеширования данных"""

import sqlite3
import json
from datetime import datetime
from typing import Dict, List, Optional
import os

from config import DB_PATH


def init_db() -> sqlite3.Connection:
    """Инициализация базы данных"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    
    # Telegram данные
    c.execute('''CREATE TABLE IF NOT EXISTS tg_channel (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        username TEXT,
        name TEXT,
        members INTEGER,
        description TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tg_posts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        channel_username TEXT,
        post_id INTEGER,
        date TEXT,
        text TEXT,
        views INTEGER,
        reactions INTEGER,
        has_link INTEGER
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS tg_metrics (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        channel_username TEXT,
        total_posts INTEGER,
        total_views INTEGER,
        total_reactions INTEGER,
        avg_views REAL,
        avg_reactions REAL,
        er REAL,
        err REAL,
        posts_with_links INTEGER,
        link_rate REAL
    )''')
    
    # Яндекс.Метрика данные
    c.execute('''CREATE TABLE IF NOT EXISTS metrika_counter (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        counter_id TEXT,
        name TEXT,
        site TEXT,
        status TEXT
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS metrika_traffic (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        counter_id TEXT,
        source TEXT,
        users INTEGER,
        visits INTEGER,
        pageviews INTEGER,
        bounce_rate REAL,
        avg_duration REAL
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS metrika_utm (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        counter_id TEXT,
        utm_source TEXT,
        utm_medium TEXT,
        utm_campaign TEXT,
        users INTEGER,
        visits INTEGER,
        pageviews INTEGER
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS metrika_daily (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        counter_id TEXT,
        date TEXT,
        users INTEGER,
        visits INTEGER,
        pageviews INTEGER,
        bounce_rate REAL
    )''')
    
    c.execute('''CREATE TABLE IF NOT EXISTS metrika_conversions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT,
        counter_id TEXT,
        source TEXT,
        users INTEGER,
        visits INTEGER,
        conversions INTEGER,
        conversion_rate REAL
    )''')
    
    # Кеш обновлений
    c.execute('''CREATE TABLE IF NOT EXISTS cache_status (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        last_update TEXT,
        status TEXT
    )''')
    
    conn.commit()
    return conn


def save_tg_data(conn: sqlite3.Connection, data: Dict) -> None:
    """Сохранение Telegram данных"""
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    info = data.get('info', {})
    metrics = data.get('metrics', {})
    posts = data.get('posts', [])
    
    # Информация о канале
    c.execute('''INSERT INTO tg_channel 
        (timestamp, username, name, members, description)
        VALUES (?, ?, ?, ?, ?)''', (
            timestamp,
            info.get('username', ''),
            info.get('name', ''),
            info.get('members', 0),
            info.get('description', '')
        ))
    
    # Метрики
    c.execute('''INSERT INTO tg_metrics 
        (timestamp, channel_username, total_posts, total_views, total_reactions,
         avg_views, avg_reactions, er, err, posts_with_links, link_rate)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''', (
            timestamp,
            info.get('username', ''),
            metrics.get('total_posts', 0),
            metrics.get('total_views', 0),
            metrics.get('total_reactions', 0),
            metrics.get('avg_views', 0),
            metrics.get('avg_reactions', 0),
            metrics.get('er', 0),
            metrics.get('err', 0),
            metrics.get('posts_with_links', 0),
            metrics.get('link_rate', 0)
        ))
    
    # Посты
    for post in posts[:100]:
        c.execute('''INSERT INTO tg_posts 
            (timestamp, channel_username, post_id, date, text, views, reactions, has_link)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
                timestamp,
                info.get('username', ''),
                post.get('post_id', 0),
                post.get('date').isoformat() if post.get('date') else '',
                post.get('text', '')[:500],
                post.get('views', 0),
                post.get('reactions', 0),
                1 if post.get('has_link') else 0
            ))
    
    # Обновить статус кеша
    c.execute('''INSERT OR REPLACE INTO cache_status (source, last_update, status)
        VALUES (?, ?, ?)''', ('telegram', timestamp, 'success'))
    
    conn.commit()


def save_metrika_data(conn: sqlite3.Connection, data: Dict, counter_id: str) -> None:
    """Сохранение данных Яндекс.Метрики"""
    c = conn.cursor()
    timestamp = datetime.now().isoformat()
    
    # Информация о счётчике
    counter_info = data.get('counter_info')
    if counter_info:
        c.execute('''INSERT INTO metrika_counter 
            (timestamp, counter_id, name, site, status)
            VALUES (?, ?, ?, ?, ?)''', (
                timestamp,
                str(counter_info.get('id', counter_id)),
                counter_info.get('name', ''),
                counter_info.get('site', ''),
                counter_info.get('status', '')
            ))
    
    # Трафик по источникам
    for item in data.get('traffic_by_source', []):
        c.execute('''INSERT INTO metrika_traffic 
            (timestamp, counter_id, source, users, visits, pageviews, bounce_rate, avg_duration)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
                timestamp,
                counter_id,
                item.get('source', ''),
                item.get('users', 0),
                item.get('visits', 0),
                item.get('pageviews', 0),
                item.get('bounce_rate', 0),
                item.get('avg_duration', 0)
            ))
    
    # UTM данные
    for item in data.get('traffic_by_utm', []):
        c.execute('''INSERT INTO metrika_utm 
            (timestamp, counter_id, utm_source, utm_medium, utm_campaign, users, visits, pageviews)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', (
                timestamp,
                counter_id,
                item.get('utm_source', ''),
                item.get('utm_medium', ''),
                item.get('utm_campaign', ''),
                item.get('users', 0),
                item.get('visits', 0),
                item.get('pageviews', 0)
            ))
    
    # Статистика по дням
    for item in data.get('daily_stats', []):
        c.execute('''INSERT INTO metrika_daily 
            (timestamp, counter_id, date, users, visits, pageviews, bounce_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                timestamp,
                counter_id,
                item.get('date', ''),
                item.get('users', 0),
                item.get('visits', 0),
                item.get('pageviews', 0),
                item.get('bounce_rate', 0)
            ))
    
    # Конверсии
    for item in data.get('conversions', []):
        c.execute('''INSERT INTO metrika_conversions 
            (timestamp, counter_id, source, users, visits, conversions, conversion_rate)
            VALUES (?, ?, ?, ?, ?, ?, ?)''', (
                timestamp,
                counter_id,
                item.get('source', ''),
                item.get('users', 0),
                item.get('visits', 0),
                item.get('conversions', 0),
                item.get('conversion_rate', 0)
            ))
    
    # Обновить статус кеша
    c.execute('''INSERT OR REPLACE INTO cache_status (source, last_update, status)
        VALUES (?, ?, ?)''', ('metrika', timestamp, 'success' if not data.get('is_demo') else 'demo'))
    
    conn.commit()


def get_last_update(conn: sqlite3.Connection, source: str) -> Optional[str]:
    """Получение времени последнего обновления"""
    c = conn.cursor()
    c.execute('SELECT last_update FROM cache_status WHERE source = ?', (source,))
    result = c.fetchone()
    return result[0] if result else None


def get_cached_tg_metrics(conn: sqlite3.Connection, username: str) -> Optional[Dict]:
    """Получение кешированных метрик Telegram"""
    c = conn.cursor()
    c.execute('''SELECT * FROM tg_metrics 
        WHERE channel_username = ? 
        ORDER BY timestamp DESC LIMIT 1''', (username,))
    result = c.fetchone()
    
    if result:
        return {
            'timestamp': result[1],
            'total_posts': result[3],
            'total_views': result[4],
            'total_reactions': result[5],
            'avg_views': result[6],
            'avg_reactions': result[7],
            'er': result[8],
            'err': result[9]
        }
    return None


def get_cached_tg_posts(conn: sqlite3.Connection, username: str, limit: int = 50) -> List[Dict]:
    """Получение кешированных постов"""
    c = conn.cursor()
    c.execute('''SELECT post_id, date, text, views, reactions, has_link 
        FROM tg_posts 
        WHERE channel_username = ? 
        ORDER BY date DESC LIMIT ?''', (username, limit))
    
    posts = []
    for row in c.fetchall():
        posts.append({
            'post_id': row[0],
            'date': row[1],
            'text': row[2],
            'views': row[3],
            'reactions': row[4],
            'has_link': bool(row[5])
        })
    return posts


def get_cached_metrika_daily(conn: sqlite3.Connection, counter_id: str) -> List[Dict]:
    """Получение кешированной статистики по дням"""
    c = conn.cursor()
    c.execute('''SELECT date, users, visits, pageviews, bounce_rate 
        FROM metrika_daily 
        WHERE counter_id = ? 
        ORDER BY date ASC''', (counter_id,))
    
    daily = []
    for row in c.fetchall():
        daily.append({
            'date': row[0],
            'users': row[1],
            'visits': row[2],
            'pageviews': row[3],
            'bounce_rate': row[4]
        })
    return daily


def get_cached_metrika_traffic(conn: sqlite3.Connection, counter_id: str) -> List[Dict]:
    """Получение кешированного трафика по источникам"""
    c = conn.cursor()
    c.execute('''SELECT source, users, visits, pageviews, bounce_rate, avg_duration 
        FROM metrika_traffic 
        WHERE counter_id = ? 
        ORDER BY timestamp DESC LIMIT 20''', (counter_id,))
    
    traffic = []
    for row in c.fetchall():
        traffic.append({
            'source': row[0],
            'users': row[1],
            'visits': row[2],
            'pageviews': row[3],
            'bounce_rate': row[4],
            'avg_duration': row[5]
        })
    return traffic
