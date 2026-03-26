# -*- coding: utf-8 -*-
"""
Дашборд сквозной аналитики SMM + Яндекс.Метрика
Streamlit приложение
"""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
import pandas as pd

from config import TG_CHANNEL, YANDEX_COUNTER_ID, DEFAULT_DAYS
from tg_collector import get_tg_full_data
from metrika_collector import get_metrika_data, get_demo_metrika_data
from database import init_db, save_tg_data, save_metrika_data, get_last_update
from analytics import aggregate_metrics, get_top_posts, calculate_funnel


# Настройка страницы
st.set_page_config(
    page_title='Сквозная аналитика SMM',
    page_icon='📊',
    layout='wide',
    initial_sidebar_state='expanded'
)

# Кастомные стили
st.markdown("""
<style>
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 20px;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin: 5px;
    }
    .kpi-value {
        font-size: 28px;
        font-weight: bold;
    }
    .kpi-label {
        font-size: 14px;
        opacity: 0.9;
    }
    .metric-card {
        background: #f8f9fa;
        padding: 15px;
        border-radius: 8px;
        border-left: 4px solid #667eea;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db():
    """Получение соединения с БД"""
    return init_db()


@st.cache_data(ttl=300)
def load_data(days_back: int, use_cache: bool = True):
    """Загрузка данных с кешированием"""
    conn = get_db()
    
    # Проверяем кеш
    if use_cache:
        last_tg = get_last_update(conn, 'telegram')
        last_metrika = get_last_update(conn, 'metrika')
        
        if last_tg and last_metrika:
            # Если данные свежие (< 1 часа), используем кеш
            last_update = datetime.fromisoformat(last_tg)
            if (datetime.now() - last_update).total_seconds() < 3600:
                st.info(f"Используются кешированные данные от {last_update.strftime('%H:%M:%S')}")
    
    # Загружаем Telegram
    with st.spinner('Загрузка данных Telegram...'):
        tg_data = get_tg_full_data(TG_CHANNEL, days_back)
        if tg_data:
            save_tg_data(conn, tg_data)
    
    # Загружаем Яндекс.Метрику
    with st.spinner('Загрузка данных Яндекс.Метрики...'):
        try:
            metrika_data = get_metrika_data(days_back)
            if not metrika_data.get('traffic_by_source'):
                metrika_data = get_demo_metrika_data(days_back)
                st.warning('Используются демо-данные Яндекс.Метрики (API недоступен)')
        except Exception as e:
            metrika_data = get_demo_metrika_data(days_back)
            st.warning(f'Ошибка API Метрики: {e}. Используются демо-данные.')
        
        save_metrika_data(conn, metrika_data, YANDEX_COUNTER_ID)
    
    # Агрегируем
    aggregated = aggregate_metrics(tg_data, metrika_data)
    
    return tg_data, metrika_data, aggregated


def render_kpi_cards(aggregated: dict):
    """Отображение KPI карточек"""
    col1, col2, col3, col4 = st.columns(4)
    
    tg = aggregated.get('telegram', {})
    cross = aggregated.get('cross_channel', {})
    
    with col1:
        st.metric(
            label="Подписчиков TG",
            value=f"{tg.get('members', 0):,}".replace(',', ' '),
            delta=None
        )
    
    with col2:
        st.metric(
            label="ERR (%)",
            value=f"{tg.get('err', 0):.2f}%",
            delta=None
        )
    
    with col3:
        st.metric(
            label="Трафик из соцсетей",
            value=f"{cross.get('social_users_on_site', 0):,}".replace(',', ' '),
            delta=f"{cross.get('smm_traffic_share', 0):.1f}% от общего"
        )
    
    with col4:
        st.metric(
            label="Конверсия",
            value=f"{cross.get('conversion_rate', 0):.2f}%",
            delta=None
        )


def render_telegram_section(tg_data: dict, aggregated: dict):
    """Секция Telegram"""
    st.subheader("📱 Telegram-канал")
    
    tg = aggregated.get('telegram', {})
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        # Метрики канала
        st.markdown("**Метрики канала**")
        metrics_df = pd.DataFrame({
            'Метрика': ['Подписчиков', 'Постов за период', 'Средние просмотры', 
                       'Средние реакции', 'ER (%)', 'ERR (%)', 'Постов со ссылками'],
            'Значение': [
                tg.get('members', 0),
                tg.get('total_posts', 0),
                f"{tg.get('avg_views', 0):.0f}",
                f"{tg.get('avg_reactions', 0):.1f}",
                f"{tg.get('er', 0):.2f}",
                f"{tg.get('err', 0):.2f}",
                f"{tg.get('posts_with_links', 0)} ({tg.get('link_rate', 0):.1f}%)"
            ]
        })
        st.dataframe(metrics_df, use_container_width=True, hide_index=True)
    
    with col2:
        # Топ постов
        st.markdown("**Топ-5 постов по просмотрам**")
        posts = tg_data.get('posts', []) if tg_data else []
        top_posts = get_top_posts(posts, 'views', 5)
        
        if top_posts:
            for i, post in enumerate(top_posts, 1):
                st.markdown(f"**{i}.** {post['text'][:50]}...")
                st.caption(f"👁 {post['views']:,} | ❤️ {post['reactions']} | ERR: {post['er']:.2f}%".replace(',', ' '))


def render_metrika_section(metrika_data: dict, aggregated: dict):
    """Секция Яндекс.Метрика"""
    st.subheader("🌐 Яндекс.Метрика")
    
    if aggregated.get('metrika', {}).get('is_demo'):
        st.info("📊 Отображаются демо-данные (API Метрики недоступен)")
    
    metrika = aggregated.get('metrika', {})
    daily = metrika.get('daily_stats', [])
    
    col1, col2 = st.columns(2)
    
    with col1:
        # График динамики визитов
        if daily:
            df_daily = pd.DataFrame(daily)
            fig = px.line(df_daily, x='date', y=['visits', 'users'],
                         title='Динамика визитов и пользователей',
                         labels={'value': 'Количество', 'variable': 'Метрика', 'date': 'Дата'},
                         color_discrete_sequence=['#667eea', '#764ba2'])
            fig.update_layout(hovermode='x unified')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Трафик по источникам
        traffic = metrika.get('traffic_by_source', [])
        if traffic:
            df_traffic = pd.DataFrame(traffic)
            fig = px.bar(df_traffic, x='source', y='visits',
                        title='Визиты по источникам трафика',
                        labels={'visits': 'Визиты', 'source': 'Источник'},
                        color='visits',
                        color_continuous_scale='Purples')
            fig.update_xaxes(tickangle=45)
            st.plotly_chart(fig, use_container_width=True)


def render_utm_table(aggregated: dict):
    """Таблица по UTM-кампаниям"""
    st.subheader("🏷️ Эффективность кампаний (UTM)")
    
    utm_data = aggregated.get('metrika', {}).get('traffic_by_utm', [])
    
    if utm_data:
        df_utm = pd.DataFrame(utm_data)
        df_utm.columns = ['Источник', 'Medium', 'Кампания', 'Пользователи', 'Визиты', 'Просмотры']
        st.dataframe(df_utm, use_container_width=True, hide_index=True)
    else:
        st.info("Нет данных по UTM-меткам")


def render_funnel(aggregated: dict):
    """Воронка конверсий"""
    st.subheader("🔄 Воронка конверсий")
    
    funnel = aggregated.get('funnel', [])
    
    if funnel:
        fig = go.Figure(go.Funnel(
            y=[f['stage'] for f in funnel],
            x=[f['value'] for f in funnel],
            textinfo="value+percent previous",
            marker={
                'color': ['#667eea', '#764ba2', '#f093fb', '#f5576c', '#4facfe'][:len(funnel)]
            }
        ))
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Недостаточно данных для построения воронки")


def render_source_comparison(aggregated: dict):
    """Сравнительный график источников"""
    st.subheader("📈 Сравнение источников трафика")
    
    traffic = aggregated.get('metrika', {}).get('traffic_by_source', [])
    
    if traffic:
        df = pd.DataFrame(traffic)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            name='Пользователи',
            x=df['source'],
            y=df['users'],
            marker_color='#667eea'
        ))
        
        fig.add_trace(go.Bar(
            name='Визиты',
            x=df['source'],
            y=df['visits'],
            marker_color='#764ba2'
        ))
        
        fig.update_layout(
            barmode='group',
            xaxis_tickangle=-45,
            height=400
        )
        
        st.plotly_chart(fig, use_container_width=True)


def main():
    st.title("📊 Дашборд сквозной аналитики")
    st.markdown("### SMM + Яндекс.Метрика")
    
    # Боковая панель
    st.sidebar.header("⚙️ Настройки")
    
    # Выбор периода
    st.sidebar.subheader("Период анализа")
    period = st.sidebar.selectbox(
        "Выберите период",
        options=[7, 14, 30, 60, 90],
        index=2,
        format_func=lambda x: f"{x} дней"
    )
    
    # Кнопка обновления
    force_refresh = st.sidebar.button("🔄 Обновить данные")
    
    st.sidebar.markdown("---")
    
    # Информация
    st.sidebar.markdown(f"""
    **Каналы:**
    - Telegram: `{TG_CHANNEL}`
    - Яндекс.Метрика: `{YANDEX_COUNTER_ID}`
    """)
    
    # Загрузка данных
    tg_data, metrika_data, aggregated = load_data(period, use_cache=not force_refresh)
    
    # Проверка данных
    if not tg_data:
        st.error("Не удалось загрузить данные Telegram")
        return
    
    # KPI карточки
    render_kpi_cards(aggregated)
    
    st.markdown("---")
    
    # Telegram секция
    render_telegram_section(tg_data, aggregated)
    
    st.markdown("---")
    
    # Метрика секция
    render_metrika_section(metrika_data, aggregated)
    
    st.markdown("---")
    
    # Две колонки: воронка и UTM таблица
    col1, col2 = st.columns(2)
    
    with col1:
        render_funnel(aggregated)
    
    with col2:
        render_utm_table(aggregated)
    
    st.markdown("---")
    
    # Сравнение источников
    render_source_comparison(aggregated)
    
    # Футер
    st.markdown("---")
    st.caption(f"Последнее обновление: {datetime.now().strftime('%d.%m.%Y %H:%M:%S')}")


if __name__ == '__main__':
    main()
