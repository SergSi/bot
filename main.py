"""
ТЕЛЕГРАМ-БОТ С УПРОЩЕННЫМ ПАРСИНГОМ КАНАЛА
Без aiohttp - использует стандартные библиотеки
"""

import logging
import asyncio
import json
import os
import re
from datetime import datetime, timedelta
from functools import wraps
from typing import List, Dict, Optional
from html import unescape
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application, 
    CommandHandler, 
    CallbackQueryHandler, 
    ContextTypes, 
    MessageHandler, 
    filters
)
from telegram.error import TimedOut, NetworkError, BadRequest

# ⚠️ ⚠️ ⚠️ НАСТРОЙКИ ⚠️ ⚠️ ⚠️
TOKEN = "8486101545:AAGlzxkTFZr6qMmBuIr4E_7hOOZwER4RaRA"
CHANNEL_USERNAME = "@land_use_58"
CHANNEL_LINK = "https://t.me/land_use_58"
GROUP_LINK = "https://t.me/land_use_59"
EMAIL = "a1457780@yandex.ru"

# Константы
CACHE_FILE = "channel_cache.json"
UPDATE_INTERVAL_WEEKS = 1
POSTS_LIMIT = 50

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', 
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ---------- УТИЛИТЫ ДЛЯ ОЧИСТКИ ТЕКСТА ----------

def clean_post_text(text: str, max_length: int = 100) -> str:
    """Очистить текст поста для корректного отображения в Telegram"""
    if not text or text.strip() == '':
        return "Без названия"
    
    # 1. Удаляем HTML-теги
    text = re.sub(r'<[^>]+>', '', text)
    
    # 2. Декодируем HTML-сущности
    text = unescape(text)
    
    # 3. Заменяем служебные символы пробелами
    text = text.replace('<br>', ' ').replace('<br/>', ' ').replace('<br />', ' ')
    text = text.replace('&nbsp;', ' ').replace('\r\n', ' ').replace('\n', ' ')
    
    # 4. Удаляем лишние пробелы
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 5. Обрезаем до нужной длины
    if len(text) > max_length:
        # Пытаемся обрезать по последнему пробелу
        truncated = text[:max_length]
        if ' ' in truncated:
            truncated = truncated[:truncated.rfind(' ')]
        text = truncated + "..."
    
    return text

def escape_markdown(text: str) -> str:
    """Экранировать специальные символы Markdown, но не мешать тексту"""
    # Сначала убираем ЛИШНЕЕ экранирование точек
    # 1. Точки после цифр в начале строки или после пробела
    text = re.sub(r'(\d)\\\.', r'\1.', text)
    
    # 2. Многоточия (три точки подряд)
    text = text.replace('\\.\\.\\.', '...')
    
    # 3. Точки в середине предложений
    text = text.replace('\\.', '.')
    
    # Теперь экранируем только реально опасные символы
    # Но НЕ точку!
    escape_chars = r'_*[]()~`>#+-=|{}!'
    for char in escape_chars:
        text = text.replace(char, f'\\{char}')
    
    return text

# ---------- УПРОЩЕННЫЙ ПАРСИНГ (БЕЗ AIOHTTP) ----------

import urllib.request
import urllib.error

class SimpleChannelParser:
    """Упрощенный парсер без aiohttp"""
    
    def __init__(self, channel_username: str):
        self.channel_username = channel_username
    
    async def fetch_messages(self) -> List[Dict]:
        """Получить сообщения через requests (синхронно в executor)"""
        try:
            # Запускаем в executor, чтобы не блокировать event loop
            loop = asyncio.get_event_loop()
            messages = await loop.run_in_executor(
                None, 
                self._fetch_messages_sync
            )
            return messages
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}")
            return []
    
    def _fetch_messages_sync(self) -> List[Dict]:
        """Синхронный парсинг"""
        try:
            url = f"https://tg.i-c-a.su/json/{self.channel_username[1:]}/?limit={POSTS_LIMIT}"
            
            # Делаем запрос
            req = urllib.request.Request(
                url,
                headers={'User-Agent': 'Mozilla/5.0'}
            )
            
            with urllib.request.urlopen(req, timeout=30) as response:
                data = json.loads(response.read().decode('utf-8'))
                messages = data.get('messages', [])
                
                # Обрабатываем сообщения
                processed = []
                for msg in messages:
                    if msg.get('message'):
                        raw_title = msg.get('message', '')
                        cleaned_title = clean_post_text(raw_title, 100)
                        
                        # Преобразуем дату
                        date_str = msg.get('date')
                        post_date = None
                        if date_str:
                            try:
                                # Пример формата: "2024-01-01T12:00:00"
                                # Убираем 'Z' в конце и добавляем часовой пояс
                                if date_str.endswith('Z'):
                                    date_str = date_str[:-1] + '+00:00'
                                post_date = datetime.fromisoformat(date_str)
                            except Exception as e:
                                logger.warning(f"Не удалось распарсить дату '{date_str}': {e}")
                                post_date = datetime.now()
                        else:
                            post_date = datetime.now()
                        
                        processed.append({
                            'id': msg.get('id'),
                            'title': cleaned_title,
                            'link': f"https://t.me/{self.channel_username[1:]}/{msg.get('id')}",
                            'views': int(msg.get('views', 0)),
                            'forwards': int(msg.get('forwards', 0)),
                            'replies': int(msg.get('replies', {}).get('replies', 0)),
                            'date': post_date,
                            'raw_date': date_str
                        })
                
                logger.info(f"Парсинг успешен: получено {len(processed)} сообщений")
                if processed:
                    logger.info(f"Первое сообщение: {processed[0]}")
                return processed
                
        except urllib.error.URLError as e:
            logger.error(f"Ошибка URL: {e}")
            return []
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка JSON: {e}")
            return []
        except Exception as e:
            logger.error(f"Ошибка парсинга: {e}")
            return []
    
    def get_top_posts(self, posts: List[Dict], limit: int = 5) -> List[Dict]:
        """Получить топ постов по композитному рейтингу"""
        if not posts:
            return []
        
        logger.info(f"Сортировка {len(posts)} постов для определения топ-{limit}")
        
        for post in posts:
            # Получаем дату
            post_date = post.get('date', datetime.now())
            
            # Баллы за просмотры, репосты и комментарии
            views_score = post.get('views', 0) * 0.4
            forwards_score = post.get('forwards', 0) * 0.25
            replies_score = post.get('replies', 0) * 0.20
            
            # Свежесть (чем новее - тем выше балл)
            days_old = (datetime.now() - post_date).days
            freshness_score = max(0, 100 - days_old) * 0.15
            
            post['composite_score'] = views_score + forwards_score + replies_score + freshness_score
            
            # Для отладки
            if logger.isEnabledFor(logging.DEBUG):
                logger.debug(f"Пост '{post.get('title', '')[:30]}...': "
                           f"views={post.get('views')}*0.4={views_score:.1f}, "
                           f"forwards={post.get('forwards')}*0.25={forwards_score:.1f}, "
                           f"replies={post.get('replies')}*0.20={replies_score:.1f}, "
                           f"freshness={freshness_score:.1f}, "
                           f"total={post['composite_score']:.1f}")
        
        # Сортируем по композитному скору
        sorted_posts = sorted(posts, key=lambda x: x.get('composite_score', 0), reverse=True)
        
        logger.info(f"Топ-пост имеет score: {sorted_posts[0].get('composite_score', 0):.1f}" 
                   if sorted_posts else "Нет постов для сортировки")
        
        return sorted_posts[:limit]

# ---------- МЕНЕДЖЕР КЭША ----------

class CacheManager:
    """Управление кэшем канала"""
    
    def __init__(self, cache_file: str = CACHE_FILE):
        self.cache_file = cache_file
        self.parser = SimpleChannelParser(CHANNEL_USERNAME)
    
    async def update_cache(self) -> bool:
        """Обновить кэш канала"""
        logger.info(f"🔄 Обновление кэша для {CHANNEL_USERNAME}")
        
        try:
            messages = await self.parser.fetch_messages()
            
            if not messages:
                logger.warning("Не удалось получить сообщения")
                return False
            
            logger.info(f"Получено {len(messages)} сообщений для анализа")
            
            # Используем метод get_top_posts из парсера
            top_posts = self.parser.get_top_posts(messages, 10)
            
            # Рассчитываем статистику
            total_views = sum(p.get('views', 0) for p in messages)
            avg_views = total_views / max(len(messages), 1)
            
            # Находим самый просматриваемый пост
            most_viewed = {}
            if messages:
                most_viewed = max(messages, key=lambda x: x.get('views', 0))
            
            # Рассчитываем средние репосты
            total_forwards = sum(p.get('forwards', 0) for p in messages)
            avg_forwards = total_forwards / max(len(messages), 1)
            
            # Подготавливаем данные для кэша
            cache_data = {
                'channel': CHANNEL_USERNAME,
                'last_updated': datetime.now().isoformat(),
                'total_posts': len(messages),
                'top_posts': top_posts[:5],  # Берем только топ-5
                'stats': {
                    'total_views': total_views,
                    'avg_views': round(avg_views, 1),
                    'total_forwards': total_forwards,
                    'avg_forwards': round(avg_forwards, 1),
                    'most_viewed': most_viewed
                }
            }
            
            # Сохраняем в файл
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(cache_data, f, ensure_ascii=False, indent=2, default=str)
            
            logger.info(f"✅ Кэш обновлен: {len(top_posts[:5])} постов, "
                       f"всего просмотров: {total_views:,}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Ошибка обновления кэша: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def get_cache(self) -> Optional[Dict]:
        """Получить данные из кэша"""
        try:
            if not os.path.exists(self.cache_file):
                logger.info("Файл кэша не найден")
                return None
            
            with open(self.cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            last_updated = datetime.fromisoformat(data['last_updated'])
            days_since_update = (datetime.now() - last_updated).days
            
            logger.info(f"Кэш обновлялся {days_since_update} дней назад")
            
            if days_since_update >= UPDATE_INTERVAL_WEEKS * 7:
                logger.info("Кэш устарел")
                return None
            
            return data
            
        except json.JSONDecodeError as e:
            logger.error(f"Ошибка чтения кэша: {e}")
            return None
        except Exception as e:
            logger.error(f"Ошибка получения кэша: {e}")
            return None
    
    def get_top_posts_from_cache(self) -> List[Dict]:
        """Получить топ-посты из кэша"""
        cache = self.get_cache()
        if cache:
            posts = cache.get('top_posts', [])
            logger.info(f"Из кэша получено {len(posts)} постов")
            return posts
        logger.info("Кэш пуст или устарел")
        return []
    
    def get_cache_stats(self) -> Optional[Dict]:
        """Получить статистику из кэша"""
        cache = self.get_cache()
        if cache:
            stats = cache.get('stats', {})
            stats['total_posts'] = cache.get('total_posts', 0)
            stats['last_updated'] = cache.get('last_updated', 'Неизвестно')
            logger.info(f"Статистика из кэша: {len(stats)} пунктов")
            return stats
        return None

# ---------- УТИЛИТЫ ДЛЯ ОБРАБОТКИ ОШИБОК ----------

def with_retry(max_retries=2, delay=1.0):
    """Декоратор для повторных попыток"""
    def decorator(func):
        @wraps(func)
        async def wrapper(update: Update, context: ContextTypes.DEFAULT_TYPE, *args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(update, context, *args, **kwargs)
                except (TimedOut, NetworkError) as e:
                    logger.warning(f"Таймаут в {func.__name__}, попытка {attempt+1}/{max_retries}: {e}")
                    if attempt < max_retries - 1:
                        await asyncio.sleep(delay * (attempt + 1))
                    else:
                        raise
                except Exception as e:
                    logger.error(f"Ошибка в {func.__name__}: {e}")
                    raise
            return None
        return wrapper
    return decorator

async def safe_send_message(bot, chat_id, text, **kwargs):
    """Безопасная отправка сообщения"""
    for attempt in range(3):
        try:
            return await bot.send_message(chat_id=chat_id, text=text, **kwargs)
        except (TimedOut, NetworkError):
            if attempt < 2:
                await asyncio.sleep(1.0 * (attempt + 1))
            else:
                raise
        except BadRequest as e:
            if "message is not modified" in str(e):
                return None
            raise

async def safe_edit_message(query, text, **kwargs):
    """Безопасное редактирование сообщения"""
    for attempt in range(2):
        try:
            return await query.edit_message_text(text=text, **kwargs)
        except (TimedOut, NetworkError):
            if attempt < 1:
                await asyncio.sleep(1.0 * (attempt + 1))
            else:
                raise
        except BadRequest as e:
            if "message is not modified" in str(e):
                return None
            raise

# ---------- ИНИЦИАЛИЗАЦИЯ ----------

cache_manager = CacheManager()

# ---------- ОСНОВНЫЕ ФУНКЦИИ БОТА ----------

@with_retry(max_retries=2, delay=1.0)
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start"""
    keyboard = [
        [InlineKeyboardButton("📚 Обзор материалов канала", url="https://kadinfo.ru/2025/12/25/itogi/")],
        [InlineKeyboardButton("🎓 Обучающие и справочные материалы", callback_data="main_course")],
        [InlineKeyboardButton("📅 Консультация", callback_data="consultation")],
        [InlineKeyboardButton("📊 Топ-5 постов канала", callback_data="top_posts")],
        [InlineKeyboardButton("📢 Наш канал", url=CHANNEL_LINK)],
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)

    welcome_text = (
        "Навигатор канала *Практика землепользования*.\n\n"
        "Выберите, что вас интересует:"
    )
    
    try:
        if update.message:
            await safe_send_message(
                context.bot,
                update.message.chat.id,
                welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
        elif update.callback_query:
            await safe_edit_message(
                update.callback_query,
                welcome_text,
                reply_markup=reply_markup,
                parse_mode='Markdown'
            )
            
    except Exception as e:
        logger.error(f"Ошибка в start: {e}")

@with_retry(max_retries=2, delay=1.0)
async def main_course(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обучающие материалы"""
    query = update.callback_query
    await query.answer()
    
    text = ("**Материалы для самостоятельного использования - обучающие и справочные:**\n\n"
            "1. [Анализ судебного иска](https://kadinfo.ru/2025/03/09/isk/)\n"
            "2. [Как разобраться в судебном споре?](https://kadinfo.ru/2025/10/21/train/)\n"
            "3. [История земельной реформы](https://kadinfo.ru/2025/08/25/reforma/)\n"
            "4. [Проблемы СНТ и дачных поселков](https://kadinfo.ru/2025/12/20/snt/)\n"
            "5. [Конструктор для изучения и проектирования землепользования](https://kadinfo.ru/2026/01/05/method/)")
    
    keyboard = [[InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(
        query,
        text, 
        reply_markup=reply_markup, 
        parse_mode='Markdown', 
        disable_web_page_preview=False
    )

@with_retry(max_retries=2, delay=1.0)
async def consultation(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Консультация"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "**📅 Способы связи:**\n\n"
        "📧 *Электронная почта - скопируйте:*\n"
        f"`{EMAIL}`\n\n"
        "📝 *Группа «Вопросы и ответы»:*\n"
        f"[@land_use_59]({GROUP_LINK})\n\n"
        "*Рекомендуем указать:*\n"
        "• Ваше имя\n• Краткое описание вопроса\n• Регион/город"
    )
    
    keyboard = [
        [InlineKeyboardButton("📝 Перейти в группу", url=GROUP_LINK)],
        [InlineKeyboardButton("📋 Как написать письмо", callback_data="how_to_email")],
        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(
        query,
        text, 
        reply_markup=reply_markup, 
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

@with_retry(max_retries=2, delay=1.0)
async def top_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Топ-5 постов канала (автоматически обновляемые)"""
    query = update.callback_query
    await query.answer()
    
    # Получаем топ-посты из кэша
    top_posts = cache_manager.get_top_posts_from_cache()
    cache_data = cache_manager.get_cache()
    
    if not top_posts:
        # Если кэш пустой или устарел
        await query.edit_message_text(
            "🔄 *Загружаю свежие посты канала...*\n\n"
            "Это займет несколько секунд.",
            parse_mode='Markdown'
        )
        
        # Пробуем обновить кэш
        success = await cache_manager.update_cache()
        if success:
            top_posts = cache_manager.get_top_posts_from_cache()
            cache_data = cache_manager.get_cache()
        else:
            await query.edit_message_text(
                "❌ *Не удалось обновить посты*\n\n"
                "Попробуйте позже или воспользуйтесь резервным списком.",
                parse_mode='Markdown',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data="refresh_top")],
                    [InlineKeyboardButton("📖 Резервный список", callback_data="fallback_posts")],
                    [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]
                ])
            )
            return
    
    if top_posts:
        # Форматируем дату обновления
        last_updated = "Неизвестно"
        if cache_data and 'last_updated' in cache_data:
            try:
                update_time = datetime.fromisoformat(cache_data['last_updated'])
                last_updated = update_time.strftime("%d.%m.%Y %H:%M")
            except:
                last_updated = "Недавно"
        
        # Создаем текст с топ-постами
        text = f"**🎯 ТОП-5 ПОПУЛЯРНЫХ ПОСТОВ КАНАЛА**\n\n"
        text += f"_Обновлено: {last_updated}_\n"
        text += f"_Следующее обновление через {UPDATE_INTERVAL_WEEKS} неделю(и)_\n\n"
        
        for i, post in enumerate(top_posts, 1):
            title = post.get('title', 'Без названия')
            link = post.get('link', '#')
            views = post.get('views', 0)
            forwards = post.get('forwards', 0)
            replies = post.get('replies', 0)
            
            # Экранируем Markdown символы в заголовке
            title_escaped = escape_markdown(title)
            
            # Форматируем статистику
            stats_parts = []
            if views > 0:
                stats_parts.append(f"👁 {views:,}")
            if forwards > 0:
                stats_parts.append(f"🔄 {forwards}")
            if replies > 0:
                stats_parts.append(f"💬 {replies}")
            
            stats_str = " | ".join(stats_parts) if stats_parts else "📊 Нет статистики"
            
            text += f"{i}\\. [{title_escaped}]({link})\n"
            text += f"   {stats_str}\n\n"
        
        # Добавляем информацию о следующем обновлении
        text += "\n*Как определяется рейтинг:*\n"
        text += "• 👁 Просмотры (40%)\n"
        text += "• 🔄 Репосты (25%)\n"
        text += "• 💬 Комментарии (20%)\n"
        text += "• 📅 Свежесть (15%)\n"
        
        keyboard = [
            [InlineKeyboardButton("🔄 Обновить сейчас", callback_data="refresh_top")],
            [InlineKeyboardButton("📊 Статистика канала", callback_data="channel_stats")],
            [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(
            query,
            text,
            reply_markup=reply_markup,
            parse_mode='Markdown',
            disable_web_page_preview=True
        )
    else:
        # Если нет постов даже после обновления
        await show_fallback_top_posts(query)

async def fallback_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать резервный список постов"""
    query = update.callback_query
    await query.answer()
    await show_fallback_top_posts(query)

async def show_fallback_top_posts(query):
    """Резервный список постов"""
    text = ("**📖 Топ-5 постов (резервный список):**\n\n"
            "1. [КОРОТКО И ПО ДЕЛУ: что должен проверить покупатель перед выбором участка](https://t.me/land_use_58/5)\n"
            "2. [ОНТОЛОГИЯ СУДЕБНОГО СПОРА](https://t.me/land_use_58/12)\n"
            "3. [КАДАСТРОВЫЙ ИНЖЕНЕР РАДИСТ С «ТИТАНИКА» РОССИЙСКОЙ НЕДВИЖИМОСТИ](https://t.me/land_use_58/13)\n"
            "4. [ДЛЯ ВСЕХ ДАЧНИКОВ И ЗАСТРОЙЩИКОВ!](https://t.me/land_use_58/15)\n"
            "5. [МОЖНО ЛИ ПЕРЕВЕСТИ САДОВЫЙ УЧАСТОК В ИЖС?](https://t.me/land_use_58/23)\n\n"
            "_Автоматическое обновление временно недоступно_")
    
    keyboard = [
        [InlineKeyboardButton("🔄 Попробовать обновить", callback_data="refresh_top")],
        [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(
        query,
        text,
        reply_markup=reply_markup,
        parse_mode='Markdown',
        disable_web_page_preview=True
    )

@with_retry(max_retries=2, delay=1.0)
async def refresh_top_posts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обновить топ-посты вручную"""
    query = update.callback_query
    await query.answer("🔄 Обновляю список...", show_alert=False)
    
    # Показываем сообщение об обновлении
    await query.edit_message_text(
        "🔄 *Обновление списка постов...*\n\n"
        "Пожалуйста, подождите несколько секунд.",
        parse_mode='Markdown'
    )
    
    success = await cache_manager.update_cache()
    
    if success:
        # Возвращаемся к обновленному списку
        await top_posts(update, context)
    else:
        await query.edit_message_text(
            "❌ *Не удалось обновить список*\n\n"
            "Возможно, возникли проблемы с доступом к каналу. "
            "Попробуйте позже или воспользуйтесь резервным списком.",
            parse_mode='Markdown',
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔄 Попробовать снова", callback_data="refresh_top")],
                [InlineKeyboardButton("📖 Резервный список", callback_data="fallback_posts")],
                [InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_start")]
            ])
        )

@with_retry(max_retries=2, delay=1.0)
async def channel_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Статистика канала"""
    query = update.callback_query
    await query.answer()
    
    stats = cache_manager.get_cache_stats()
    cache_data = cache_manager.get_cache()
    
    if stats and cache_data:
        last_updated = "Неизвестно"
        if 'last_updated' in stats:
            try:
                update_time = datetime.fromisoformat(stats['last_updated'])
                last_updated = update_time.strftime("%d.%m.%Y %H:%M")
            except:
                pass
        
        text = f"**📊 СТАТИСТИКА КАНАЛА {CHANNEL_USERNAME}**\n\n"
        text += f"_Данные обновлены: {last_updated}_\n"
        text += f"_Проанализировано постов: {stats.get('total_posts', 0)}_\n\n"
        
        text += "*Средние показатели:*\n"
        text += f"• 👁 Средние просмотры: {stats.get('avg_views', 0):.0f}\n"
        text += f"• 🔄 Средние репосты: {stats.get('avg_forwards', 0):.1f}\n"
        text += f"• 📊 Всего просмотров: {stats.get('total_views', 0):,}\n"
        text += f"• 📈 Всего репостов: {stats.get('total_forwards', 0):,}\n\n"
        
        # Самый популярный пост
        most_viewed = stats.get('most_viewed', {})
        if most_viewed:
            text += "*Самый популярный пост:*\n"
            text += f"• 👁 Просмотров: {most_viewed.get('views', 0):,}\n"
            text += f"• 🔄 Репостов: {most_viewed.get('forwards', 0)}\n"
            if most_viewed.get('title'):
                title = most_viewed.get('title', '')[:50]
                if len(most_viewed.get('title', '')) > 50:
                    title += "..."
                text += f"• 📝 {title}\n"
            if most_viewed.get('link'):
                text += f"• 🔗 [Ссылка на пост]({most_viewed['link']})\n"
        
        text += f"\n*Следующее обновление:* через {UPDATE_INTERVAL_WEEKS} неделю(и)"
        
        keyboard = [
            [InlineKeyboardButton("📝 К топ-постам", callback_data="top_posts")],
            [InlineKeyboardButton("🔄 Обновить статистику", callback_data="refresh_top")],
            [InlineKeyboardButton("📢 Перейти в канал", url=CHANNEL_LINK)],
            [InlineKeyboardButton("🔙 В меню", callback_data="back_to_start")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await safe_edit_message(
            query,
            text,
            reply_markup=reply_markup,
            parse_mode='Markdown'
        )
    else:
        # Пытаемся обновить кэш, если статистики нет
        await query.edit_message_text(
            "🔄 *Статистика недоступна. Обновляю данные...*",
            parse_mode='Markdown'
        )
        
        success = await cache_manager.update_cache()
        if success:
            await channel_stats(update, context)
        else:
            await query.edit_message_text(
                "📊 Статистика временно недоступна.\n\n"
                "Попробуйте обновить данные позже.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔄 Попробовать снова", callback_data="channel_stats")],
                    [InlineKeyboardButton("🔙 Назад к постам", callback_data="top_posts")]
                ])
            )

async def copy_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Копирование email"""
    query = update.callback_query
    await query.answer(
        f"📋 Email скопирован: {EMAIL}\n\n"
        "Вставьте его в поле 'Кому' вашего почтового клиента.",
        show_alert=True
    )

async def how_to_email(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Как написать письмо"""
    query = update.callback_query
    await query.answer()
    
    text = (
        "📝 *Как правильно написать письмо для консультации:*\n\n"
        "1. *Тема письма:*\n"
        "   «Консультация по земельному вопросу»\n\n"
        "2. *Содержание письма:*\n"
        "   • Ваше ФИО\n"
        "   • Контактный телефон\n"
        "   • Регион и город\n"
        "   • Кадастровый номер участка (если есть)\n"
        "   • Суть вопроса\n"
        "   • Что уже пробовали сделать\n\n"
        "3. *Приложения:*\n"
        "   • Скан документов (ГПЗУ, выписка ЕГРН)\n"
        "   • Фото участка\n"
        "   • Схемы, планы\n\n"
        f"📧 *Email:* `{EMAIL}`\n\n"
        "*Срок ответа:* 1-2 рабочих дня"
    )
    
    keyboard = [
        [InlineKeyboardButton("📝 Перейти в группу", url=GROUP_LINK)],
        [InlineKeyboardButton("🔙 К консультациям", callback_data="consultation")]
    ]
    reply_markup = InlineKeyboardMarkup(keyboard)
    
    await safe_edit_message(
        query,
        text, 
        reply_markup=reply_markup, 
        parse_mode='Markdown'
    )

@with_retry(max_retries=3, delay=1.5)
async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка сообщений от пользователей"""
    if update.message and update.message.text and update.message.text.startswith('/'):
        return
    
    if update.message:
        await safe_send_message(
            context.bot,
            update.message.chat.id,
            "Для навигации используйте меню бота. ✨\n\n"
            "Напишите /start чтобы открыть главное меню.",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🚀 Открыть меню", callback_data="back_to_start")]
            ])
        )

# ---------- ФОНОВЫЕ ЗАДАЧИ ----------

async def scheduled_cache_update(context: ContextTypes.DEFAULT_TYPE):
    """Плановое обновление кэша"""
    logger.info("⏰ Запуск планового обновления кэша...")
    await cache_manager.update_cache()

async def background_update_task(application: Application):
    """Фоновая задача для обновления кэша раз в неделю"""
    while True:
        try:
            # Ждем неделю
            await asyncio.sleep(UPDATE_INTERVAL_WEEKS * 7 * 24 * 3600)
            
            # Обновляем кэш
            await scheduled_cache_update(application)
            
        except Exception as e:
            logger.error(f"Ошибка в фоновом обновлении: {e}")
            await asyncio.sleep(3600)  # Ждем час при ошибке

# ---------- ЗАПУСК БОТА ----------

async def post_init(application: Application):
    """Инициализация после запуска бота"""
    logger.info("🚀 Инициализация бота...")
    
    # Первоначальное обновление кэша
    logger.info("🔍 Первоначальное обновление кэша канала...")
    success = await cache_manager.update_cache()
    
    if success:
        logger.info("✅ Кэш успешно инициализирован")
    else:
        logger.warning("⚠️ Не удалось инициализировать кэш. Будет использован резервный список.")
    
    # Запуск фоновой задачи
    asyncio.create_task(background_update_task(application))
    
    logger.info("✅ Бот инициализирован")

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработчик ошибок"""
    error = context.error
    logger.error(f"Ошибка: {error}", exc_info=error)

def main():
    """Запуск бота"""
    print("=" * 70)
    print("🤖 ТЕЛЕГРАМ-БОТ С АВТОПАРСИНГОМ КАНАЛА")
    print("=" * 70)
    print(f"📢 Канал для парсинга: {CHANNEL_USERNAME}")
    print(f"⏰ Обновление топ-постов: раз в {UPDATE_INTERVAL_WEEKS} неделю")
    print(f"📊 Лимит постов для анализа: {POSTS_LIMIT}")
    print("=" * 70)
    
    try:
        # Создаем приложение
        app = Application.builder() \
            .token(TOKEN) \
            .connect_timeout(30.0) \
            .read_timeout(30.0) \
            .write_timeout(30.0) \
            .pool_timeout(30.0) \
            .post_init(post_init) \
            .build()
        
        # Добавляем обработчик ошибок
        app.add_error_handler(error_handler)
        
        # Команды
        app.add_handler(CommandHandler("start", start))
        
        # Обработчики кнопок
        app.add_handler(CallbackQueryHandler(start, pattern="^back_to_start$"))
        app.add_handler(CallbackQueryHandler(main_course, pattern="^main_course$"))
        app.add_handler(CallbackQueryHandler(consultation, pattern="^consultation$"))
        app.add_handler(CallbackQueryHandler(top_posts, pattern="^top_posts$"))
        app.add_handler(CallbackQueryHandler(refresh_top_posts, pattern="^refresh_top$"))
        app.add_handler(CallbackQueryHandler(channel_stats, pattern="^channel_stats$"))
        app.add_handler(CallbackQueryHandler(fallback_posts, pattern="^fallback_posts$"))
        app.add_handler(CallbackQueryHandler(copy_email, pattern="^copy_email$"))
        app.add_handler(CallbackQueryHandler(how_to_email, pattern="^how_to_email$"))
        
        # Обработчик сообщений
        app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, handle_message))
        
        print("✅ Все обработчики зарегистрированы")
        print("=" * 70)
        print("🚀 Бот запущен. Нажмите Ctrl+C для остановки.")
        print("=" * 70)
        
        # Запускаем бота
        app.run_polling(
            allowed_updates=Update.ALL_TYPES,
            drop_pending_updates=True,
            poll_interval=0.5,
            timeout=20
        )
        
    except KeyboardInterrupt:
        print("\n👋 Остановка бота...")
    except Exception as e:
        print(f"\n💥 Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    # Создаем директорию для кэша, если нет
    cache_dir = os.path.dirname(CACHE_FILE)
    if cache_dir and not os.path.exists(cache_dir):
        os.makedirs(cache_dir, exist_ok=True)
    
    main()