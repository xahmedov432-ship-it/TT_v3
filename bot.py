import asyncio
import contextlib
import logging
import random
import json
import time
from urllib.parse import quote
import os
import re
import csv
import io
import glob
import requests
import aiosqlite
from datetime import datetime

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, Router, F, BaseMiddleware
from aiogram.filters import Command, StateFilter
from aiogram.types import Message, BufferedInputFile, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, ReplyKeyboardMarkup, KeyboardButton
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from playwright.async_api import async_playwright
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# ================= КОНФИГУРАЦИЯ (из .env) =================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = [
    int(os.getenv("ADMIN_ID_1", "0")),
    int(os.getenv("ADMIN_ID_2", "0")),
]
ADMIN_IDS = [aid for aid in ADMIN_IDS if aid != 0]
ADMIN_ID = ADMIN_IDS[0] if ADMIN_IDS else 0

ADSPOWER_API_PORT = os.getenv("ADSPOWER_API_PORT", "50325")
DB_NAME = os.getenv("DB_NAME", "tiktok_bot.db")
ASOCKS_CHANGE_IP_LINK = os.getenv("ASOCKS_CHANGE_IP_LINK", "")
LIMIT_PER_HOUR = int(os.getenv("LIMIT_PER_HOUR", "10"))
LIMIT_PER_DAY = int(os.getenv("LIMIT_PER_DAY", "30"))

# ================= ГЛОБАЛЬНЫЕ СТАТУСЫ =================
active_statuses = {}

# Замена глобального флага на asyncio.Lock (потокобезопасно)
shadow_check_lock = asyncio.Lock()

# Кэш api_key с TTL 60 секунд (избегаем лишних запросов к БД)
_api_key_cache: dict = {"value": None, "ts": 0.0}

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
router = Router()
dp.include_router(router)


async def send_to_all_admins(text: str, parse_mode: str = "Markdown", reply_markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, text, parse_mode=parse_mode, reply_markup=reply_markup)
        except Exception as e:
            logging.error(f"Не удалось отправить уведомление админу {admin_id}: {e}")



# ================= МИДЛВАРЬ АВТОРИЗАЦИИ =================
class AdminOnlyMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        user = data.get("event_from_user")
        if user and user.id not in ADMIN_IDS:
            if isinstance(event, Message):
                await event.answer("❌ У вас нет доступа к этому боту.")
            elif isinstance(event, CallbackQuery):
                await event.answer("❌ Доступ ограничен.", show_alert=True)
            return
        return await handler(event, data)

router.message.outer_middleware(AdminOnlyMiddleware())
router.callback_query.outer_middleware(AdminOnlyMiddleware())
scheduler = AsyncIOScheduler()

# ================= FSM =================
class BotStates(StatesGroup):
    waiting_for_project_name = State()
    waiting_for_account_data = State()
    waiting_for_search_data = State()
    waiting_for_grab_data = State()
    waiting_for_foryou_data = State()
    waiting_for_boost_data = State()
    waiting_for_masslike_data = State()
    waiting_for_dialogue_data = State()
    waiting_for_revisor_data = State()
    waiting_for_ban_username = State()
    waiting_for_unban_username = State()
    waiting_for_api_key = State()

# ================= БАЗА ДАННЫХ =================
async def init_db():
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute('''CREATE TABLE IF NOT EXISTS projects (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            name TEXT UNIQUE,
                            status TEXT DEFAULT 'active'
                        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS accounts (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            project_name TEXT,
                            name TEXT UNIQUE,
                            session_file TEXT,
                            proxy TEXT,
                            status TEXT DEFAULT 'active',
                            tiktok_nickname TEXT
                        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS tasks (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            project_name TEXT,
                            video_url TEXT,
                            template_text TEXT,
                            status TEXT DEFAULT 'pending',
                            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                        )''')

        await db.execute('''CREATE TABLE IF NOT EXISTS task_history (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            account_name TEXT,
                            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS blacklist (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            username TEXT UNIQUE
                        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS dialogues (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            project_name TEXT,
                            video_url TEXT,
                            comment_text TEXT,
                            reply_text TEXT,
                            delay_minutes INTEGER,
                            status TEXT DEFAULT 'pending_comment',
                            first_comment_time DATETIME
                        )''')
        await db.execute('''CREATE TABLE IF NOT EXISTS settings (
                            key TEXT PRIMARY KEY,
                            value TEXT
                        )''')
        await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('ads_api_key', 'ed2a1b2b2fbcc9e818de71d5f286aeb0008c34ec73fa1418')")
        # --- Миграции (выполняются один раз при старте, не в планировщике) ---
        try:
            await db.execute("ALTER TABLE accounts ADD COLUMN tiktok_nickname TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE tasks ADD COLUMN created_at DATETIME DEFAULT CURRENT_TIMESTAMP")
        except:
            pass
        try:
            await db.execute("ALTER TABLE dialogues ADD COLUMN first_account TEXT")
        except:
            pass
        try:
            await db.execute("ALTER TABLE dialogues ADD COLUMN first_comment_id TEXT")
        except:
            pass

        # --- Уникальный индекс для дедупликации задач (#9) ---
        await db.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS idx_tasks_project_url "
            "ON tasks(project_name, video_url)"
        )

        await db.commit()


async def get_api_key() -> str:
    """Возвращает api_key из БД с кэшированием на 60 секунд (#8)."""
    now = time.monotonic()
    if _api_key_cache["value"] is not None and now - _api_key_cache["ts"] < 60:
        return _api_key_cache["value"]
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT value FROM settings WHERE key='ads_api_key'")
        row = await cursor.fetchone()
    value = row[0] if row else ""
    _api_key_cache["value"] = value
    _api_key_cache["ts"] = now
    return value



# ================= ИНТЕРФЕЙС (КЛАВИАТУРЫ) =================
def kb_main_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🚀 Задачи и Трафик", callback_data="menu_tasks")],
        [InlineKeyboardButton(text="⚙️ Проекты и Аккаунты", callback_data="menu_manage")],
        [InlineKeyboardButton(text="📊 Статистика и Контроль", callback_data="menu_stats")]
    ])

def kb_tasks_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Спарсить Поиск (В работу)", callback_data="ask_search"),
         InlineKeyboardButton(text="🧲 Спарсить Поиск (.txt)", callback_data="ask_grab")],
        [InlineKeyboardButton(text="🌟 Спарсить Рекомендации (.txt)", callback_data="ask_foryou")],
        [InlineKeyboardButton(text="🚀 Вывод в ТОП (Boost)", callback_data="ask_boost"),
         InlineKeyboardButton(text="❤️ Масс-лайкинг", callback_data="ask_masslike")],
        [InlineKeyboardButton(text="🗣 Запустить Диалог", callback_data="ask_dialogue")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
    ])

def kb_manage_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Мои проекты", callback_data="list_projects"),
         InlineKeyboardButton(text="➕ Создать проект", callback_data="ask_create_project")],
        [InlineKeyboardButton(text="👥 Мои аккаунты", callback_data="list_accounts"),
         InlineKeyboardButton(text="👤 Добавить аккаунт", callback_data="ask_add_account")],
        [InlineKeyboardButton(text="🔑 Настроить API AdsPower", callback_data="ask_api_key")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
    ])

def kb_stats_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📈 Статус фермы", callback_data="action_status"),
         InlineKeyboardButton(text="🕵️ Ревизор (Проверка)", callback_data="ask_revisor")],
        [InlineKeyboardButton(text="🛡️ Тест теневого бана", callback_data="action_check_shadow_ban"),
         InlineKeyboardButton(text="📥 Выгрузить отчет", callback_data="action_export")],
        [InlineKeyboardButton(text="🚫 Управление ЧС", callback_data="menu_blacklist"),
         InlineKeyboardButton(text="🧹 Очистить очередь", callback_data="action_clear_queue")],
        [InlineKeyboardButton(text="⬅️ Назад в меню", callback_data="menu_main")]
    ])

def kb_blacklist_menu():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Добавить в ЧС", callback_data="ask_ban"),
         InlineKeyboardButton(text="➖ Удалить из ЧС", callback_data="ask_unban")],
        [InlineKeyboardButton(text="📜 Показать ЧС", callback_data="action_show_blacklist")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data="menu_stats")]
    ])

def kb_dynamic_list(items, prefix, back_callback):
    buttons = []
    for item in items:
        buttons.append([InlineKeyboardButton(text=f"📌 {item}", callback_data=f"{prefix}_{item}")])
    buttons.append([InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)])
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def kb_delete_item(item_type, item_name, back_callback):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑 Удалить {item_name}", callback_data=f"del_{item_type}_{item_name}")],
        [InlineKeyboardButton(text="⬅️ Назад", callback_data=back_callback)]
    ])



# ================= ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =================
def spin_text(text: str) -> str:
    while True:
        match = re.search(r'\{([^{}]*)\}', text)
        if not match:
            break
        options = match.group(1).split('|')
        choice = random.choice(options)
        text = text[:match.start()] + choice + text[match.end():]
    return text

def parse_view_count(view_str: str) -> int:
    try:
        view_str = view_str.upper().strip().replace(',', '.')
        if 'M' in view_str or 'М' in view_str:
            return int(float(view_str.replace('M', '').replace('М', '')) * 1000000)
        elif 'K' in view_str or 'К' in view_str:
            return int(float(view_str.replace('K', '').replace('К', '')) * 1000)
        else:
            return int(float(view_str))
    except:
        return 0

def is_valid_tiktok_video_url(url: str) -> bool:
    if not url:
        return False
    pattern = r'https?://(www\.)?tiktok\.com/@[\w\.-]+/video/\d+'
    return bool(re.match(pattern, url))

def add_comment_id_to_url(url: str, comment_id: str | None) -> str:
    if not comment_id:
        return url
    clean_url = url.split('#')[0]
    separator = '&' if '?' in clean_url else '?'
    if 'comment_id=' in clean_url:
        clean_url = re.sub(r'comment_id=[^&]+', f'comment_id={comment_id}', clean_url)
        return clean_url
    return f"{clean_url}{separator}comment_id={comment_id}"

def extract_comment_id_from_json(data):
    if data is None:
        return None
    priority_keys = ('comment_id', 'commentId', 'cid', 'reply_id', 'replyId',
                     'commentIdStr', 'id_str', 'id', 'aweme_id')
    def looks_like_comment_id(value):
        value = str(value).strip()
        return value.isdigit() and len(value) >= 10
    def walk(obj):
        if isinstance(obj, dict):
            for key in priority_keys:
                value = obj.get(key)
                if value is not None and looks_like_comment_id(value):
                    return str(value)
            for value in obj.values():
                found = walk(value)
                if found:
                    return found
        elif isinstance(obj, list):
            for value in obj:
                found = walk(value)
                if found:
                    return found
        return None
    return walk(data)

async def rotate_asocks_ip(account_name):
    if ASOCKS_CHANGE_IP_LINK:
        logging.info(f"[{account_name}] 🔄 Отправляем команду ASocks на смену IP...")
        try:
            requests.get(ASOCKS_CHANGE_IP_LINK, timeout=10)
            logging.info(f"[{account_name}] ⏳ Ждем 15 секунд...")
            await asyncio.sleep(15)
        except Exception as e:
            logging.error(f"[{account_name}] ❌ Ошибка смены IP: {e}")

async def send_error_to_tg(error_msg: str, account_name: str = "SYSTEM"):
    try:
        text = (
            f"⚠️ **ОШИБКА В РАБОТЕ БОТА**\n\n"
            f"👤 **Аккаунт:** `{account_name}`\n"
            f"❌ **Детали:**\n`{error_msg}`"
        )
        await send_to_all_admins(text)
    except Exception as e:
        logging.error(f"Не удалось отправить лог в TG: {e}")


# ================= КОНТЕКСТНЫЙ МЕНЕДЖЕР ADSPOWER (#6) =================
@contextlib.asynccontextmanager
async def adspower_profile(adspower_id: str, account_name: str):
    """
    Гарантирует закрытие AdsPower-профиля даже при исключениях.
    Использование:
        async with adspower_profile(adspower_id, account_name) as ws_endpoint:
            ...
    """
    api_key = await get_api_key()
    open_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/start?user_id={adspower_id}&api_key={api_key}"
    try:
        resp = requests.get(open_url, timeout=10).json()
        if resp.get("code") != 0:
            raise RuntimeError(f"AdsPower ошибка: {resp.get('msg')}")
        ws_endpoint = resp["data"]["ws"]["puppeteer"]
    except Exception as e:
        raise RuntimeError(f"Не удалось запустить профиль {adspower_id}: {e}")

    try:
        yield ws_endpoint
    finally:
        try:
            api_key_stop = await get_api_key()
            stop_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/stop?user_id={adspower_id}&api_key={api_key_stop}"
            requests.get(stop_url, timeout=10)
            logging.info(f"[{account_name}] 🛑 Профиль AdsPower закрыт.")
        except Exception as e:
            logging.debug(f"[{account_name}] Ошибка закрытия профиля: {e}")



# ================= ОЖИДАНИЕ ЗАГРУЗКИ СТРАНИЦ =================
async def is_captcha_page(page, account_name) -> bool:
    """Проверяет, показана ли на странице капча или верификация TikTok."""
    try:
        captcha_signals = await page.evaluate(r'''() => {
            const bodyText = (document.body?.innerText || document.body?.textContent || "").toLowerCase();
            const captchaKeywords = [
                "verify", "verification", "captcha", "robot", "human",
                "проверка", "верификация", "подтвердите", "я не робот",
                "tiktok_verify", "are you a human", "security check",
                "slide to verify", "puzzlecaptcha", "verifypage"
            ];
            const hasCaptchaText = captchaKeywords.some(kw => bodyText.includes(kw));
            const captchaSelectors = [
                '#captcha-verify-image',
                'div[class*="captcha"]',
                'div[class*="verify"]',
                'canvas[id*="captcha"]',
                'div[id*="captcha"]',
                '.captcha_verify_bar',
                '.secsdk-captcha-drag-icon',
                'div[class*="VerifyPage"]',
                'div[class*="Captcha"]',
                'iframe[src*="captcha"]',
                'iframe[src*="verify"]',
            ];
            const hasCaptchaEl = captchaSelectors.some(sel => !!document.querySelector(sel));
            const title = document.title.toLowerCase();
            const hasCaptchaTitle = title.includes("verify") || title.includes("captcha") || title.includes("проверка");
            return hasCaptchaText || hasCaptchaEl || hasCaptchaTitle;
        }''')
        if captcha_signals:
            logging.warning(f"[{account_name}] 🛡️ Обнаружена капча / верификация!")
        return bool(captcha_signals)
    except Exception as e:
        logging.debug(f"[{account_name}] Ошибка проверки капчи: {e}")
        return False


async def wait_for_search_page_ready(page, account_name, timeout_seconds=60):
    logging.info(f"[{account_name}] ⏳ Ждём загрузки ленты с видео (до {timeout_seconds}с)...")
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < timeout_seconds:
        try:
            count = await page.evaluate('''() => {
                let links = document.querySelectorAll('a[href*="/video/"]').length;
                let cards = document.querySelectorAll('[data-e2e="recommend-list-item-container"], [data-e2e="challenge-item"], [data-e2e="search_top-item"], [data-e2e="search_video-item"], div[class*="DivItemContainer"], div[class*="DivVideoFeedItem"], video').length;
                return links + cards;
            }''')
            if count > 0:
                logging.info(f"[{account_name}] ✅ Лента загружена! Элементов: {count}")
                return True
        except:
            pass
        await page.wait_for_timeout(2000)
    logging.warning(f"[{account_name}] ⚠️ Лента не загрузилась за {timeout_seconds}с.")
    return False


async def wait_for_video_page_ready(page, account_name, timeout_seconds=60):
    logging.info(f"[{account_name}] ⏳ Ждём полной загрузки страницы видео (до {timeout_seconds}с)...")
    video_selectors = [
        'video',
        '[data-e2e="browse-video"]',
        '[data-e2e="video-player"]',
        'div[class*="DivVideoContainer"]',
    ]
    comment_ready_selectors = [
        '[data-e2e="comment-icon"]',
        'span[data-e2e="comment-icon"]',
        '[data-e2e="browse-comment-icon"]',
        '[data-e2e="comment-level-1"]',
        'div[data-e2e="comment-input"]',
    ]
    start = asyncio.get_event_loop().time()
    video_found = False
    comments_ready = False
    while asyncio.get_event_loop().time() - start < timeout_seconds:
        if not video_found:
            for sel in video_selectors:
                try:
                    if await page.locator(sel).first.is_visible(timeout=2000):
                        video_found = True
                        logging.info(f"[{account_name}] ✅ Видео-плеер найден ({sel})")
                        break
                except:
                    pass
        if not comments_ready:
            for sel in comment_ready_selectors:
                try:
                    if await page.locator(sel).first.is_visible(timeout=2000):
                        comments_ready = True
                        logging.info(f"[{account_name}] ✅ Кнопка комментариев готова ({sel})")
                        break
                except:
                    pass
        if video_found and comments_ready:
            elapsed = asyncio.get_event_loop().time() - start
            logging.info(f"[{account_name}] ✅ Страница полностью загружена за {elapsed:.1f}с")
            return True
        await page.wait_for_timeout(2000)
    elapsed = asyncio.get_event_loop().time() - start
    logging.warning(f"[{account_name}] ⚠️ Страница не загрузилась за {elapsed:.1f}с")
    return video_found



# ================= СКРОЛЛ ПАНЕЛИ КОММЕНТОВ =================
async def scroll_comments_panel(page, account_name, pixels=400):
    try:
        hover_success = False
        hover_selectors = [
            '[data-e2e="comment-list"]',
            'div[class*="CommentListContainer"]',
            'div[class*="comment-list"]',
            'div[class*="DivCommentListContainer"]',
            'div[class*="CommentContainer"]',
            'div[class*="DivCommentContainer"]',
            '[data-e2e="comment-level-1"]',
            'div[class*="CommentItem"]',
            'div[class*="DivCommentNode"]'
        ]
        for sel in hover_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible():
                    box = await loc.bounding_box()
                    if box:
                        await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                        await page.wait_for_timeout(100)
                        await page.mouse.wheel(0, pixels)
                        hover_success = True
                        break
            except Exception as e:
                logging.debug(f"[{account_name}] Ошибка нативного скролла через {sel}: {e}")
                continue

        await page.evaluate(f"""() => {{
            const fireScrollEvent = (element) => {{
                if (!element) return;
                const event = new Event('scroll', {{ bubbles: true, cancelable: true }});
                element.dispatchEvent(event);
                const uiEvent = document.createEvent('UIEvents');
                uiEvent.initUIEvent('scroll', true, true, window, 1);
                element.dispatchEvent(uiEvent);
            }};
            let selectors = [
                '[data-e2e="comment-list"]',
                'div[class*="CommentListContainer"]',
                'div[class*="comment-list"]',
                'div[class*="DivCommentListContainer"]',
                'div[class*="CommentContainer"]',
                'div[class*="DivCommentContainer"]'
            ];
            for (let sel of selectors) {{
                let el = document.querySelector(sel);
                if (el) {{
                    el.scrollTop += {pixels};
                    fireScrollEvent(el);
                    setTimeout(() => {{ el.scrollTop = el.scrollHeight; fireScrollEvent(el); }}, 50);
                }}
            }}
            let firstComment = document.querySelector('[data-e2e="comment-level-1"], [data-e2e="comment-level-2"], div[class*="CommentItem"], div[class*="DivCommentNode"]');
            if (firstComment) {{
                let parent = firstComment.parentElement;
                while (parent && parent !== document.body) {{
                    let style = window.getComputedStyle(parent);
                    if (style.overflowY === 'auto' || style.overflowY === 'scroll' || parent.scrollHeight > parent.clientHeight) {{
                        parent.scrollTop += {pixels};
                        fireScrollEvent(parent);
                        setTimeout(() => {{ parent.scrollTop = parent.scrollHeight; fireScrollEvent(parent); }}, 50);
                        break;
                    }}
                    parent = parent.parentElement;
                }}
            }}
            window.scrollBy(0, {pixels});
            window.scrollTo(0, document.body.scrollHeight);
            fireScrollEvent(window);
        }}""")
        logging.info(f"[{account_name}] 📜 Скролл панели комментов (Native: {hover_success}, JS: True).")
        return True
    except Exception as e:
        logging.warning(f"[{account_name}] ⚠️ Не удалось прокрутить панель: {e}")
        return False



# ================= ОСНОВНОЙ ВОРКЕР =================
async def tiktot_worker(account_name, adspower_id, project_name, video_url, template_text, reply_to_text=None, reply_to_comment_id=None, skip_like=False, reply_to_author_nickname=None):
    logging.info(f"[{account_name}] Попытка запуска профиля: {adspower_id}")
    active_statuses[account_name] = f"Запуск профиля AdsPower в проекте '{project_name}'..."

    try:
        async with adspower_profile(adspower_id, account_name) as ws_endpoint:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(ws_endpoint)
                context = browser.contexts[0]
                page = await context.new_page()

                open_video_url = add_comment_id_to_url(video_url, reply_to_comment_id) if reply_to_comment_id else video_url
                logging.info(f"[{account_name}] Переход к видео: {open_video_url}")
                active_statuses[account_name] = f"Подключение к браузеру, открытие видео {open_video_url[:35]}..."


            goto_success = False
            for goto_attempt in range(1, 4):
                try:
                    logging.info(f"[{account_name}] 🌐 Попытка открытия страницы {goto_attempt}/3")
                    await page.goto(open_video_url, wait_until="domcontentloaded", timeout=60000)
                    goto_success = True
                    logging.info(f"[{account_name}] ✅ Страница открылась успешно")
                    break
                except Exception as e:
                    err_text = str(e)
                    logging.warning(f"[{account_name}] ⚠️ Ошибка открытия страницы {goto_attempt}/3: {err_text[:180]}")
                    if "ERR_SOCKS_CONNECTION_FAILED" in err_text or "ERR_PROXY_CONNECTION_FAILED" in err_text or "ERR_TUNNEL_CONNECTION_FAILED" in err_text:
                        logging.info(f"[{account_name}] ⏳ Прокси/сеть ещё не готова. Ждём 20 секунд...")
                        await page.wait_for_timeout(20000)
                    else:
                        logging.info(f"[{account_name}] ⏳ Ждём 10 секунд перед повтором...")
                        await page.wait_for_timeout(10000)
                    try:
                        await page.goto("about:blank", wait_until="domcontentloaded", timeout=15000)
                    except:
                        pass

            if not goto_success:
                await send_error_to_tg(f"Не удалось открыть видео после 3 попыток: {open_video_url}", account_name)
                return False

            active_statuses[account_name] = "Ожидание загрузки видео..."
            is_ready = await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
            if not is_ready:
                logging.warning(f"[{account_name}] 🔄 Страница не загрузилась за 60 секунд! Обновляем...")
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)
                is_ready_again = await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
                if not is_ready_again:
                    logging.error(f"[{account_name}] ❌ Страница так и не загрузилась после обновления.")
                    return False

            await page.wait_for_timeout(random.randint(3000, 5000))

            login_visible = False
            for login_sel in ['button[data-e2e="top-login-button"]', 'div[id="login-modal"]', 'a[href*="login"]']:
                try:
                    if await page.locator(login_sel).first.is_visible(timeout=2000):
                        login_visible = True
                        break
                except:
                    pass

            if login_visible:
                logging.warning(f"[{account_name}] 🔄 Просит логин. Обновляем...")
                active_statuses[account_name] = "Обнаружено требование авторизации. Обновление страницы..."
                await page.reload(wait_until="domcontentloaded")
                await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
                await page.wait_for_timeout(5000)
                for login_sel in ['button[data-e2e="top-login-button"]', 'div[id="login-modal"]']:
                    try:
                        if await page.locator(login_sel).first.is_visible(timeout=2000):
                            active_statuses[account_name] = "Сессия истекла! Требуется повторный вход."
                            await send_error_to_tg("Слетела сессия!", account_name)
                            return False
                    except:
                        pass

            if not skip_like:
                logging.info(f"[{account_name}] ⏩ Лайк на видео пропущен (по умолчанию).")
            else:
                logging.info(f"[{account_name}] ⏩ Лайк на видео пропущен (skip_like=True)")



            # ================= ВСПОМОГАТЕЛЬНАЯ: ОТКРЫТИЕ ПАНЕЛИ КОММЕНТОВ =================
            async def ensure_panel_open():
                async def comments_are_open():
                    open_selectors = [
                        '[data-e2e="comment-level-1"]',
                        'div[data-e2e="comment-input"]',
                        'div[role="textbox"][contenteditable="true"]',
                        '.public-DraftEditor-content',
                        '.DraftEditor-root',
                        '[data-e2e="comment-post"]',
                    ]
                    for sel in open_selectors:
                        try:
                            loc = page.locator(sel).first
                            if await loc.count() > 0 and await loc.is_visible(timeout=1200):
                                return True
                        except:
                            pass
                    return False

                if await comments_are_open():
                    logging.info(f"[{account_name}] ✅ Панель комментариев уже открыта")
                    return True

                async def close_popups():
                    popup_selectors = [
                        'div[data-e2e="modal-close-inner-button"]',
                        'button:has-text("Not now")',
                        'button:has-text("Не сейчас")',
                        'button:has-text("Decline all")',
                    ]
                    for psel in popup_selectors:
                        try:
                            btn = page.locator(psel).first
                            if await btn.count() > 0 and await btn.is_visible(timeout=700):
                                await btn.click(force=True, timeout=1500)
                                await page.wait_for_timeout(800)
                        except:
                            pass

                await close_popups()
                active_statuses[account_name] = "Открытие панели комментариев..."
                logging.info(f"[{account_name}] 🔎 Открываем комментарии через иконку...")

                comment_icon_selectors = [
                    'span[data-e2e="comment-icon"]',
                    '[data-e2e="comment-icon"]',
                    '[data-e2e="browse-comment-icon"]',
                    'button[aria-label*="comment" i]',
                    'button[aria-label*="comments" i]',
                    'button[aria-label*="коммент" i]',
                    'div[role="button"][aria-label*="comment" i]',
                    'div[role="button"][aria-label*="коммент" i]',
                ]

                for sel in comment_icon_selectors:
                    try:
                        target = page.locator(sel).first
                        if await target.count() == 0:
                            continue
                        await target.scroll_into_view_if_needed(timeout=3000)
                        await page.wait_for_timeout(700)
                        clicked = False
                        try:
                            box = await target.bounding_box()
                            if box:
                                await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                await page.wait_for_timeout(300)
                                await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                clicked = True
                        except:
                            pass
                        if not clicked:
                            try:
                                await target.click(force=True, timeout=3000)
                                clicked = True
                            except:
                                pass
                        if not clicked:
                            try:
                                await target.evaluate("""node => {
                                    const clickable = node.closest('button, div[role=button], span[role=button], a') || node;
                                    clickable.click();
                                }""")
                                clicked = True
                            except:
                                pass
                        if clicked:
                            await page.wait_for_timeout(4500)
                            if await comments_are_open():
                                logging.info(f"[{account_name}] ✅ Комментарии открылись через selector: {sel}")
                                return True
                    except Exception as e:
                        logging.debug(f"[{account_name}] Не смогли открыть через {sel}: {e}")
                        continue

                logging.info(f"[{account_name}] 🔎 Пробуем открыть комментарии клавишей 'c'...")
                try:
                    await page.keyboard.press('c')
                    await page.wait_for_timeout(4500)
                    if await comments_are_open():
                        logging.info(f"[{account_name}] ✅ Комментарии открылись через клавишу 'c'")
                        return True
                except:
                    pass

                logging.info(f"[{account_name}] 🔎 Последняя попытка: JS-поиск иконки комментариев...")
                try:
                    clicked_by_js = await page.evaluate("""() => {
                        const all = Array.from(document.querySelectorAll('button, div[role=button], span[role=button], p[role=button], a'));
                        const el = all.find(e => {
                            const txt = (e.innerText || e.getAttribute('aria-label') || '').toLowerCase();
                            return txt.includes('comment') || txt.includes('коммент');
                        });
                        if (!el) return false;
                        el.click();
                        return true;
                    }""")
                    if clicked_by_js:
                        await page.wait_for_timeout(4500)
                        if await comments_are_open():
                            logging.info(f"[{account_name}] ✅ Комментарии открылись через JS-поиск")
                            return True
                except:
                    pass

                logging.error(f"[{account_name}] ❌ Не удалось открыть панель комментариев")
                return False



            # ================= РЕЖИМ МАСС-ЛАЙКИНГА =================
            _is_masslike = template_text.startswith("[MASSLIKE]") or template_text.startswith("[MASS_LIKE:")

            if _is_masslike:
                try:
                    raw = template_text.replace("[MASSLIKE]", "").replace("[MASS_LIKE:", "").rstrip("]").strip()
                    target_likes = int(raw)
                except:
                    target_likes = 5

                active_statuses[account_name] = f"Масс-лайкинг комментариев (цель: {target_likes} лайков)..."
                logging.info(f"[{account_name}] 🔥 Масс-лайкинг! Цель: {target_likes}")
                await page.wait_for_timeout(5000)

                if not await ensure_panel_open():
                    return False
                await page.wait_for_timeout(3000)

                likes_done = 0
                no_new_comments_streak = 0

                for attempt in range(20):
                    if likes_done >= target_likes:
                        break
                    prev_likes = likes_done

                    like_btns = await page.locator('div[role="button"][aria-pressed="false"][class*="LikeContainer"], div[role="button"][aria-pressed="false"][aria-label*="Like"], div[role="button"][aria-pressed="false"][aria-label*="Нравится"]').all()
                    logging.info(f"[{account_name}] 💬 Вижу на экране нелайкнутых кнопок: {len(like_btns)}")

                    for btn in like_btns:
                        if likes_done >= target_likes:
                            break
                        try:
                            if await btn.is_visible(timeout=1000):
                                await btn.scroll_into_view_if_needed(timeout=2000)
                                await page.wait_for_timeout(500)
                                await btn.click(force=True, delay=150)
                                likes_done += 1
                                active_statuses[account_name] = f"Масс-лайкинг: поставлено {likes_done}/{target_likes} лайков..."
                                logging.info(f"[{account_name}] ❤️ Лайк поставлен ({likes_done}/{target_likes})")
                                await page.wait_for_timeout(random.randint(1500, 3000))
                        except Exception as e:
                            logging.debug(f"Не смогли кликнуть (пропускаем): {e}")
                            continue

                    if likes_done == prev_likes:
                        no_new_comments_streak += 1
                        if no_new_comments_streak >= 3:
                            logging.info(f"[{account_name}] 🛑 Конец списка комментариев. Лайкнуто {likes_done} из {target_likes}.")
                            break
                    else:
                        no_new_comments_streak = 0

                    if likes_done < target_likes:
                        active_statuses[account_name] = f"Масс-лайкинг: скроллинг комментариев ({likes_done}/{target_likes})..."
                        await scroll_comments_panel(page, account_name, pixels=1200)
                        await page.wait_for_timeout(random.randint(3000, 5000))

                logging.info(f"[{account_name}] ✅ Масс-лайкинг завершен. Поставлено: {likes_done}/{target_likes}")
                return True



            # ================= РЕЖИМ BOOST =================
            elif template_text.startswith("[BOOST]"):
                raw_boost = template_text.replace("[BOOST]", "").strip()
                if "|" in raw_boost:
                    target_nick = raw_boost.split("|")[0].strip().replace("@", "")
                    target_text = raw_boost.split("|")[1].strip()
                else:
                    target_nick = ""
                    target_text = raw_boost

                active_statuses[account_name] = f"Вывод в ТОП (Boost): Ник='{target_nick}', Текст='{target_text}'..."
                logging.info(f"[{account_name}] 🚀 Вывод в ТОП: Ник='{target_nick}', Текст='{target_text}'")
                await page.wait_for_timeout(8000)

                if not await ensure_panel_open():
                    return False
                await page.wait_for_timeout(3000)

                found = False
                search_query = target_nick if target_nick else target_text
                logging.info(f"[{account_name}] 🔍 Ctrl+F: ищем '{search_query}'")
                await page.keyboard.press('Control+f')
                await page.wait_for_timeout(1500)
                await page.keyboard.type(search_query, delay=50)
                await page.wait_for_timeout(2000)
                await page.keyboard.press('Escape')
                await page.wait_for_timeout(500)

                for attempt in range(20):
                    if found:
                        break
                    active_statuses[account_name] = f"Boost: поиск комментария (попытка {attempt + 1}/20)..."
                    comments = await page.locator('[data-e2e="comment-level-1"], [data-e2e="comment-level-2"], div[class*="DivCommentNode"], div[class*="CommentItem"], div[class*="comment-item"]').all()
                    for comment_block in comments:
                        try:
                            content_lower = await comment_block.evaluate("node => (node.textContent || '').toLowerCase()")
                            content_nospace = content_lower.replace(" ", "").replace("\\n", "").replace("\\r", "")
                            nick_query = target_nick.lower().replace(" ", "")
                            text_query = target_text.lower().replace(" ", "")
                            nick_match = True if not nick_query else (nick_query in content_nospace)
                            text_match = text_query in content_nospace
                            if nick_match and text_match:
                                logging.info(f"[{account_name}] ✅ Блок найден! Ищем кнопку лайка...")
                                like_status = await comment_block.evaluate("""node => {
                                    let elements = Array.from(node.querySelectorAll('div[role="button"], button, span, svg'));
                                    for (let el of elements) {
                                        let cls = (el.className || '').toString().toLowerCase();
                                        let aria = (el.getAttribute('aria-label') || '').toLowerCase();
                                        let e2e = (el.getAttribute('data-e2e') || '').toLowerCase();
                                        if (cls.includes('dislike') || aria.includes('dislike') || e2e.includes('dislike')) continue;
                                        if (cls.includes('reply') || aria.includes('reply') || e2e.includes('reply')) continue;
                                        if (cls.includes('like') || aria.includes('like') || aria.includes('нравится') || e2e.includes('like-icon') || (el.tagName === 'svg' && el.closest('div[role="button"]'))) {
                                            let parent = el.closest('div[role="button"]') || el;
                                            if (parent.getAttribute('aria-pressed') === 'true' || cls.includes('liked')) {
                                                return 'ALREADY_LIKED';
                                            }
                                            parent.scrollIntoView({block: 'center', behavior: 'smooth'});
                                            parent.click();
                                            return 'CLICKED';
                                        }
                                    }
                                    return 'NOT_FOUND';
                                }""")
                                if like_status == 'ALREADY_LIKED':
                                    logging.info(f"[{account_name}] ⚠️ Лайк на этот коммент УЖЕ СТОИТ!")
                                    found = True
                                    break
                                elif like_status == 'CLICKED':
                                    active_statuses[account_name] = "Boost: ставим лайк на целевой комментарий..."
                                    logging.info(f"[{account_name}] ✅ Лайк на свой коммент поставлен!")
                                    found = True
                                    await page.wait_for_timeout(2000)
                                    break
                                else:
                                    logging.warning(f"[{account_name}] ❌ Кнопка лайка не найдена внутри найденного блока!")
                        except:
                            continue
                    if found:
                        break
                    active_statuses[account_name] = f"Boost: скроллинг комментариев (попытка {attempt + 1}/20)..."
                    await scroll_comments_panel(page, account_name, pixels=1200)
                    await page.wait_for_timeout(2500)

                if not found:
                    logging.error(f"[{account_name}] ❌ Не смогли найти или лайкнуть коммент.")
                    return False
                return True



            # ================= РЕЖИМ ОБЫЧНОГО КОММЕНТАРИЯ / ДИАЛОГА =================
            else:
                captured_comment_id = None
                response_listener_active = True

                async def intercept_comment_publish_response(response):
                    nonlocal captured_comment_id
                    if not response_listener_active or captured_comment_id:
                        return
                    try:
                        url_l = response.url.lower()
                        if not any(x in url_l for x in ['comment', 'reply', 'commit']):
                            return
                        data = await response.json()
                        cid = extract_comment_id_from_json(data)
                        if cid:
                            captured_comment_id = cid
                            logging.info(f"[{account_name}] 🧷 Поймали comment_id из API: {captured_comment_id}")
                    except:
                        pass

                page.on("response", intercept_comment_publish_response)

                async def try_click_reply_in_block(comment_block, source_label):
                    try:
                        await comment_block.scroll_into_view_if_needed(timeout=3000)
                        await page.wait_for_timeout(800)
                        reply_target = comment_block.locator('[data-e2e="comment-reply-1"], [data-e2e*="reply-btn"]').first
                        if await reply_target.count() > 0:
                            logging.info(f"[{account_name}] Найдена явная кнопка Reply в блоке ({source_label}).")
                            await reply_target.scroll_into_view_if_needed(timeout=2000)
                            await page.wait_for_timeout(500)
                            box = await reply_target.bounding_box()
                            if box:
                                await page.mouse.move(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                await page.wait_for_timeout(200)
                                await page.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
                                logging.info(f"[{account_name}] ✅ Клик по координатам Reply выполнен.")
                                return True
                            await reply_target.click(force=True, timeout=2000)
                            return True
                        clicked_js = await comment_block.evaluate("""node => {
                            const elements = Array.from(node.querySelectorAll('p, span, div, [role="button"]'));
                            const btn = elements.find(el => {
                                const txt = (el.textContent || el.getAttribute('aria-label') || '').toLowerCase().trim();
                                return txt === 'ответить' || txt === 'reply' || el.getAttribute('data-e2e') === 'comment-reply-1';
                            });
                            if (btn) {
                                btn.scrollIntoView({block: 'center'});
                                const event = new MouseEvent('click', { view: window, bubbles: true, cancelable: true, buttons: 1 });
                                btn.dispatchEvent(event);
                                return true;
                            }
                            return false;
                        }""")
                        if clicked_js:
                            logging.info(f"[{account_name}] ✅ Reply нажата через JS ({source_label})")
                            return True
                    except Exception as e:
                        logging.debug(f"[{account_name}] Не удалось нажать Reply в блоке {source_label}: {e}")
                    return False

                async def focus_comment_input():
                    is_focused = await page.evaluate("""() => {
                        let el = document.activeElement;
                        if (!el) return false;
                        return el.isContentEditable || el.tagName === 'INPUT' || el.tagName === 'TEXTAREA' || el.getAttribute('contenteditable') === 'true';
                    }""")
                    if is_focused:
                        logging.info(f"[{account_name}] ✅ Поле ввода УЖЕ в фокусе.")
                        return True
                    input_selectors = [
                        'div[data-e2e="comment-input"] div[contenteditable="true"]',
                        'div[role="textbox"][contenteditable="true"]',
                        'div[data-e2e="comment-input"] .public-DraftEditor-content',
                        'div[data-e2e="comment-input"]',
                        '.public-DraftEditor-content',
                        '.DraftEditor-root',
                    ]
                    for sel in input_selectors:
                        try:
                            target = page.locator(sel).first
                            if await target.count() > 0 and await target.is_visible(timeout=3000):
                                await target.scroll_into_view_if_needed(timeout=3000)
                                await page.wait_for_timeout(500)
                                await target.click(force=True, timeout=3000)
                                await page.wait_for_timeout(800)
                                logging.info(f"[{account_name}] ✅ Поле ввода найдено и кликнуто: {sel}")
                                return True
                        except:
                            continue
                    return False


                async def force_click_input():
                    if reply_to_text:
                        active_statuses[account_name] = f"Диалог: поиск комментария '{reply_to_text[:30]}'..."
                        logging.info(f"[{account_name}] 🔍 Режим Reply. Ищем коммент: '{reply_to_text[:50]}'")
                        if not await ensure_panel_open():
                            raise Exception("Не смогли открыть панель комментариев для Reply")
                        await page.wait_for_timeout(3000)
                        reply_clicked = False

                        if reply_to_comment_id and not reply_clicked:
                            logging.info(f"[{account_name}] 🧷 Ищем по точному comment_id: {reply_to_comment_id}")
                            id_selectors = [
                                f'[id*="{reply_to_comment_id}"]',
                                f'[data-id="{reply_to_comment_id}"]',
                                f'[data-comment-id="{reply_to_comment_id}"]',
                            ]
                            for sel in id_selectors:
                                if reply_clicked: break
                                try:
                                    block = page.locator(sel).first
                                    if await block.count() > 0:
                                        if await try_click_reply_in_block(block, f"id-selector/{sel}"):
                                            reply_clicked = True
                                            break
                                except: continue

                        if not reply_clicked:
                            nick_query = reply_to_author_nickname.lower().replace(" ", "") if reply_to_author_nickname else None
                            text_query = reply_to_text.lower().replace(" ", "")
                            for scroll_attempt in range(35):
                                comments = await page.locator('[data-e2e="comment-level-1"], [data-e2e="comment-level-2"], div[class*="CommentItem"], div[class*="DivCommentNode"]').all()
                                active_statuses[account_name] = f"Диалог: поиск комментов (скролл {scroll_attempt + 1}/35, найдено {len(comments)})..."
                                logging.info(f"[{account_name}] 🔎 Поиск целевого комментария. Скролл {scroll_attempt + 1}/35. Видимых: {len(comments)}")
                                for comment_block in comments:
                                    try:
                                        content_lower = await comment_block.evaluate("node => (node.textContent || '').toLowerCase()")
                                        content_nospace = content_lower.replace(" ", "")
                                        if nick_query and nick_query not in content_nospace:
                                            continue
                                        if text_query in content_nospace:
                                            active_statuses[account_name] = "Диалог: нажимаем Ответить..."
                                            logging.info(f"[{account_name}] ✅ Найден целевой коммент")
                                            if await try_click_reply_in_block(comment_block, "nick+text-search"):
                                                reply_clicked = True
                                                break
                                    except:
                                        continue
                                if reply_clicked:
                                    break
                                await scroll_comments_panel(page, account_name, pixels=600)
                                await page.wait_for_timeout(2500)

                        if not reply_clicked:
                            active_statuses[account_name] = "Диалог: глобальный JS поиск комментария..."
                            logging.info(f"[{account_name}] 🔎 Глобальная JS инъекция для нажатия на ответ...")
                            try:
                                safe_text = reply_to_text.lower().replace(" ", "").replace('"', '').replace("'", "").replace('\\\\', '')
                                safe_nick = (reply_to_author_nickname or "").lower().replace(" ", "").replace('"', '').replace("'", "")
                                clicked_global = await page.evaluate(f"""() => {{
                                    const containers = Array.from(document.querySelectorAll('[data-e2e="comment-level-1"], [data-e2e="comment-level-2"], div[class*="DivCommentNode"], div[class*="CommentItem"], div[class*="comment-item"], div[class*="comment-node"]'));
                                    const targetText = "{safe_text}";
                                    const targetNick = "{safe_nick}";
                                    const targetContainer = containers.find(c => {{
                                        let txt = (c.textContent || '').toLowerCase().replace(/\\s+/g, '');
                                        if (targetNick && !txt.includes(targetNick)) return false;
                                        return txt.includes(targetText);
                                    }});
                                    if (targetContainer) {{
                                        const elements = Array.from(targetContainer.querySelectorAll('p, span, div, [role="button"], button'));
                                        const replyBtn = elements.find(el => {{
                                            const txt = (el.textContent || el.getAttribute('aria-label') || '').toLowerCase().trim();
                                            return txt === 'ответить' || txt === 'reply' || el.getAttribute('data-e2e') === 'comment-reply-1' || el.getAttribute('data-e2e') === 'comment-reply';
                                        }});
                                        if (replyBtn) {{
                                            replyBtn.scrollIntoView({{block: 'center', behavior: 'auto'}});
                                            const ev = new MouseEvent('click', {{ view: window, bubbles: true, cancelable: true, buttons: 1 }});
                                            replyBtn.dispatchEvent(ev);
                                            try {{ replyBtn.click(); }} catch(e) {{}}
                                            return true;
                                        }}
                                    }}
                                    return false;
                                }}""")
                                if clicked_global:
                                    logging.info(f"[{account_name}] ✅ Reply нажата через Глобальный JS")
                                    reply_clicked = True
                            except Exception as e:
                                logging.debug(f"Ошибка Глобального JS: {e}")

                        if not reply_clicked:
                            raise Exception(f"Кнопка Reply/Ответить не найдена. comment_id={reply_to_comment_id}, текст='{reply_to_text[:40]}'")

                        await page.wait_for_timeout(2000)
                        if not await focus_comment_input():
                            raise Exception("Не смогли сфокусироваться на поле ввода после Reply")

                    else:
                        active_statuses[account_name] = "Подготовка к написанию комментария..."
                        if not await ensure_panel_open():
                            raise Exception("Не смогли открыть панель комментариев")
                        await page.wait_for_timeout(1500)
                        if not await focus_comment_input():
                            raise Exception("Не смогли сфокусироваться на поле ввода комментария")


                try:
                    await force_click_input()
                except Exception as e:
                    logging.warning(f"[{account_name}] ⚠️ Первая попытка не удалась ({e}). Перезагружаем страницу...")
                    await page.reload(wait_until="domcontentloaded")
                    await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
                    await page.wait_for_timeout(5000)
                    try:
                        await force_click_input()
                    except Exception as e2:
                        logging.error(f"[{account_name}] ❌ Не смогли обойти защиту: {e2}")
                        return False

                final_text = spin_text(template_text)
                active_statuses[account_name] = f"Печатаем коммент: '{final_text[:25]}'"
                logging.info(f"[{account_name}] ⌨️ Печатаем текст: {final_text}")

                try:
                    for char in final_text:
                        await page.keyboard.type(char, delay=random.randint(80, 250))
                    await page.wait_for_timeout(random.randint(1500, 3000))
                    logging.info(f"[{account_name}] ✅ Текст успешно напечатан.")
                except Exception as fatal_e:
                    logging.error(f"[{account_name}] ❌ Фатальная ошибка ввода: {fatal_e}")
                    return False

                active_statuses[account_name] = "Публикация комментария..."
                try:
                    post_btn = page.locator('[data-e2e="comment-post"]').last
                    await post_btn.wait_for(state="visible", timeout=5000)
                    await post_btn.click()
                    logging.info(f"[{account_name}] ✅ Кнопка 'Опубликовать' нажата!")
                except Exception:
                    await page.keyboard.press('Enter')
                    logging.info(f"[{account_name}] ✅ Enter (фолбэк).")

                for _ in range(10):
                    if captured_comment_id:
                        break
                    await page.wait_for_timeout(1000)

                response_listener_active = False

                if captured_comment_id:
                    active_statuses[account_name] = "Комментарий опубликован!"
                    logging.info(f"[{account_name}] ✅ Комментарий опубликован! comment_id={captured_comment_id}")
                else:
                    logging.warning(f"[{account_name}] ✅ Комментарий опубликован, но comment_id не пойман.")

                return {"text": final_text, "comment_id": captured_comment_id}

    except Exception as e:
        await send_error_to_tg(f"Ошибка на странице: {str(e)[:100]}", account_name)
        return False
    except RuntimeError as e:
        # Ошибка запуска AdsPower-профиля (из adspower_profile)
        await send_error_to_tg(str(e), account_name)
        return False
    finally:
        active_statuses.pop(account_name, None)
        try:
            if 'page' in locals() and not page.is_closed():
                await page.close()
            if 'context' in locals():
                await context.close()
            if 'browser' in locals():
                await browser.close()
        except Exception as e:
            logging.debug(f"Ошибка закрытия браузера: {e}")



# ================= АВТО-ПАРСЕР TIKTOK =================

# --- Вспомогательная функция (#2): заменяет дублированный блок проверки видео ---
async def check_and_filter_video(
    page,
    video_url: str,
    account_name: str,
    search_url: str,
    current_ts: int,
    max_age_seconds: int,
    min_views: int,
    kw_lower: str,
    kw_words: list,
    is_foryou: bool,
    is_hashtag: bool,
) -> str | None:
    """
    Открывает страницу видео, проверяет просмотры / дату / релевантность.
    Возвращает чистый URL если видео прошло все фильтры, иначе None.
    """
    try:
        await page.goto(video_url, wait_until="domcontentloaded", timeout=25000)
        await page.wait_for_timeout(2500)

        video_data = await page.evaluate(r'''() => {
            let views = 0, createTime = 0, fullText = "";
            try {
                const scripts = document.querySelectorAll(
                    'script[id="__UNIVERSAL_DATA_FOR_REHYDRATION__"], script[id="SIGI_STATE"]'
                );
                for (let sc of scripts) {
                    const t = sc.textContent || "";
                    const mv = t.match(/"playCount"\s*:\s*(\d+)/);
                    if (mv) views = parseInt(mv[1]);
                    const mc = t.match(/"createTime"\s*:\s*"?(\d+)"?/);
                    if (mc) createTime = parseInt(mc[1]);
                    const md = t.match(/"desc"\s*:\s*"([^"]{0,800})"/);
                    if (md) fullText += " " + md[1];
                    const tagMatches = [...t.matchAll(/"hashtagName"\s*:\s*"([^"]+)"/g)];
                    for (const m of tagMatches) fullText += " " + m[1];
                    if (views > 0) break;
                }
                const descSelectors = [
                    '[data-e2e="browse-video-desc"]', '[data-e2e="video-desc"]',
                    '[class*="SpanDesc"]', '[class*="video-meta-title"]', 'h1[class*="title"]',
                ];
                for (const sel of descSelectors) {
                    const el = document.querySelector(sel);
                    if (el) { fullText += " " + (el.innerText || el.textContent || ""); break; }
                }
                document.querySelectorAll('a[href*="/tag/"], a[data-e2e*="hashtag"]').forEach(a => {
                    fullText += " " + (a.innerText || a.textContent || "");
                });
                if (views === 0) {
                    const viewSelectors = [
                        '[data-e2e="like-icon"] + strong', '[data-e2e="video-views"]',
                        '[class*="StrongVideoPlayCount"]',
                    ];
                    for (const sel of viewSelectors) {
                        const el = document.querySelector(sel);
                        if (el) {
                            const txt = (el.innerText || "").replace(/[,. ]/g, "").toUpperCase();
                            if (txt.includes("M")) views = parseFloat(txt) * 1000000;
                            else if (txt.includes("K")) views = parseFloat(txt) * 1000;
                            else views = parseInt(txt) || 0;
                            break;
                        }
                    }
                }
            } catch(e) {}
            return { views, createTime, fullText: fullText.toLowerCase() };
        }''')

        views_count = video_data.get("views", 0)
        create_time = video_data.get("createTime", 0)
        full_text = video_data.get("fullText", "")

        logging.info(f"[{account_name}] 📊 Видео: {views_count} просм. | текст: '{full_text[:80]}'")

        if max_age_seconds > 0 and create_time > 0:
            if (current_ts - create_time) > max_age_seconds:
                logging.info(f"[{account_name}] ⏩ Слишком старое видео, пропускаю")
                await page.go_back(wait_until="domcontentloaded", timeout=15000)
                await page.wait_for_timeout(1000)
                return None

        if views_count < min_views:
            logging.info(f"[{account_name}] ⏩ Мало просмотров: {views_count} < {min_views}")
            await page.go_back(wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(1000)
            return None

        is_relevant = True
        if kw_lower and not is_foryou:
            if is_hashtag:
                is_relevant = kw_lower in full_text
            else:
                is_relevant = any(w in full_text for w in kw_words) if kw_words else kw_lower in full_text

        if not is_relevant:
            logging.info(f"[{account_name}] ⏩ Не по теме '{kw_lower}': {video_url}")
            await page.go_back(wait_until="domcontentloaded", timeout=15000)
            await page.wait_for_timeout(1000)
            return None

        clean_video_url = video_url.split('?')[0].split('#')[0]
        await page.go_back(wait_until="domcontentloaded", timeout=15000)
        await page.wait_for_timeout(1200)
        return clean_video_url

    except Exception as e_check:
        logging.warning(f"[{account_name}] ⚠️ Ошибка проверки {video_url}: {e_check}")
        try:
            if search_url not in page.url:
                await page.goto(search_url, wait_until="domcontentloaded", timeout=25000)
                await page.wait_for_timeout(2000)
        except:
            pass
        return None


async def tiktok_parser(account_name, adspower_id, project_name, keyword, target_count, min_views, age_filter, message: Message, export_only: bool = False):
    logging.info(f"[{account_name}] Парсинг: {keyword} | Мин. просмотров: {min_views}")
    active_statuses[f"Парсер_{account_name}"] = f"Запуск парсера для ключевого слова '{keyword}'..."

    current_ts = int(datetime.now().timestamp())
    try:
        max_age_days = int(age_filter) if age_filter and age_filter != "0" else 0
    except:
        max_age_days = 0
    max_age_seconds = max_age_days * 86400 if max_age_days > 0 else 0

    collected_urls: set = set()
    processed_raw_links: set = set()
    is_foryou = (keyword == "FORYOU")
    is_hashtag = keyword.startswith("#") and not is_foryou

    try:
        async with adspower_profile(adspower_id, account_name) as ws_endpoint:
            async with async_playwright() as p:
                browser = await p.chromium.connect_over_cdp(ws_endpoint)
                context = browser.contexts[0]

                reset_page = await context.new_page()
                await reset_page.goto("about:blank", wait_until="domcontentloaded")
                await reset_page.wait_for_timeout(500)
                await reset_page.close()

                page = await context.new_page()

                intercepted_data = {}

            async def intercept_api(response):
                try:
                    if "tiktok.com/api/" in response.url:
                        json_data = await response.json()
                        items = json_data.get("itemList", [])
                        if not items and "item_list" in json_data:
                            items = json_data.get("item_list", [])
                        if not items and "data" in json_data:
                            for d in json_data["data"]:
                                if "item" in d:
                                    items.append(d["item"])
                        for item in items:
                            try:
                                vid_id = item.get("id")
                                author = item.get("author", {}).get("uniqueId")
                                views = item.get("stats", {}).get("playCount", 0)
                                create_time = int(item.get("createTime", 0))
                                if vid_id and author:
                                    url = f"https://www.tiktok.com/@{author}/video/{vid_id}"
                                    if max_age_seconds > 0 and create_time > 0:
                                        age = current_ts - create_time
                                        if age > max_age_seconds:
                                            processed_raw_links.add(url)
                                            continue
                                    intercepted_data[url] = int(views)
                            except:
                                pass
                except:
                    pass

            page.on("response", intercept_api)
            await page.bring_to_front()

            if is_foryou:
                search_url = "https://www.tiktok.com/foryou"
            elif is_hashtag and max_age_days == 0:
                tag_name = keyword.replace("#", "").strip()
                search_url = f"https://www.tiktok.com/tag/{quote(tag_name)}"
            else:
                def days_to_tiktok_publish_time(days_str):
                    try:
                        days = int(days_str)
                        if days <= 0: return ""
                        elif days <= 1: return "&publish_time=1"
                        elif days <= 7: return "&publish_time=7"
                        elif days <= 30: return "&publish_time=30"
                        else: return "&publish_time=90"
                    except:
                        return ""
                age_param = days_to_tiktok_publish_time(age_filter)
                search_url = f"https://www.tiktok.com/search/video?q={quote(keyword)}{age_param}"

            page_loaded = False
            for retry in range(3):
                try:
                    if is_hashtag and max_age_days == 0 and retry == 0:
                        await page.goto("https://www.tiktok.com", wait_until="domcontentloaded", timeout=45000)
                        await page.wait_for_timeout(3000)
                        await page.evaluate(f"window.location.href = '{search_url}'")
                    else:
                        await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(5000)
                except Exception as e:
                    logging.warning(f"[{account_name}] ⚠️ Ошибка перехода: {e}")

                for close_btn in ['div[data-e2e="modal-close-inner-button"]', 'button:has-text("Decline all")']:
                    try:
                        btn = page.locator(close_btn).first
                        if await btn.is_visible(timeout=1000):
                            await btn.click(force=True)
                            await page.wait_for_timeout(1000)
                    except:
                        pass

                is_ready = await wait_for_search_page_ready(page, account_name, timeout_seconds=40)
                if is_ready:
                    page_loaded = True
                    break
                else:
                    # Проверяем капчу
                    if await is_captcha_page(page, account_name):
                        logging.warning(f"[{account_name}] 🔄 Капча! Ждём 10с и обновляем страницу (Попытка {retry+1}/3)...")
                        active_statuses[f"Парсер_{account_name}"] = f"Капча! Обновляю страницу (попытка {retry+1}/3)..."
                        await page.wait_for_timeout(10000)
                        try:
                            await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                        except:
                            try:
                                await page.reload(wait_until="domcontentloaded", timeout=30000)
                            except:
                                pass
                        await page.wait_for_timeout(7000)
                    else:
                        logging.warning(f"[{account_name}] 🔄 Лента пуста (Попытка {retry+1}/3). Обновляем страницу...")
                        try:
                            await page.reload(wait_until="domcontentloaded", timeout=30000)
                        except:
                            pass
                        await page.wait_for_timeout(5000)

            if not page_loaded:
                logging.error(f"[{account_name}] ❌ Страница так и не отдала видео.")
                try:
                    await message.answer(f"❌ TikTok не загрузил видео по запросу `{keyword}`.")
                except:
                    pass
                return

            attempts = 0
            no_new_streak = 0
            max_attempts = max(100, target_count * 3)

            while len(collected_urls) < target_count and attempts < max_attempts:
                await page.bring_to_front()
                prev_size = len(collected_urls)

                for url, views in list(intercepted_data.items()):
                    if url not in collected_urls and url not in processed_raw_links:
                        processed_raw_links.add(url)
                        if views >= min_views:
                            collected_urls.add(url)
                            logging.info(f"[{account_name}] 🧲 Перехвачено (API): {views} просм. — {url}")
                            if len(collected_urls) >= target_count:
                                break

                if len(collected_urls) >= target_count:
                    break

                new_raw_links_js = await page.evaluate(r'''() => {
                    let results = [];
                    function isInsideSidebar(el) {
                        return el.closest('nav') || el.closest('aside') || el.closest('[data-e2e="nav-container"]') ||
                               el.closest('[data-e2e="nav-menu"]') || el.closest('[class*="NavContainer"]') ||
                               el.closest('[class*="SidebarContainer"]') || el.closest('[class*="SideNavContainer"]');
                    }
                    let cards = document.querySelectorAll('[data-e2e="recommend-list-item-container"], [data-e2e="challenge-item"], [data-e2e="search_top-item"], [data-e2e="search_video-item"], [data-e2e="search-item"], div[class*="DivItemContainer"], div[class*="DivVideoFeedItem"]');
                    cards.forEach(card => {
                        if (isInsideSidebar(card)) return;
                        let text = card.innerText || "";
                        let links = Array.from(card.querySelectorAll('a'));
                        for (let a of links) {
                            if (a.href && a.href.includes('/video/') && a.href.includes('@')) {
                                results.push({url: a.href, text: text});
                                return;
                            }
                        }
                        let html = card.innerHTML;
                        let match = html.match(/(\/@[a-zA-Z0-9_.-]+\/video\/\d+)/);
                        if (match) {
                            results.push({url: "https://www.tiktok.com" + match[1], text: text});
                        }
                    });
                    if (results.length === 0) {
                        let allLinks = Array.from(document.querySelectorAll('a'));
                        allLinks.forEach(a => {
                            if (!isInsideSidebar(a)) {
                                if (a.href && a.href.includes('/video/') && a.href.includes('@')) {
                                    let container = a.closest('[data-e2e="search_video-item"]') || a.closest('div[class*="DivItemContainer"]') || a.closest('div');
                                    results.push({url: a.href, text: container ? container.innerText : ""});
                                }
                            }
                        });
                    }
                    return results;
                }''')

                new_raw_links = []
                for item in new_raw_links_js:
                    try:
                        if isinstance(item, str):
                            href = item
                            card_text = ""
                        else:
                            href = item.get("url", "")
                            card_text = item.get("text", "")
                        clean_url = href.split('?')[0].split('#')[0]
                        if not clean_url:
                            continue
                        if clean_url.startswith('/'):
                            clean_url = "https://www.tiktok.com" + clean_url
                        clean_url = clean_url.replace("m.tiktok.com", "www.tiktok.com")
                        if "tiktok.com" not in clean_url:
                            continue
                        if not is_valid_tiktok_video_url(clean_url):
                            continue
                        if clean_url not in collected_urls and clean_url not in processed_raw_links:
                            new_raw_links.append((clean_url, card_text))
                            processed_raw_links.add(clean_url)
                    except:
                        continue

                if new_raw_links:
                    logging.info(f"[{account_name}] 🔎 Найдено в DOM видео: {len(new_raw_links)}")

                    # Используем единую функцию check_and_filter_video (#2)
                    kw_lower = keyword.lower().replace("#", "").strip() if not is_foryou else ""
                    kw_words = [w for w in kw_lower.split() if len(w) > 2] if kw_lower else []

                    for video_url, card_text in new_raw_links:
                        if len(collected_urls) >= target_count:
                            break

                        active_statuses[f"Парсер_{account_name}"] = f"Проверяю видео {len(collected_urls)+1}/{target_count}..."
                        logging.info(f"[{account_name}] 🖱️ Кликаю на видео: {video_url}")

                        result_url = await check_and_filter_video(
                            page=page,
                            video_url=video_url,
                            account_name=account_name,
                            search_url=search_url,
                            current_ts=current_ts,
                            max_age_seconds=max_age_seconds,
                            min_views=min_views,
                            kw_lower=kw_lower,
                            kw_words=kw_words,
                            is_foryou=is_foryou,
                            is_hashtag=is_hashtag,
                        )
                        if result_url:
                            collected_urls.add(result_url)
                            logging.info(f"[{account_name}] ✅ ДОБАВЛЕНО [{len(collected_urls)}/{target_count}]: {result_url}")
                            active_statuses[f"Парсер_{account_name}"] = f"✅ Собрано {len(collected_urls)}/{target_count} — '{keyword}'"

                            # Промежуточный прогресс (#7): каждые 25 URL сообщаем пользователю
                            if len(collected_urls) % 25 == 0:
                                try:
                                    await message.answer(
                                        f"⏳ Прогресс парсинга: `{len(collected_urls)}/{target_count}` видео собрано...",
                                        parse_mode="Markdown"
                                    )
                                except:
                                    pass

                logging.info(f"[{account_name}] 📊 Итого собрано: {len(collected_urls)}/{target_count}")
                active_statuses[f"Парсер_{account_name}"] = f"Парсинг '{keyword}': собрано {len(collected_urls)}/{target_count}, попытка {attempts}"

                if len(collected_urls) == prev_size:
                    no_new_streak += 1
                    # При 2 пустых итерациях подряд — проверяем капчу и обновляем страницу
                    if no_new_streak % 2 == 0 and no_new_streak < 8:
                        logging.info(f"[{account_name}] 🔎 Нет новых ссылок {no_new_streak} раз подряд — проверяем страницу...")
                        if await is_captcha_page(page, account_name):
                            logging.warning(f"[{account_name}] 🔄 Капча в процессе парсинга! Ждём 10с и обновляем...")
                            active_statuses[f"Парсер_{account_name}"] = f"Капча! Обновляю страницу... (собрано {len(collected_urls)}/{target_count})"
                            await page.wait_for_timeout(10000)
                            try:
                                await page.goto(search_url, wait_until="domcontentloaded", timeout=45000)
                            except:
                                try:
                                    await page.reload(wait_until="domcontentloaded", timeout=30000)
                                except:
                                    pass
                            await page.wait_for_timeout(7000)
                            # Ждём пока страница нормально загрузится
                            page_ok = await wait_for_search_page_ready(page, account_name, timeout_seconds=40)
                            if not page_ok:
                                # Ещё одна попытка
                                await page.reload(wait_until="domcontentloaded", timeout=30000)
                                await page.wait_for_timeout(7000)
                                await wait_for_search_page_ready(page, account_name, timeout_seconds=30)
                        else:
                            # Страница просто не прогрузила новый контент — обычный reload
                            logging.info(f"[{account_name}] 🔄 Нет капчи, обновляем страницу для подгрузки контента...")
                            try:
                                await page.reload(wait_until="domcontentloaded", timeout=30000)
                            except:
                                pass
                            await page.wait_for_timeout(5000)
                    if no_new_streak >= 8:
                        logging.warning(f"[{account_name}] 🛑 Стрик 8 — контент кончился.")
                        break
                else:
                    no_new_streak = 0

                attempts += 1

                await page.bring_to_front()
                try:
                    close_btn = page.locator('div[data-e2e="modal-close-inner-button"]').first
                    if await close_btn.is_visible(timeout=1000):
                        await close_btn.click()
                except:
                    pass

                if is_foryou:
                    await page.keyboard.press("ArrowDown")
                    await page.wait_for_timeout(1000)
                    await page.keyboard.press("ArrowDown")
                    await page.wait_for_timeout(random.randint(4000, 7000))
                else:
                    await page.evaluate('''() => {
                        let videos = document.querySelectorAll('a[href*="/video/"]');
                        if (videos.length > 0) videos[videos.length - 1].scrollIntoView({behavior: "smooth", block: "end"});
                    }''')
                    await page.wait_for_timeout(1000)
                    await page.mouse.wheel(0, 5000)
                    await page.keyboard.press("PageDown")
                    await page.wait_for_timeout(random.randint(5000, 8000))

    except Exception as e:
        logging.error(f"Ошибка парсера: {e}")
    except RuntimeError as e:
        await message.answer(f"❌ Ошибка запуска профиля AdsPower: {e}")
    finally:
        active_statuses.pop(f"Парсер_{account_name}", None)
        try:
            if 'page' in locals() and not page.is_closed():
                await page.close()
            if 'context' in locals():
                await context.close()
            if 'browser' in locals():
                await browser.close()
        except:
            pass

    if not collected_urls:
        try:
            await message.answer(f"⚠️ По запросу `{keyword}` ничего не найдено с просмотрами >= {min_views}.")
        except:
            pass
        return

    try:
        if export_only:
            content = "\n".join(collected_urls)
            file_buffer = io.BytesIO(content.encode('utf-8'))
            file_name_kw = "foryou" if is_foryou else keyword.replace('#', '')
            export_file = BufferedInputFile(file_buffer.getvalue(), filename=f"parsed_{file_name_kw}.txt")
            await message.answer_document(export_file, caption=f"🎯 **Сбор ссылок завершен!**\nИсточник: `{'Рекомендации' if is_foryou else keyword}`\nСобрано: `{len(collected_urls)}` шт.", parse_mode="Markdown")
        else:
            added = 0
            async with aiosqlite.connect(DB_NAME) as db:
                for url in collected_urls:
                    # INSERT OR IGNORE — дедупликация через уникальный индекс (#9)
                    await db.execute(
                        "INSERT OR IGNORE INTO tasks (project_name, video_url, template_text) VALUES (?, ?, ?)",
                        (project_name, url, "{Круто|Интересно|Супер|Согласен}")
                    )
                    added += 1
                await db.commit()
            await message.answer(f"🎯 **Глубокий парсинг завершен!**\nПроект: `{project_name}`\nЗапрос: `{keyword}`\nМин. просмотров: `{min_views}`\nДобавлено в задачи: `{added}` шт.")
    except Exception as send_err:
        logging.error(f"Не удалось отправить отчет в Telegram: {send_err}")



# ================= ПЛАНИРОВЩИК =================
async def process_task_queue():
    async with aiosqlite.connect(DB_NAME) as db:
        query_reply = '''
            SELECT id, project_name, video_url, comment_text, reply_text, first_account, first_comment_id
            FROM dialogues
            WHERE status = 'waiting_reply'
            AND datetime(first_comment_time, '+' || delay_minutes || ' minutes') <= datetime('now')
            LIMIT 1
        '''
        async with db.execute(query_reply) as cursor:
            reply_task = await cursor.fetchone()

        if reply_task:
            task_id, proj, url, target_text, reply_text, first_acc, first_comment_id = reply_task
            first_acc_nickname = None
            async with db.execute("SELECT tiktok_nickname FROM accounts WHERE name = ? AND tiktok_nickname IS NOT NULL AND tiktok_nickname != ''", (first_acc,)) as cursor:
                nick_row = await cursor.fetchone()
                if nick_row:
                    first_acc_nickname = nick_row[0]
            async with db.execute("SELECT name, session_file FROM accounts WHERE status='active' AND project_name=? AND name != ? LIMIT 1", (proj, first_acc)) as cursor:
                acc = await cursor.fetchone()
            if acc:
                account_name, ads_id = acc
                await db.execute("UPDATE dialogues SET status = 'processing_reply' WHERE id = ?", (task_id,))
                await db.commit()
                success = await tiktot_worker(account_name, ads_id, proj, url, reply_text, reply_to_text=target_text, reply_to_comment_id=first_comment_id, reply_to_author_nickname=first_acc_nickname)
                new_status = 'completed' if success else 'failed_reply'
                await db.execute("UPDATE dialogues SET status = ? WHERE id = ?", (new_status, task_id))
                await db.commit()
                if success:
                    await send_to_all_admins(f"🗣 [Диалог] Ответ оставлен!\nВидео: {url}\nАккаунт: {account_name}")
            return

        query_first = "SELECT id, project_name, video_url, comment_text FROM dialogues WHERE status = 'pending_comment' ORDER BY id DESC LIMIT 1"
        async with db.execute(query_first) as cursor:
            first_task = await cursor.fetchone()

        if first_task:
            task_id, proj, url, comment_text = first_task
            async with db.execute("SELECT name, session_file FROM accounts WHERE status='active' AND project_name=? LIMIT 1", (proj,)) as cursor:
                acc = await cursor.fetchone()
            if acc:
                account_name, ads_id = acc
                await db.execute("UPDATE dialogues SET status = 'processing_first' WHERE id = ?", (task_id,))
                await db.commit()
                success = await tiktot_worker(account_name, ads_id, proj, url, comment_text)
                if success:
                    first_comment_id = success.get("comment_id") if isinstance(success, dict) else None
                    posted_text = success.get("text") if isinstance(success, dict) else success
                    await db.execute(
                        "UPDATE dialogues SET status = 'waiting_reply', first_comment_time = datetime('now'), first_account = ?, first_comment_id = ?, comment_text = ? WHERE id = ?",
                        (account_name, first_comment_id, posted_text or comment_text, task_id)
                    )
                    cid_info = f"\ncomment_id: `{first_comment_id}`" if first_comment_id else "\n⚠️ comment_id не пойман, будет fallback по тексту"
                    await send_to_all_admins(f"🗣 [Диалог] Затравка заброшена!\nЖдем ответа на: `{posted_text or comment_text}`{cid_info}")
                else:
                    await db.execute("UPDATE dialogues SET status = 'failed_first' WHERE id = ?", (task_id,))
                await db.commit()
            return

        query_task = '''
            SELECT t.id, t.project_name, t.video_url, t.template_text
            FROM tasks t
            JOIN projects p ON t.project_name = p.name
            WHERE t.status = 'pending' AND p.status = 'active'
            AND t.created_at >= datetime('now', '-24 hours')
            ORDER BY t.id DESC
            LIMIT 1
        '''
        async with db.execute(query_task) as cursor:
            task = await cursor.fetchone()

        if not task:
            return
        task_id, project_name, video_url, template_text = task

        match = re.search(r'@([\w\.-]+)', video_url)
        if match:
            author_username = match.group(1)
            async with db.execute("SELECT id FROM blacklist WHERE username = ?", (author_username,)) as cursor:
                is_banned = await cursor.fetchone()
            if is_banned:
                logging.info(f"⏩ Пропуск задачи #{task_id}: автор @{author_username} в ЧС.")
                await db.execute("UPDATE tasks SET status = 'skipped' WHERE id = ?", (task_id,))
                await db.commit()
                return

        query_acc = '''
            SELECT name, session_file
            FROM accounts
            WHERE status = 'active' AND project_name = ?
            ORDER BY (
                SELECT IFNULL(MAX(timestamp), '2000-01-01')
                FROM task_history
                WHERE account_name = accounts.name
            ) ASC
        '''
        async with db.execute(query_acc, (project_name,)) as cursor:
            accounts = await cursor.fetchall()

        if not accounts:
            return

        selected_account = None
        for acc in accounts:
            account_name, adspower_id = acc
            async with db.execute("SELECT COUNT(*) FROM task_history WHERE account_name = ? AND timestamp >= datetime('now', '-1 hour')", (account_name,)) as cursor:
                count_hour = (await cursor.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM task_history WHERE account_name = ? AND timestamp >= datetime('now', '-1 day')", (account_name,)) as cursor:
                count_day = (await cursor.fetchone())[0]
            if count_hour < LIMIT_PER_HOUR and count_day < LIMIT_PER_DAY:
                selected_account = acc
                break

        if not selected_account:
            return

        account_name, adspower_id = selected_account
        await db.execute("UPDATE tasks SET status = 'processing' WHERE id = ?", (task_id,))
        await db.commit()

        result = await tiktot_worker(account_name, adspower_id, project_name, video_url, template_text)
        status = 'completed' if result else 'failed'
        if result:
            await db.execute("INSERT INTO task_history (account_name) VALUES (?)", (account_name,))
            if isinstance(result, dict):
                posted_text = result.get("text") or template_text
                await db.execute("UPDATE tasks SET status = ?, template_text = ? WHERE id = ?", (status, posted_text, task_id))
            elif isinstance(result, str):
                await db.execute("UPDATE tasks SET status = ?, template_text = ? WHERE id = ?", (status, result, task_id))
            else:
                await db.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        else:
            await db.execute("UPDATE tasks SET status = ? WHERE id = ?", (status, task_id))
        await db.commit()



# ================= РЕВИЗОР =================
async def run_revisor(proj_name, message: Message):
    active_statuses[f"Ревизор_{proj_name}"] = "Запуск Ревизора..."
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT name, session_file FROM accounts WHERE status='active' AND project_name=? LIMIT 1", (proj_name,)) as cursor:
            acc = await cursor.fetchone()
        if not acc:
            await message.answer("❌ Нет активных аккаунтов для работы Ревизора.")
            active_statuses.pop(f"Ревизор_{proj_name}", None)
            return
        account_name, adspower_id = acc
        async with db.execute("SELECT tiktok_nickname FROM accounts WHERE project_name=? AND tiktok_nickname IS NOT NULL AND tiktok_nickname != ''", (proj_name,)) as cursor:
            nickname_rows = await cursor.fetchall()
        project_nicknames = [row[0] for row in nickname_rows]
        if not project_nicknames:
            await message.answer("⚠️ У аккаунтов проекта не заданы TikTok-никнеймы!\nДобавьте их, чтобы Ревизор мог работать.")
            return
        query = "SELECT id, video_url, template_text FROM tasks WHERE project_name=? AND status='completed' AND template_text NOT LIKE '[MASS_LIKE%' ORDER BY id DESC LIMIT 10"
        async with db.execute(query, (proj_name,)) as cursor:
            tasks = await cursor.fetchall()

    if not tasks:
        await message.answer("🤷‍♂️ Нет успешных комментариев для проверки.")
        return

    alive_count = 0
    deleted_count = 0

    api_key = await get_api_key()
    open_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/start?user_id={adspower_id}&api_key={api_key}"
    try:
        resp = requests.get(open_url, timeout=10).json()
        ws_endpoint = resp["data"]["ws"]["puppeteer"]
    except Exception as e:
        await message.answer(f"❌ Ошибка Ревизора (AdsPower): {e}")
        return

    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(ws_endpoint)
            page = await browser.contexts[0].new_page()

            for task_id, url, text in tasks:
                logging.info(f"[{account_name}] 🕵️‍♂️ РЕВИЗОР: Проверка: {url}")
                try:
                    clean_template = text.replace("[BOOST]", "").strip()
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await wait_for_video_page_ready(page, account_name, timeout_seconds=45)
                    await page.wait_for_timeout(random.randint(3000, 5000))

                    is_open = False
                    for sel in ['[data-e2e="comment-level-1"]', 'div[data-e2e="comment-input"]', 'textarea']:
                        try:
                            if await page.locator(sel).first.is_visible(timeout=2000):
                                is_open = True
                                break
                        except:
                            pass

                    if not is_open:
                        await page.keyboard.press('c')
                        await page.wait_for_timeout(3000)
                        for sel in ['[data-e2e="comment-level-1"]', 'textarea']:
                            try:
                                if await page.locator(sel).first.is_visible(timeout=2000):
                                    is_open = True
                                    break
                            except:
                                pass

                    await page.wait_for_timeout(3000)

                    is_alive = False
                    await page.keyboard.press('Control+f')
                    await page.wait_for_timeout(1500)
                    await page.keyboard.type(clean_template, delay=50)
                    await page.wait_for_timeout(2000)

                    try:
                        page_text = await page.evaluate("document.body.innerText")
                        text_found = clean_template.lower() in page_text.lower()
                    except:
                        text_found = False

                    await page.keyboard.press('Escape')
                    await page.wait_for_timeout(500)

                    if text_found:
                        comments = await page.locator('[data-e2e="comment-level-1"]').all()
                        for comment_block in comments:
                            try:
                                block_text = await comment_block.inner_text(timeout=1000)
                                block_lower = block_text.lower()
                                if clean_template.lower() in block_lower:
                                    for nick in project_nicknames:
                                        if nick.lower() in block_lower:
                                            is_alive = True
                                            break
                                if is_alive:
                                    break
                            except:
                                continue

                    async with aiosqlite.connect(DB_NAME) as db:
                        if is_alive:
                            alive_count += 1
                        else:
                            deleted_count += 1
                            await db.execute("UPDATE tasks SET status='deleted' WHERE id=?", (task_id,))
                        await db.commit()

                except Exception as inner_e:
                    logging.error(f"[{account_name}] ❌ Ошибка проверки: {inner_e}")
                    continue

    except Exception as e:
        logging.error(f"Ошибка Ревизора (Playwright): {e}")
    finally:
        active_statuses.pop(f"Ревизор_{proj_name}", None)
        try:
            if 'page' in locals() and not page.is_closed():
                await page.close()
            if 'context' in locals():
                await context.close()
            if 'browser' in locals():
                await browser.close()
            try:
                api_key = await get_api_key()
                stop_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/stop?user_id={adspower_id}&api_key={api_key}"
                requests.get(stop_url, timeout=10)
            except:
                pass
        except:
            pass

    report = (
        f"📊 **Отчет Ревизора (Проект: `{proj_name}`):**\n\n"
        f"✅ Живых комментариев: `{alive_count}`\n"
        f"👻 Удалено/Теневой бан: `{deleted_count}`\n\n"
        f"*Удаленные комментарии вычтены из статистики успешных.*"
    )
    await message.answer(report, parse_mode="Markdown")



# ================= ПРОВЕРКА НА ТЕНЕВОЙ БАН =================
def kb_persistent_status():
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📊 Статус работы")]],
        resize_keyboard=True,
        persistent=True
    )


async def _fetch_live_video_url(page, account_name: str) -> str:
    """Открывает /foryou и перехватывает первый живой URL видео из API-ответа TikTok."""
    captured = {}

    async def on_response(response):
        if captured.get("url"):
            return
        try:
            url = response.url
            if "tiktok.com/api/" in url and (
                "recommend/item_list" in url or "item_list" in url or "feed" in url
            ):
                data = await response.json()
                items = data.get("itemList") or data.get("item_list") or []
                for item in items:
                    vid_id = item.get("id")
                    author = (item.get("author") or {}).get("uniqueId")
                    if vid_id and author:
                        captured["url"] = f"https://www.tiktok.com/@{author}/video/{vid_id}"
                        return
        except:
            pass

    page.on("response", on_response)
    try:
        await page.goto("https://www.tiktok.com/foryou", wait_until="domcontentloaded", timeout=30000)
        await wait_for_search_page_ready(page, account_name, timeout_seconds=20)
        for _ in range(30):
            if captured.get("url"):
                break
            await page.wait_for_timeout(500)
    except Exception as e:
        logging.warning(f"[{account_name}] _fetch_live_video_url: goto error: {e}")

    try:
        page.remove_listener("response", on_response)
    except:
        pass

    result = captured.get("url", "")
    if result:
        logging.info(f"[{account_name}] 🎯 Живое тест-видео: {result}")
    else:
        logging.warning(f"[{account_name}] ⚠️ Не удалось перехватить живое видео из /foryou")
    return result


async def _human_like_click_like_button(page, account_name: str) -> bool:
    """
    Ставит лайк реальным кликом мыши с имитацией человеческого поведения.
    Перехватывает сетевой ответ TikTok для подтверждения что лайк дошёл до сервера.
    Возвращает True если лайк успешно зарегистрирован на сервере.
    """
    # Перехватываем сетевой ответ TikTok на лайк
    like_confirmed_on_server = {"value": False}

    async def on_like_response(response):
        if like_confirmed_on_server["value"]:
            return
        try:
            url = response.url.lower()
            # TikTok отправляет POST на один из этих эндпоинтов при лайке
            if any(x in url for x in ["/commit/item/digg", "/aweme/v1/commit/item/digg", "/api/commit/item/digg"]):
                if response.status in (200, 201):
                    try:
                        data = await response.json()
                        # status_code 0 = успех у TikTok API
                        if data.get("status_code") == 0 or data.get("status_msg") == "success":
                            like_confirmed_on_server["value"] = True
                            logging.info(f"[{account_name}] 🌐 Сервер TikTok подтвердил лайк! (digg API)")
                        else:
                            logging.warning(f"[{account_name}] ⚠️ Лайк отклонён сервером: {data.get('status_msg', 'unknown')}")
                    except:
                        # Если не JSON — всё равно считаем подтверждённым по HTTP 200
                        like_confirmed_on_server["value"] = True
                        logging.info(f"[{account_name}] 🌐 Сервер TikTok ответил 200 на лайк (non-JSON)")
        except:
            pass

    page.on("response", on_like_response)

    try:
        # Селекторы кнопки лайка на странице видео
        like_selectors = [
            '[data-e2e="like-icon"]',
            '[data-e2e="browse-like-icon"]',
            '[data-e2e="video-like-icon"]',
            'button[aria-label*="Like" i]',
            'button[aria-label*="Нравится" i]',
            'span[data-e2e="like-icon"]',
        ]

        like_element = None
        for sel in like_selectors:
            try:
                loc = page.locator(sel).first
                if await loc.count() > 0 and await loc.is_visible(timeout=3000):
                    like_element = loc
                    logging.info(f"[{account_name}] 🎯 Кнопка лайка найдена: {sel}")
                    break
            except:
                continue

        if not like_element:
            logging.warning(f"[{account_name}] ❌ Кнопка лайка не найдена ни одним селектором")
            return False

        # Скроллим к кнопке лайка
        await like_element.scroll_into_view_if_needed(timeout=5000)
        await page.wait_for_timeout(random.randint(800, 1500))

        # Получаем точные координаты кнопки
        box = await like_element.bounding_box()
        if not box:
            logging.warning(f"[{account_name}] ❌ Не удалось получить координаты кнопки лайка")
            return False

        center_x = box["x"] + box["width"] / 2
        center_y = box["y"] + box["height"] / 2

        # Имитация движения мыши как у человека: подводим курсор издалека
        start_x = center_x + random.randint(-200, 200)
        start_y = center_y + random.randint(-150, 150)
        await page.mouse.move(start_x, start_y)
        await page.wait_for_timeout(random.randint(200, 400))

        # Плавно приближаемся к кнопке (несколько промежуточных точек)
        steps = random.randint(3, 6)
        for i in range(steps):
            mid_x = start_x + (center_x - start_x) * (i + 1) / steps + random.randint(-5, 5)
            mid_y = start_y + (center_y - start_y) * (i + 1) / steps + random.randint(-5, 5)
            await page.mouse.move(mid_x, mid_y)
            await page.wait_for_timeout(random.randint(30, 80))

        # Небольшая пауза перед кликом (человек немного думает)
        await page.wait_for_timeout(random.randint(300, 700))

        # Реальный клик мышью (не JS!)
        await page.mouse.click(center_x, center_y)
        logging.info(f"[{account_name}] 🖱️ Реальный клик мышью по кнопке лайка выполнен (x={center_x:.0f}, y={center_y:.0f})")

        # Ждём подтверждения от сервера (до 8 секунд)
        for _ in range(16):
            if like_confirmed_on_server["value"]:
                break
            await page.wait_for_timeout(500)

        if like_confirmed_on_server["value"]:
            logging.info(f"[{account_name}] ✅ Лайк подтверждён сервером TikTok!")
            return True

        # Сервер не ответил через digg API — проверяем визуально изменилось ли состояние кнопки
        logging.info(f"[{account_name}] ℹ️ Сервер не ответил через digg API, проверяем состояние кнопки визуально...")
        await page.wait_for_timeout(2000)

        is_liked_visually = await page.evaluate("""() => {
            const selectors = [
                '[data-e2e="like-icon"]',
                '[data-e2e="browse-like-icon"]',
                '[data-e2e="video-like-icon"]',
                'button[aria-label*="Like" i]',
                'button[aria-label*="Нравится" i]'
            ];
            for (let sel of selectors) {
                let el = document.querySelector(sel);
                if (el) {
                    let btn = el.closest('button') || el.closest('div[role="button"]') || el;
                    let ariaPressed = btn.getAttribute('aria-pressed');
                    let label = (btn.getAttribute('aria-label') || '').toLowerCase();
                    let cls = (btn.className || '').toLowerCase() + ' ' + (el.className || '').toLowerCase();
                    // Проверяем fill цвет SVG (красный = лайкнуто)
                    let svgs = el.querySelectorAll('svg, path');
                    let isRed = false;
                    svgs.forEach(svg => {
                        let fill = window.getComputedStyle(svg).fill || '';
                        if (fill.includes('254, 44') || fill.includes('fe2c') || fill.includes('255, 0') || fill.includes('ff0')) {
                            isRed = true;
                        }
                    });
                    return ariaPressed === 'true'
                        || cls.includes('liked')
                        || cls.includes('active')
                        || isRed
                        || label.includes('unlike')
                        || label.includes('убрать');
                }
            }
            return false;
        }""")

        if is_liked_visually:
            logging.info(f"[{account_name}] ✅ Лайк визуально подтверждён (кнопка изменила состояние)")
            return True
        else:
            logging.warning(f"[{account_name}] ⚠️ Кнопка лайка не изменила состояние после клика")
            return False

    except Exception as e:
        logging.error(f"[{account_name}] ❌ Ошибка при клике на лайк: {e}")
        return False
    finally:
        try:
            page.remove_listener("response", on_like_response)
        except:
            pass



async def check_account_shadow_ban(account_name, adspower_id):
    """
    Проверка теневого бана по методу лайк-теста.
    ИСПРАВЛЕНО: лайк ставится реальным кликом мыши с имитацией человека,
    а не через JS .click(). Дополнительно перехватывается сетевой ответ TikTok
    для подтверждения что лайк реально дошёл до сервера до проверки.
    """
    active_statuses[f"ShadowCheck_{account_name}"] = "Запуск проверки на теневой бан..."
    try:
        api_key = await get_api_key()
        open_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/start?user_id={adspower_id}&api_key={api_key}"

        try:
            resp = requests.get(open_url, timeout=10).json()
            if resp.get("code") != 0:
                msg = (resp.get("msg") or "").lower()
                logging.error(f"[{account_name}] Ошибка AdsPower: {resp.get('msg')}")
                if "does not exist" in msg or "not exist" in msg:
                    return "PROFILE_BROKEN"
                return "ERROR"
            ws_endpoint = resp["data"]["ws"]["puppeteer"]
        except Exception as e:
            logging.error(f"[{account_name}] Ошибка запуска AdsPower: {e}")
            return "ERROR"

        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(ws_endpoint)
            context = browser.contexts[0]
            page = await context.new_page()

            # ШАГ 1: Получаем живой URL видео из ленты /foryou
            active_statuses[f"ShadowCheck_{account_name}"] = "Получаем живой URL из /foryou..."
            video_url = await _fetch_live_video_url(page, account_name)
            if not video_url:
                logging.error(f"[{account_name}] Не удалось получить живой URL видео.")
                return "ERROR"

            # ШАГ 2: Открываем страницу видео
            active_statuses[f"ShadowCheck_{account_name}"] = "Открываем страницу видео..."
            goto_success = False
            for goto_attempt in range(1, 4):
                try:
                    logging.info(f"[{account_name}] (ShadowCheck) Попытка открытия {goto_attempt}/3")
                    await page.goto(video_url, wait_until="domcontentloaded", timeout=60000)
                    goto_success = True
                    break
                except Exception as e:
                    err_text = str(e)
                    logging.warning(f"[{account_name}] Ошибка открытия: {err_text[:150]}")
                    if any(x in err_text for x in ["ERR_SOCKS", "ERR_PROXY", "ERR_TUNNEL"]):
                        await page.wait_for_timeout(20000)
                    else:
                        await page.wait_for_timeout(10000)
                    try:
                        await page.goto("about:blank", wait_until="domcontentloaded", timeout=15000)
                    except:
                        pass

            if not goto_success:
                logging.error(f"[{account_name}] Не удалось открыть видео после 3 попыток.")
                return "ERROR"

            # ШАГ 3: Ждём полной загрузки страницы
            active_statuses[f"ShadowCheck_{account_name}"] = "Ожидание загрузки страницы..."
            is_ready = await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
            if not is_ready:
                logging.warning(f"[{account_name}] Страница не загрузилась, пробуем обновить...")
                await page.reload(wait_until="domcontentloaded")
                await page.wait_for_timeout(5000)
                is_ready = await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
                if not is_ready:
                    logging.error(f"[{account_name}] Страница так и не загрузилась.")
                    return "ERROR"

            # Естественная пауза — человек смотрит видео перед лайком
            await page.wait_for_timeout(random.randint(3000, 6000))

            # ШАГ 4: Проверяем авторизацию (сессию)
            for login_sel in ['button[data-e2e="top-login-button"]', 'div[id="login-modal"]']:
                try:
                    if await page.locator(login_sel).first.is_visible(timeout=2000):
                        await page.reload(wait_until="domcontentloaded")
                        await wait_for_video_page_ready(page, account_name, timeout_seconds=60)
                        await page.wait_for_timeout(3000)
                        if await page.locator(login_sel).first.is_visible(timeout=2000):
                            await send_error_to_tg("Слетела сессия при проверке теневого бана!", account_name)
                            return "ERROR"
                except:
                    pass

            # ШАГ 5: Проверяем — не стоит ли лайк уже (снимаем если стоит)
            active_statuses[f"ShadowCheck_{account_name}"] = "Проверяем начальное состояние лайка..."
            already_liked = await page.evaluate("""() => {
                const selectors = [
                    '[data-e2e="like-icon"]', '[data-e2e="browse-like-icon"]',
                    '[data-e2e="video-like-icon"]', 'button[aria-label*="Like" i]'
                ];
                for (let sel of selectors) {
                    let el = document.querySelector(sel);
                    if (el) {
                        let btn = el.closest('button') || el.closest('div[role="button"]') || el;
                        return btn.getAttribute('aria-pressed') === 'true'
                            || (btn.className || '').toLowerCase().includes('liked');
                    }
                }
                return false;
            }""")

            if already_liked:
                logging.info(f"[{account_name}] ℹ️ Лайк уже стоит, снимаем перед тестом...")
                # Снимаем лайк реальным кликом
                await _human_like_click_like_button(page, account_name)
                await page.wait_for_timeout(random.randint(2000, 3000))

            # ШАГ 6: Ставим лайк РЕАЛЬНЫМ кликом мыши с подтверждением от сервера
            active_statuses[f"ShadowCheck_{account_name}"] = "Ставим лайк (реальный клик мыши)..."
            like_sent_to_server = await _human_like_click_like_button(page, account_name)

            if not like_sent_to_server:
                logging.warning(f"[{account_name}] ❌ Не удалось поставить лайк (кнопка не нашлась или не ответила).")
                return "ERROR"

            logging.info(f"[{account_name}] ✅ Лайк поставлен и подтверждён. Ждём 15 секунд...")

            # ШАГ 7: Ждём 15 секунд — TikTok должен надёжно сохранить лайк
            active_statuses[f"ShadowCheck_{account_name}"] = "Лайк поставлен, ждём 15 секунд для синхронизации..."
            await page.wait_for_timeout(15000)

            # ШАГ 8: Перезагружаем страницу — это единственный надёжный способ
            # проверить сохранился ли лайк на сервере TikTok.
            active_statuses[f"ShadowCheck_{account_name}"] = "Перезагружаем страницу для проверки лайка..."
            logging.info(f"[{account_name}] 🔄 Перезагружаем страницу видео...")
            await page.reload(wait_until="domcontentloaded")
            await page.wait_for_timeout(5000)

            await wait_for_video_page_ready(page, account_name, timeout_seconds=30)
            await page.wait_for_timeout(2000)

            # ШАГ 9: Проверяем стоит ли лайк после перезагрузки
            # Если теневой бан — TikTok не сохранил лайк и он исчезнет.
            active_statuses[f"ShadowCheck_{account_name}"] = "Проверяем лайк после перезагрузки..."
            like_found = False
            like_present = False

            for check_attempt in range(8):
                await page.wait_for_timeout(2000)
                res_final = await page.evaluate("""() => {
                    const selectors = [
                        '[data-e2e="like-icon"]',
                        '[data-e2e="browse-like-icon"]',
                        '[data-e2e="video-like-icon"]',
                        'button[aria-label*="Like" i]',
                        'button[aria-label*="Нравится" i]'
                    ];
                    for (let sel of selectors) {
                        let el = document.querySelector(sel);
                        if (el) {
                            let btn = el.closest('button') || el.closest('div[role="button"]') || el;
                            let ariaPressed = btn.getAttribute('aria-pressed');
                            let label = (btn.getAttribute('aria-label') || '').toLowerCase();
                            let cls = (btn.className || '').toLowerCase() + ' ' + (el.className || '').toLowerCase();
                            // Проверяем fill цвет SVG (красный = лайкнуто)
                            let svgs = el.querySelectorAll('svg, path');
                            let isRed = false;
                            svgs.forEach(svg => {
                                let fill = window.getComputedStyle(svg).fill || '';
                                if (fill.includes('254, 44') || fill.includes('fe2c') || fill.includes('255, 0')) {
                                    isRed = true;
                                }
                            });
                            let isLiked = ariaPressed === 'true'
                                || cls.includes('liked')
                                || cls.includes('active')
                                || isRed
                                || label.includes('unlike')
                                || label.includes('убрать');
                            return { found: true, isLiked: !!isLiked };
                        }
                    }
                    return { found: false, isLiked: false };
                }""")

                if res_final["found"]:
                    like_found = True
                    like_present = res_final["isLiked"]
                    logging.info(f"[{account_name}] 🔍 Попытка {check_attempt+1}/8: found={like_found}, isLiked={like_present}")
                    break
                logging.info(f"[{account_name}] 🔍 Попытка {check_attempt+1}/8: кнопка лайка не найдена, ждём...")

            if not like_found:
                logging.warning(f"[{account_name}] ⚠️ Кнопка лайка не найдена после перезагрузки. ERROR.")
                return "ERROR"

            if like_present:
                logging.info(f"[{account_name}] ✅ Лайк на месте после перезагрузки. Теневого бана НЕТ.")
                return "OK"
            else:
                logging.warning(f"[{account_name}] ❌ Лайк исчез после перезагрузки! Обнаружен ТЕНЕВОЙ БАН!")
                return "SHADOW_BAN"

    except Exception as e:
        logging.error(f"[{account_name}] Исключение при проверке теневого бана: {e}")
        return "ERROR"
    finally:
        active_statuses.pop(f"ShadowCheck_{account_name}", None)
        try:
            api_key = await get_api_key()
            stop_url = f"http://127.0.0.1:{ADSPOWER_API_PORT}/api/v1/browser/stop?user_id={adspower_id}&api_key={api_key}"
            requests.get(stop_url, timeout=10)
            logging.info(f"[{account_name}] 🛑 Профиль AdsPower закрыт после проверки.")
        except:
            pass



async def _apply_shadow_ban_result(res, name, ads_id, tiktok_nick):
    """Обновляет БД и шлёт алерт по результату проверки."""
    if res == "SHADOW_BAN":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE accounts SET status = 'shadow_banned' WHERE name = ?", (name,))
            await db.commit()
        nick_str = f" @{tiktok_nick}" if tiktok_nick else ""
        await send_to_all_admins(
            f"⚠️ **[ВНИМАНИЕ] ОБНАРУЖЕН ТЕНЕВОЙ БАН!**\n\n"
            f"👤 Аккаунт: `{name}`{nick_str}\n"
            f"🚫 Статус изменен на `shadow_banned`. Исключён из работы!"
        )
    elif res == "PROFILE_BROKEN":
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("UPDATE accounts SET status = 'broken' WHERE name = ?", (name,))
            await db.commit()
        await send_to_all_admins(
            f"⚠️ **AdsPower-профиль не найден!**\n\n"
            f"👤 Аккаунт: `{name}` (ads_id `{ads_id}`)\n"
            f"🚫 Статус изменен на `broken`. Проверь AdsPower и обнови привязку."
        )


async def check_all_accounts_shadow_ban():
    """Плановая проверка всех активных аккаунтов на теневой бан (каждые 15 минут)."""
    if active_statuses:
        busy_tasks = [k for k in active_statuses.keys() if not k.startswith("ShadowCheck_")]
        if busy_tasks:
            logging.info(f"⏸ Плановая проверка теневого бана отложена — идут активные задачи: {busy_tasks}")
            return

    if shadow_check_lock.locked():
        logging.info("Проверка на теневой бан уже выполняется.")
        return

    async with shadow_check_lock:
        logging.info("🕵️ Начинаем плановую проверку аккаунтов на теневой бан (лайк-тест)...")
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute("SELECT name, session_file, tiktok_nickname FROM accounts WHERE status = 'active'")
            accounts = await cursor.fetchall()

        if not accounts:
            logging.info("Нет активных аккаунтов для проверки.")
            return

        for name, ads_id, tiktok_nick in accounts:
            res = await check_account_shadow_ban(name, ads_id)
            active_statuses.pop(f"ShadowCheck_{name}", None)
            await _apply_shadow_ban_result(res, name, ads_id, tiktok_nick)


async def check_all_accounts_shadow_ban_manual(message: Message):
    """Ручная проверка всех активных аккаунтов на теневой бан по нажатию кнопки."""
    if shadow_check_lock.locked():
        await message.answer("🕵️‍♂️ **Проверка на теневой бан уже запущена!** Пожалуйста, подождите её окончания.")
        return

    async with shadow_check_lock:
        msg = await message.answer(
            "🕵️‍♂️ **Начинаю проверку активных аккаунтов на теневой бан...**\n\n"
            "📋 **Метод:** лайк-тест с реальным кликом мыши\n"
            "1️⃣ Заходим на живое видео из /foryou\n"
            "2️⃣ Реальный клик мыши по кнопке лайка\n"
            "3️⃣ Ждём подтверждения от сервера TikTok\n"
            "4️⃣ Ждём 15 секунд\n"
            "5️⃣ Перезагружаем страницу\n"
            "6️⃣ Проверяем — стоит ли лайк\n\n"
            "⏳ _Это может занять несколько минут..._"
        )
        async with aiosqlite.connect(DB_NAME) as db:
            cursor = await db.execute("SELECT name, session_file, tiktok_nickname FROM accounts WHERE status = 'active'")
            accounts = await cursor.fetchall()

        if not accounts:
            await msg.edit_text("🤷‍♂️ **Нет активных аккаунтов для проверки.**")
            return

        checked = 0
        banned = 0
        ok = 0
        broken = 0
        errors = 0

        await msg.edit_text(
            f"🕵️‍♂️ **Проверка запущена!**\n"
            f"Метод: `лайк-тест (реальный клик)` | Аккаунтов: `{len(accounts)}`\n"
            f"_Это может занять несколько минут._"
        )

        for name, ads_id, tiktok_nick in accounts:
            res = await check_account_shadow_ban(name, ads_id)
            active_statuses.pop(f"ShadowCheck_{name}", None)
            checked += 1

            if res == "OK":
                ok += 1
                logging.info(f"[{name}] ✅ Теневого бана нет.")
            elif res == "SHADOW_BAN":
                banned += 1
                nick_str = f" @{tiktok_nick}" if tiktok_nick else ""
                await send_to_all_admins(
                    f"⚠️ **[ТЕНЕВОЙ БАН]** Обнаружен бан на аккаунте `{name}`{nick_str}!\n"
                    f"🚫 Статус изменен на `shadow_banned`, аккаунт отключен."
                )
            elif res == "PROFILE_BROKEN":
                broken += 1
            else:
                errors += 1

            await _apply_shadow_ban_result(res, name, ads_id, tiktok_nick)

        await msg.answer(
            f"🕵️‍♂️ **Проверка на теневой бан завершена!**\n\n"
            f"• Проверено аккаунтов: `{checked}`\n"
            f"• ✅ Без бана (активны): `{ok}`\n"
            f"• ⚠️ Обнаружен теневой бан: `{banned}`\n"
            f"• 🔧 Сломанные AdsPower-профили: `{broken}`\n"
            f"• ❓ Ошибки проверки: `{errors}`"
        )



async def get_working_status_text():
    active_lines = []
    if active_statuses:
        for name, status in active_statuses.items():
            active_lines.append(f"👤 **{name}**: {status}")

    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT status, COUNT(*) FROM tasks GROUP BY status")
        tasks_stats = {row[0]: row[1] for row in await cursor.fetchall()}
        cursor = await db.execute("SELECT status, COUNT(*) FROM dialogues GROUP BY status")
        dial_stats = {row[0]: row[1] for row in await cursor.fetchall()}
        cursor = await db.execute("SELECT status, COUNT(*) FROM accounts GROUP BY status")
        acc_stats = {row[0]: row[1] for row in await cursor.fetchall()}
        cursor = await db.execute(
            "SELECT project_name, comment_text FROM dialogues "
            "WHERE status IN ('pending_comment','processing_first','waiting_reply','processing_reply') "
            "ORDER BY id DESC LIMIT 3"
        )
        upcoming_dialogs = await cursor.fetchall()

    if not active_statuses:
        pending_total = tasks_stats.get('pending', 0)
        if upcoming_dialogs:
            for proj, txt in upcoming_dialogs:
                short = (txt or '')[:40]
                active_lines.append(f"🗣 Диалог в очереди ({proj}): `{short}`")
            active_lines.append("_(планировщик подберёт в ближайшие 30 сек)_")
        elif pending_total > 0:
            active_lines.append(f"📥 В очереди задач: {pending_total} — планировщик подберёт в ближайшие 30 сек.")
        else:
            active_lines.append("💤 В данный момент активных процессов нет (все боты спят).")

    text = (
        "📊 **ТЕКУЩИЙ СТАТУС РАБОТЫ ФЕРМЫ**\n\n"
        "⚡ **Активные процессы в реальном времени:**\n" + "\n".join(active_lines) + "\n\n"
        "📅 **Очередь общих Задач (комменты/лайки):**\n"
        f"• В ожидании (pending): `{tasks_stats.get('pending', 0)}` шт.\n"
        f"• В процессе (processing): `{tasks_stats.get('processing', 0)}` шт.\n"
        f"• Завершено успешно: `{tasks_stats.get('completed', 0)}` шт.\n"
        f"• Ошибки: `{tasks_stats.get('failed', 0)}` шт.\n\n"
        "🗣 **Кампания Диалогов:**\n"
        f"• Ожидают затравки: `{dial_stats.get('pending_comment', 0)}` шт.\n"
        f"• Заброска затравки: `{dial_stats.get('processing_first', 0)}` шт.\n"
        f"• Ждут ответа от цели: `{dial_stats.get('waiting_reply', 0)}` шт.\n"
        f"• Успешные диалоги: `{dial_stats.get('completed', 0)}` шт.\n"
        f"• Сбой затравки: `{dial_stats.get('failed_first', 0)}` шт.\n"
        f"• Сбой ответа: `{dial_stats.get('failed_reply', 0)}` шт.\n\n"
        "👥 **Состояние аккаунтов:**\n"
        f"• Активные (в работе): `{acc_stats.get('active', 0)}` шт.\n"
        f"• Теневой бан: `{acc_stats.get('shadow_banned', 0)}` шт.\n"
        f"• Сломанные профили: `{acc_stats.get('broken', 0)}` шт."
    )
    return text


@router.message(StateFilter("*"), F.text == "📊 Статус работы")
async def cmd_persistent_status(message: Message, state: FSMContext):
    try:
        text = await get_working_status_text()
        await message.answer(text, parse_mode="Markdown", reply_markup=kb_persistent_status())
    except Exception as e:
        logging.exception(f"Ошибка cmd_persistent_status: {e}")
        await message.answer(f"❌ Не удалось собрать статус: `{e}`", parse_mode="Markdown", reply_markup=kb_persistent_status())


@router.callback_query(F.data == "action_check_shadow_ban")
async def cb_check_shadow_ban(callback: CallbackQuery):
    await callback.answer("Запуск проверки...")
    asyncio.create_task(check_all_accounts_shadow_ban_manual(callback.message))



# ================= ГЛАВНОЕ МЕНЮ И НАВИГАЦИЯ =================
@router.message(Command("start", "help"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM projects WHERE status='active'")
        projects_count = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM accounts WHERE status='active'")
        accounts_count = (await cursor.fetchone())[0]

    text = (
        "👋 **Добро пожаловать в панель управления TikTok!**\n\n"
        "📊 **Текущее состояние:**\n"
        f"• Активных проектов: {projects_count}\n"
        f"• Аккаунтов в работе: {accounts_count}\n\n"
        "Здесь вы можете управлять проектами, добавлять профили AdsPower и запускать накрутку/парсинг.\n\n"
        "👇 _Выберите нужный раздел в меню ниже:_"
    )
    await message.answer("🤖 Бот запущен и готов к работе!", reply_markup=kb_persistent_status())
    await message.answer(text, parse_mode="Markdown", reply_markup=kb_main_menu())


@router.callback_query(F.data == "menu_main")
async def cb_menu_main(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM projects WHERE status='active'")
        projects_count = (await cursor.fetchone())[0]
        cursor = await db.execute("SELECT COUNT(*) FROM accounts WHERE status='active'")
        accounts_count = (await cursor.fetchone())[0]

    text = (
        "👋 **Добро пожаловать в панель управления TikTok!**\n\n"
        "📊 **Текущее состояние:**\n"
        f"• Активных проектов: {projects_count}\n"
        f"• Аккаунтов в работе: {accounts_count}\n\n"
        "👇 _Выберите нужный раздел в меню ниже:_"
    )
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb_main_menu())
    await callback.answer()


@router.callback_query(F.data == "menu_manage")
async def cb_menu_manage(callback: CallbackQuery):
    await callback.message.edit_text("⚙️ **Управление проектами и аккаунтами**\nВыберите действие:", parse_mode="Markdown", reply_markup=kb_manage_menu())
    await callback.answer()


@router.callback_query(F.data == "menu_tasks")
async def cb_menu_tasks(callback: CallbackQuery):
    await callback.message.edit_text("🚀 **Задачи и Трафик**\nВыберите, что хотите запустить:", parse_mode="Markdown", reply_markup=kb_tasks_menu())
    await callback.answer()


@router.callback_query(F.data == "menu_stats")
async def cb_menu_stats(callback: CallbackQuery):
    await callback.message.edit_text("📊 **Статистика и Контроль**\nВыберите действие:", parse_mode="Markdown", reply_markup=kb_stats_menu())
    await callback.answer()


# ================= ОБРАБОТЧИКИ НАСТРОЕК И API =================
@router.callback_query(F.data == "ask_api_key")
async def cb_ask_api_key(callback: CallbackQuery, state: FSMContext):
    current_key = await get_api_key()
    text = (
        "🔑 **Настройка API AdsPower**\n\n"
        f"Текущий ключ: `{current_key}`\n\n"
        "Отправьте новый API ключ в чат, чтобы изменить его:"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_api_key)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_api_key), F.text)
async def process_api_key(message: Message, state: FSMContext):
    new_key = message.text.strip()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("UPDATE settings SET value = ? WHERE key = 'ads_api_key'", (new_key,))
        await db.commit()
    await message.answer(f"✅ API ключ успешно обновлен на:\n`{new_key}`", parse_mode="Markdown", reply_markup=kb_manage_menu())
    await state.clear()


# ================= ОБРАБОТЧИКИ ПРОЕКТОВ / АККАУНТОВ =================
@router.callback_query(F.data == "list_projects")
async def cb_list_projects(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT name FROM projects")
        rows = await cursor.fetchall()
        projects = [row[0] for row in rows]
    if not projects:
        await callback.answer("📂 У вас пока нет проектов.", show_alert=True)
        return
    await callback.message.edit_text("📂 **Ваши проекты:**\n_Нажмите на проект для управления_", parse_mode="Markdown", reply_markup=kb_dynamic_list(projects, "proj", "menu_manage"))
    await callback.answer()


@router.callback_query(F.data.startswith("proj_"))
async def cb_proj_info(callback: CallbackQuery):
    proj_name = callback.data.split("_")[1]
    await callback.message.edit_text(f"📁 **Проект:** `{proj_name}`\n\nВы хотите удалить этот проект?", parse_mode="Markdown", reply_markup=kb_delete_item("proj", proj_name, "list_projects"))
    await callback.answer()


@router.callback_query(F.data.startswith("del_proj_"))
async def cb_del_proj(callback: CallbackQuery):
    proj_name = callback.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM projects WHERE name = ?", (proj_name,))
        await db.execute("DELETE FROM accounts WHERE project_name = ?", (proj_name,))
        await db.commit()
    await callback.message.edit_text(f"✅ Проект `{proj_name}` и все его аккаунты удалены!", parse_mode="Markdown", reply_markup=kb_manage_menu())
    await callback.answer()


@router.callback_query(F.data == "list_accounts")
async def cb_list_accounts(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT name FROM accounts")
        rows = await cursor.fetchall()
        accounts = [row[0] for row in rows]
    if not accounts:
        await callback.answer("👥 У вас пока нет аккаунтов.", show_alert=True)
        return
    await callback.message.edit_text("👥 **Ваши аккаунты:**\n_Нажмите на аккаунт для управления_", parse_mode="Markdown", reply_markup=kb_dynamic_list(accounts, "acc", "menu_manage"))
    await callback.answer()


@router.callback_query(F.data.startswith("acc_"))
async def cb_acc_info(callback: CallbackQuery):
    acc_name = callback.data.split("_")[1]
    await callback.message.edit_text(f"👤 **Аккаунт:** `{acc_name}`\n\nВы хотите удалить этот профиль?", parse_mode="Markdown", reply_markup=kb_delete_item("acc", acc_name, "list_accounts"))
    await callback.answer()


@router.callback_query(F.data.startswith("del_acc_"))
async def cb_del_acc(callback: CallbackQuery):
    acc_name = callback.data.split("_")[2]
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM accounts WHERE name = ?", (acc_name,))
        await db.commit()
    await callback.message.edit_text(f"✅ Аккаунт `{acc_name}` удален!", parse_mode="Markdown", reply_markup=kb_manage_menu())
    await callback.answer()


@router.callback_query(F.data == "ask_create_project")
async def cb_ask_create_project(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("📝 Введите **имя нового проекта** (одним словом, например: `alpha`):", parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_project_name)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_project_name), F.text)
async def process_create_project(message: Message, state: FSMContext):
    proj_name = message.text.strip()
    try:
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO projects (name, status) VALUES (?, 'active')", (proj_name,))
            await db.commit()
        await message.answer(f"✅ Проект `{proj_name}` успешно создан!", reply_markup=kb_manage_menu())
    except Exception:
        await message.answer("❌ Ошибка (возможно проект уже существует).")
    await state.clear()


@router.callback_query(F.data == "ask_add_account")
async def cb_ask_add_account(callback: CallbackQuery, state: FSMContext):
    text = (
        "👤 **Добавление аккаунта**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ через пробел в формате:\n"
        "`[проект] [имя_аккаунта] [Ads_ID] [TikTok-ник]`\n\n"
        "Пример: `alpha bot1 j92kx2a @trade_crypto`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_account_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_account_data), F.text)
async def process_add_account(message: Message, state: FSMContext):
    try:
        parts = message.text.split(maxsplit=3)
        proj_name, account_name, ads_id = parts[0], parts[1], parts[2]
        tiktok_nickname = parts[3] if len(parts) > 3 else None
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO accounts (project_name, name, session_file, proxy, tiktok_nickname) VALUES (?, ?, ?, ?, ?)",
                (proj_name, account_name, ads_id, "adspower", tiktok_nickname)
            )
            await db.commit()
        nick_info = f"\nTikTok-ник: `{tiktok_nickname}`" if tiktok_nickname else "\n⚠️ TikTok-ник не указан (Ревизор не сможет проверять)"
        await message.answer(f"✅ Аккаунт `{account_name}` привязан к проекту `{proj_name}`!{nick_info}")
    except Exception:
        await message.answer("❌ Ошибка формата или базы данных. Проверьте правильность ввода.")
    await state.clear()



# ================= ОБРАБОТЧИКИ ЗАДАЧ =================
@router.callback_query(F.data == "ask_search")
async def cb_ask_search(callback: CallbackQuery, state: FSMContext):
    text = (
        "🔍 **Запуск парсера (В работу)**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ через пробел:\n"
        "`[проект] [кол-во видео] [мин_просмотров] [возраст_дней] [запрос или #хештег]`\n\n"
        "Пример: `alpha 10 5000 0 #машины`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_search_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_search_data), F.text)
async def process_search(message: Message, state: FSMContext):
    try:
        parts = message.text.split(None, 4)
        proj_name, target_count, min_views, age_filter, keyword = parts[0], int(parts[1]), int(parts[2]), parts[3].strip(), parts[4].strip()
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT name, session_file FROM accounts WHERE status = 'active' AND project_name = ? LIMIT 1", (proj_name,)) as cursor:
                acc = await cursor.fetchone()
        if not acc:
            await message.answer(f"❌ В проекте `{proj_name}` нет активных аккаунтов.")
            return
        account_name, adspower_id = acc
        await message.answer(
            f"🔍 **Запускаю умный поиск (В работу)!**\n"
            f"Слово: `{keyword}`\nФильтр просмотров: от `{min_views}`\nЦель: `{target_count}` шт.\n"
            f"📌 _Режим: ссылки добавятся в очередь задач проекта `{proj_name}`_\n\n"
            f"⏳ *Ищем...*",
            parse_mode="Markdown"
        )
        asyncio.create_task(tiktok_parser(account_name, adspower_id, proj_name, keyword, target_count, min_views, age_filter, message, export_only=False))
    except Exception:
        await message.answer("❌ Формат ошибки. Пример: `alpha 10 10000 0 трейдинг`")
    await state.clear()


@router.callback_query(F.data == "ask_grab")
async def cb_ask_grab(callback: CallbackQuery, state: FSMContext):
    text = (
        "🧲 **Сбор ссылок из Поиска в файл (.txt)**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ через пробел:\n"
        "`[проект] [кол-во видео] [мин_просмотров] [возраст_дней] [запрос или #хештег]`\n\n"
        "Пример: `alpha 100 5000 0 #машины`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_grab_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_grab_data), F.text)
async def process_grab(message: Message, state: FSMContext):
    try:
        parts = message.text.split(None, 4)
        proj_name, target_count, min_views, age_filter, keyword = parts[0], int(parts[1]), int(parts[2]), parts[3].strip(), parts[4].strip()
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT name, session_file FROM accounts WHERE status = 'active' AND project_name = ? LIMIT 1", (proj_name,)) as cursor:
                acc = await cursor.fetchone()
        if not acc:
            await message.answer(f"❌ В проекте `{proj_name}` нет активных аккаунтов.")
            return
        account_name, adspower_id = acc
        await message.answer(
            f"🧲 **Начинаю сбор базы (.txt)!**\n"
            f"Слово: `{keyword}`\nЦель: `{target_count}` шт.\n"
            f"📌 _Режим: получите файл со ссылками в конце_\n\n"
            f"⏳ *Ищем...*",
            parse_mode="Markdown"
        )
        asyncio.create_task(tiktok_parser(account_name, adspower_id, proj_name, keyword, target_count, min_views, age_filter, message, export_only=True))
    except Exception:
        await message.answer("❌ Формат ошибки. Пример: `alpha 100 10000 0 трейдинг`")
    await state.clear()


@router.callback_query(F.data == "ask_foryou")
async def cb_ask_foryou(callback: CallbackQuery, state: FSMContext):
    text = (
        "🌟 **Парсинг ленты Рекомендаций (For You)**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ через пробел:\n"
        "`[проект] [кол-во видео] [мин_просмотров]`\n\n"
        "Пример: `alpha 100 10000`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_foryou_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_foryou_data), F.text)
async def process_foryou(message: Message, state: FSMContext):
    try:
        parts = message.text.split()
        proj_name, target_count, min_views = parts[0], int(parts[1]), int(parts[2])
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT name, session_file FROM accounts WHERE status = 'active' AND project_name = ? LIMIT 1", (proj_name,)) as cursor:
                acc = await cursor.fetchone()
        if not acc:
            await message.answer(f"❌ В проекте `{proj_name}` нет активных аккаунтов.")
            return
        account_name, adspower_id = acc
        await message.answer(f"🌟 **Запускаю сбор из рекомендаций!**\nМин. просмотров: `{min_views}`\nЦель: `{target_count}` шт.\n\n⏳ *Листаем ленту...*", parse_mode="Markdown")
        asyncio.create_task(tiktok_parser(account_name, adspower_id, proj_name, "FORYOU", target_count, min_views, "0", message, export_only=True))
    except Exception:
        await message.answer("❌ Формат ошибки. Пример: `alpha 100 10000`")
    await state.clear()


@router.callback_query(F.data == "ask_boost")
async def cb_ask_boost(callback: CallbackQuery, state: FSMContext):
    text = (
        "🚀 **Вывод своего коммента в ТОП**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ в формате:\n"
        "`[проект] [ссылка] | [@ваш_ник] | [Текст]`\n\n"
        "Пример: `alpha https://vm... | @trade_crypto | Это супер!`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_boost_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_boost_data), F.text)
async def process_boost(message: Message, state: FSMContext):
    try:
        parts = message.text.split("|")
        args = parts[0].strip().split()
        if len(args) < 2:
            raise ValueError("Not enough arguments")
        proj_name, url = args[0], args[1]
        if len(parts) == 3:
            special_task_text = f"[BOOST] {parts[1].strip()} | {parts[2].strip()}"
        elif len(parts) == 2:
            special_task_text = f"[BOOST] {parts[1].strip()}"
        else:
            if len(args) > 2:
                special_task_text = f"[BOOST] {' '.join(args[2:])}"
            else:
                raise ValueError("No text provided")
        async with aiosqlite.connect(DB_NAME) as db:
            async with db.execute("SELECT COUNT(*) FROM accounts WHERE project_name=? AND status='active'", (proj_name,)) as cursor:
                accounts_count = (await cursor.fetchone())[0]
            if accounts_count == 0:
                await message.answer(f"❌ В проекте `{proj_name}` нет активных аккаунтов!")
                return
            for _ in range(accounts_count):
                await db.execute("INSERT INTO tasks (project_name, video_url, template_text) VALUES (?, ?, ?)", (proj_name, url, special_task_text))
            await db.commit()
        await message.answer(f"🚀 **Вывод в ТОП запущен!**\nПроект: `{proj_name}`\nАккаунтов в атаке: `{accounts_count}` шт.")
    except Exception:
        await message.answer("❌ Ошибка формата. Пример: `alpha https://vm... | @trade_crypto | Это супер!`")
    await state.clear()


@router.callback_query(F.data == "ask_masslike")
async def cb_ask_masslike(callback: CallbackQuery, state: FSMContext):
    text = (
        "❤️ **Масс-лайкинг комментариев**\n\n"
        "Отправьте данные ОДНИМ СООБЩЕНИЕМ в формате:\n"
        "`[проект] [ссылка_на_видео] [кол-во_лайков]`\n\n"
        "Пример: `alpha https://vm.tiktok... 15`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_masslike_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_masslike_data), F.text)
async def process_masslike(message: Message, state: FSMContext):
    try:
        parts = message.text.split()
        proj_name, url, likes_count = parts[0], parts[1], int(parts[2])
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute("INSERT INTO tasks (project_name, video_url, template_text) VALUES (?, ?, ?)", (proj_name, url, f"[MASS_LIKE:{likes_count}]"))
            await db.commit()
        await message.answer(f"❤️ **Масс-лайкинг запущен!**\nПроект: `{proj_name}`\nЦель: `{likes_count}` лайков.")
    except Exception:
        await message.answer("❌ Ошибка формата.")
    await state.clear()


@router.callback_query(F.data == "ask_dialogue")
async def cb_ask_dialogue(callback: CallbackQuery, state: FSMContext):
    text = (
        "🗣 **Настройка Диалога**\n\n"
        "**Вариант 1** — одно видео (текстом):\n"
        "`[проект] [url] | [Текст 1] | [Текст ответа] | [Мин задержки]`\n\n"
        "**Вариант 2** — много ссылок (файлом .txt):\n"
        "Прикрепите `.txt` файл со ссылками (по одной на строку).\n"
        "В подписи к файлу укажите: `[проект] | [Текст 1] | [Текст ответа] | [Мин задержки]`"
    )
    await callback.message.answer(text, parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_dialogue_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_dialogue_data), F.text)
async def process_dialogue(message: Message, state: FSMContext):
    try:
        parts = message.text.split("|")
        proj_and_url = parts[0].strip().split()
        proj_name, url = proj_and_url[0], proj_and_url[1]
        comment_text, reply_text, delay = parts[1].strip(), parts[2].strip(), int(parts[3].strip())
        async with aiosqlite.connect(DB_NAME) as db:
            await db.execute(
                "INSERT INTO dialogues (project_name, video_url, comment_text, reply_text, delay_minutes) VALUES (?, ?, ?, ?, ?)",
                (proj_name, url, comment_text, reply_text, delay)
            )
            await db.commit()
        await message.answer(
            f"🗣 **Диалог запланирован!**\nПроект: `{proj_name}`\n1️⃣ Сначала: `{comment_text}`\n⏳ Ждем: `{delay}` мин.\n2️⃣ Затем: `{reply_text}`",
            parse_mode="Markdown", reply_markup=kb_persistent_status()
        )
    except Exception:
        await message.answer("❌ Ошибка формата.", reply_markup=kb_persistent_status())
    await state.clear()



# ================= ОБРАБОТЧИКИ СТАТИСТИКИ И КОНТРОЛЯ =================
@router.callback_query(F.data == "action_status")
async def cb_action_status(callback: CallbackQuery):
    await callback.answer("Сбор статистики...")
    text = await get_working_status_text()
    await callback.message.edit_text(text, parse_mode="Markdown", reply_markup=kb_stats_menu())


@router.callback_query(F.data == "action_clear_queue")
async def cb_clear_queue(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("UPDATE tasks SET status = 'cancelled' WHERE status = 'pending'")
        deleted_count = cursor.rowcount
        await db.commit()
    await callback.message.edit_text(f"🧹 **Очередь очищена!**\nОтменено старых задач: `{deleted_count}` шт.", parse_mode="Markdown", reply_markup=kb_stats_menu())
    await callback.answer()


@router.callback_query(F.data == "action_export")
async def cb_action_export(callback: CallbackQuery):
    await callback.answer("Готовлю файл...", show_alert=False)
    async with aiosqlite.connect(DB_NAME) as db:
        cursor = await db.execute("SELECT project_name, video_url, template_text FROM tasks WHERE status='completed'")
        completed_tasks = await cursor.fetchall()
    if not completed_tasks:
        await callback.message.answer("🤷‍♂️ Нет успешно выполненных задач для выгрузки.")
        return
    csv_buffer = io.StringIO()
    writer = csv.writer(csv_buffer, delimiter=';')
    writer.writerow(['Проект', 'Ссылка на видео', 'Текст комментария'])
    for task in completed_tasks:
        writer.writerow([task[0], task[1], task[2]])
    export_file = BufferedInputFile(csv_buffer.getvalue().encode('utf-8-sig'), filename="export_all.csv")
    await callback.message.answer_document(export_file, caption=f"📊 **Таблица готова!**\nУспешных задач: `{len(completed_tasks)}` шт.", parse_mode="Markdown")


@router.callback_query(F.data == "ask_revisor")
async def cb_ask_revisor(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("🕵️ Введите **имя проекта** для проверки последних 10 комментов:", parse_mode="Markdown")
    await state.set_state(BotStates.waiting_for_revisor_data)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_revisor_data), F.text)
async def process_revisor(message: Message, state: FSMContext):
    proj_name = message.text.strip()
    await message.answer(f"🕵️‍♂️ **Ревизор запущен!** Проверяем проект `{proj_name}`...")
    asyncio.create_task(run_revisor(proj_name, message))
    await state.clear()


@router.callback_query(F.data == "menu_blacklist")
async def cb_menu_blacklist(callback: CallbackQuery):
    await callback.message.edit_text("🚫 **Управление Черным списком**", parse_mode="Markdown", reply_markup=kb_blacklist_menu())
    await callback.answer()


@router.callback_query(F.data == "ask_ban")
async def cb_ask_ban(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите **username** автора для добавления в ЧС (без @):")
    await state.set_state(BotStates.waiting_for_ban_username)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_ban_username), F.text)
async def process_ban(message: Message, state: FSMContext):
    username = message.text.replace('@', '').strip()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("INSERT OR IGNORE INTO blacklist (username) VALUES (?)", (username,))
        await db.commit()
    await message.answer(f"🚫 Автор `@{username}` добавлен в черный список.")
    await state.clear()


@router.callback_query(F.data == "ask_unban")
async def cb_ask_unban(callback: CallbackQuery, state: FSMContext):
    await callback.message.answer("Введите **username** автора для удаления из ЧС (без @):")
    await state.set_state(BotStates.waiting_for_unban_username)
    await callback.answer()


@router.message(StateFilter(BotStates.waiting_for_unban_username), F.text)
async def process_unban(message: Message, state: FSMContext):
    username = message.text.replace('@', '').strip()
    async with aiosqlite.connect(DB_NAME) as db:
        await db.execute("DELETE FROM blacklist WHERE username = ?", (username,))
        await db.commit()
    await message.answer(f"✅ Автор `@{username}` удален из черного списка.")
    await state.clear()


@router.callback_query(F.data == "action_show_blacklist")
async def cb_show_blacklist(callback: CallbackQuery):
    async with aiosqlite.connect(DB_NAME) as db:
        async with db.execute("SELECT username FROM blacklist") as cursor:
            banned_users = await cursor.fetchall()
    if not banned_users:
        await callback.message.answer("Черный список пуст.")
    else:
        text = "📜 **Черный список:**\n\n"
        for user in banned_users:
            text += f"• `@{user[0]}`\n"
        await callback.message.answer(text, parse_mode="Markdown")
    await callback.answer()


# ================= МАССОВАЯ ЗАГРУЗКА БАЗЫ (ФАЙЛ) =================
@router.message(F.document)
async def handle_document(message: Message, state: FSMContext):
    current_state = await state.get_state()
    document = message.document
    is_txt = document.file_name.endswith('.txt')
    is_json = document.file_name.endswith('.json')

    if current_state == BotStates.waiting_for_dialogue_data and is_txt:
        caption = message.caption or ""
        cap_parts = [p.strip() for p in caption.split("|")]
        if len(cap_parts) < 4 or not cap_parts[0]:
            await message.answer(
                "❌ Укажи параметры в подписи к файлу!\n"
                "Формат: `[проект] | [Текст 1] | [Текст ответа] | [Мин задержки]`",
                parse_mode="Markdown"
            )
            return
        proj_name, comment_txt, reply_txt = cap_parts[0], cap_parts[1], cap_parts[2]
        try:
            delay = int(cap_parts[3])
        except ValueError:
            await message.answer("❌ Задержка должна быть числом (минуты). Пример: `5`")
            return
        msg = await message.answer(f"⏳ Загружаю ссылки для диалога в проект `{proj_name}`...")
        try:
            file_io = io.BytesIO()
            await bot.download(document, destination=file_io)
            content_str = file_io.getvalue().decode('utf-8')
            added_count = 0
            async with aiosqlite.connect(DB_NAME) as db:
                for line in content_str.splitlines():
                    url = line.strip()
                    if not url or 'tiktok.com' not in url:
                        continue
                    await db.execute(
                        "INSERT INTO dialogues (project_name, video_url, comment_text, reply_text, delay_minutes) VALUES (?, ?, ?, ?, ?)",
                        (proj_name, url, comment_txt, reply_txt, delay)
                    )
                    added_count += 1
                await db.commit()
            await msg.edit_text(
                f"🗣 **Диалоги запланированы!**\nПроект: `{proj_name}`\nВидео: `{added_count}` шт.\n"
                f"1️⃣ Первый комент: `{comment_txt}`\n⏳ Задержка: `{delay}` мин.\n2️⃣ Ответ: `{reply_txt}`",
                parse_mode="Markdown"
            )
        except Exception as e:
            await msg.edit_text(f"❌ Ошибка чтения файла:\n`{e}`", parse_mode="Markdown")
        await state.clear()
        return

    await state.clear()
    if not any([is_txt, is_json]):
        await message.answer("❌ Я принимаю только `.txt`, `.json`")
        return
    caption = message.caption or ""
    parts = caption.split(" ", 1)
    if len(parts) < 1 or not parts[0]:
        await message.answer("❌ Укажи имя проекта в подписи! Пример: `alpha {Круто|Вау}`")
        return
    proj_name = parts[0]
    default_text = parts[1] if len(parts) > 1 else "{Супер|Круто|Ого|Класс}"
    msg = await message.answer(f"⏳ Загружаю файл в проект `{proj_name}`...")
    try:
        file_io = io.BytesIO()
        await bot.download(document, destination=file_io)
        content_str = file_io.getvalue().decode('utf-8')
        added_count = 0
        async with aiosqlite.connect(DB_NAME) as db:
            if is_json:
                data = json.loads(content_str)
                for item in data:
                    url = item.get("url")
                    text = item.get("text", default_text)
                    if url and "tiktok.com" in url:
                        await db.execute("INSERT INTO tasks (project_name, video_url, template_text) VALUES (?, ?, ?)", (proj_name, url, text))
                        added_count += 1
            elif is_txt:
                for line in content_str.splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    line_parts = line.split(" ", 1)
                    url = line_parts[0]
                    text = line_parts[1] if len(line_parts) > 1 else default_text
                    if "tiktok.com" in url:
                        await db.execute("INSERT INTO tasks (project_name, video_url, template_text) VALUES (?, ?, ?)", (proj_name, url, text))
                        added_count += 1
            await db.commit()
        await msg.edit_text(f"✅ **База ссылок загружена!**\nПроект: `{proj_name}`\nДобавлено: `{added_count}` шт.", parse_mode="Markdown")
    except Exception as e:
        await msg.edit_text(f"❌ Ошибка чтения файла:\n`{e}`", parse_mode="Markdown")


# ================= КОМАНДЫ (СЛЭШИ) =================
@router.message(Command("status"))
async def cmd_status_slash(message: Message, state: FSMContext):
    text = await get_working_status_text()
    await message.answer(text, parse_mode="Markdown")


@router.message(Command("dialogue", "create_project", "stop_campaign", "start_campaign", "add_account", "task", "ban", "unban", "blacklist", "masslike", "boost", "export", "search", "revisor"))
async def cmd_legacy_fallback(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("⚠️ Все старые слэш-команды теперь перенесены в удобное кнопочное меню! Просто напишите `/start` и выберите нужное действие.")


# ================= ЗАПУСК =================
async def main():
    await init_db()
    scheduler.add_job(process_task_queue, 'interval', seconds=30)
    scheduler.start()
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
