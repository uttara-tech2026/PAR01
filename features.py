import os
import re
import io
import time
import random
import hashlib
import asyncio
import logging
from datetime import datetime, date
import asyncpg

from aiogram import Router, F, Bot
from aiogram.types import (
    Message,
    CallbackQuery,
    ChatMemberUpdated,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    ReplyKeyboardMarkup,
    KeyboardButton
)
from aiogram.filters import Command, CommandStart, StateFilter, BaseFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

router = Router()

db_pool: asyncpg.Pool = None

# Trackers for live progress messages
broadcast_notif_tracker: dict[str, int] = {}
flezen_conf_tracker: dict[int, int] = {}

# Regex matching Telegram channel, group, private join, and message links
TG_LINK_REGEX = re.compile(
    r"(?:https?:\/\/)?(?:www\.)?(?:t\.me|telegram\.me|telegram\.dog)\/(?:[a-zA-Z0-9_+\/]+|\+[a-zA-Z0-9_-]+)"
)

# ---------------------------------------------------------------------------
# FSM STATES
# ---------------------------------------------------------------------------
class AdminStates(StatesGroup):
    waiting_for_admin_id = State()
    waiting_for_emp_manual_add = State()
    waiting_for_dest_target = State()
    waiting_for_dest_custom_delay = State()
    waiting_for_notif_channel = State()
    waiting_for_new_category = State()
    waiting_for_fixed_delay = State()
    waiting_for_random_delay = State()
    waiting_for_earning_rate = State()
    waiting_for_req_nickname = State()
    waiting_for_editor_ref = State()
    waiting_for_rejection_notes = State()
    waiting_for_admin_self_edit = State()
    waiting_for_reedit_title = State()
    multi_link_processing = State()

class UploaderStates(StatesGroup):
    uploading_videos = State()
    waiting_for_category = State()

class SorterStates(StatesGroup):
    active_sorting = State()

class EditorStates(StatesGroup):
    waiting_for_edited_video = State()
    waiting_for_video_title = State()

class FlezenStates(StatesGroup):
    waiting_for_post = State()

# ---------------------------------------------------------------------------
# LIVE UPLOAD RATE-LIMITING & MESSAGE DEDUPLICATION MANAGER
# ---------------------------------------------------------------------------
class LiveUploadManager:
    """Manages throttled updates and message cleanup for batch uploads."""
    def __init__(self):
        self.lock = asyncio.Lock()
        self.sessions: dict[int, dict] = {}
        self.dup_channel_msgs: dict[int, int] = {}
        self.dup_user_msgs: dict[int, int] = {}

    def init_session(self, link_id: int, user_id: int, chat_id: int, nick: str, count: int, user_msg_id: int):
        self.sessions[link_id] = {
            "user_id": user_id,
            "chat_id": chat_id,
            "nick": nick,
            "count": count,
            "last_channel_update": 0.0,
            "last_user_update": 0.0,
            "channel_msg_id": None,
            "user_msg_ids": [user_msg_id] if user_msg_id else [],
            "channel_task": None,
            "user_task": None,
            "pending_channel": False,
            "pending_user": False
        }

    async def record_upload(self, bot: Bot, link_id: int, user_id: int, chat_id: int, nick: str, new_count: int):
        async with self.lock:
            if link_id not in self.sessions:
                self.sessions[link_id] = {
                    "user_id": user_id,
                    "chat_id": chat_id,
                    "nick": nick,
                    "count": new_count,
                    "last_channel_update": 0.0,
                    "last_user_update": 0.0,
                    "channel_msg_id": None,
                    "user_msg_ids": [],
                    "channel_task": None,
                    "user_task": None,
                    "pending_channel": False,
                    "pending_user": False
                }
            sess = self.sessions[link_id]
            sess["count"] = new_count
            sess["nick"] = nick

            now = time.time()

            # 1. Throttle Notification Log Channel (every 1.5s)
            if now - sess["last_channel_update"] >= 1.5:
                await self._dispatch_channel_log(bot, link_id)
            else:
                sess["pending_channel"] = True
                if not sess["channel_task"] or sess["channel_task"].done():
                    sess["channel_task"] = asyncio.create_task(self._delayed_channel_worker(bot, link_id))

            # 2. Throttle Uploader Chat message (every 1.5s)
            if now - sess["last_user_update"] >= 1.5:
                await self._dispatch_user_counter(bot, link_id)
            else:
                sess["pending_user"] = True
                if not sess["user_task"] or sess["user_task"].done():
                    sess["user_task"] = asyncio.create_task(self._delayed_user_worker(bot, link_id))

    async def _delayed_channel_worker(self, bot: Bot, link_id: int):
        await asyncio.sleep(1.5)
        async with self.lock:
            if link_id in self.sessions and self.sessions[link_id]["pending_channel"]:
                await self._dispatch_channel_log(bot, link_id)

    async def _delayed_user_worker(self, bot: Bot, link_id: int):
        await asyncio.sleep(1.5)
        async with self.lock:
            if link_id in self.sessions and self.sessions[link_id]["pending_user"]:
                await self._dispatch_user_counter(bot, link_id)

    async def _dispatch_channel_log(self, bot: Bot, link_id: int):
        sess = self.sessions.get(link_id)
        if not sess:
            return
        sess["pending_channel"] = False
        sess["last_channel_update"] = time.time()

        log_channel = await get_setting("notification_log_channel", "") or os.getenv("NOTIFICATION_LOG_CHANNEL", "")
        if not log_channel:
            return

        target_chat = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
        text = (
            f"📹 <b>[Live Upload In Progress]</b>\n"
            f"• <b>Uploader:</b> {sess['nick']} (<code>{sess['user_id']}</code>)\n"
            f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
            f"• <b>Total Videos Uploaded:</b> <code>{sess['count']}</code>"
        )

        old_msg_id = sess["channel_msg_id"]
        if old_msg_id:
            try:
                await bot.edit_message_text(chat_id=target_chat, message_id=old_msg_id, text=text, parse_mode="HTML")
                return
            except Exception:
                try:
                    await bot.delete_message(chat_id=target_chat, message_id=old_msg_id)
                except Exception:
                    pass

        try:
            new_msg = await bot.send_message(chat_id=target_chat, text=text, parse_mode="HTML")
            sess["channel_msg_id"] = new_msg.message_id
        except Exception as e:
            logging.error(f"Error sending log channel update: {e}")

    async def _dispatch_user_counter(self, bot: Bot, link_id: int):
        sess = self.sessions.get(link_id)
        if not sess:
            return
        sess["pending_user"] = False
        sess["last_user_update"] = time.time()

        task_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Done Uploading All Videos", callback_data="uploader_done")],
            [InlineKeyboardButton(text="⚠️ Link Expired (Report to Admin)", callback_data=f"uploader_expired:{link_id}")]
        ])

        # Delete older counter messages in uploader chat
        old_ids = sess["user_msg_ids"][:]
        sess["user_msg_ids"] = []
        for oid in old_ids:
            try:
                await bot.delete_message(chat_id=sess["chat_id"], message_id=oid)
            except Exception:
                pass

        try:
            new_msg = await bot.send_message(
                chat_id=sess["chat_id"],
                text=f"📹 <b>Video uploaded for this link count:</b> <code>{sess['count']}</code>\n<i>(Send all videos, then tap Done or send /done)</i>",
                reply_markup=task_kb,
                parse_mode="HTML"
            )
            sess["user_msg_ids"].append(new_msg.message_id)
        except Exception as e:
            logging.error(f"Error updating uploader counter: {e}")

    async def log_duplicate_attempt(self, bot: Bot, link_id: int, user_id: int, nick: str, chat_id: int):
        async with self.lock:
            prev_dup_id = self.dup_channel_msgs.get(link_id)
            log_channel = await get_setting("notification_log_channel", "") or os.getenv("NOTIFICATION_LOG_CHANNEL", "")
            if log_channel and prev_dup_id:
                target_log = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
                try:
                    await bot.delete_message(chat_id=target_log, message_id=prev_dup_id)
                except Exception:
                    pass

            notif_msg = await send_notification_log(
                bot,
                f"⚠️ <b>[Duplicate Video Attempt Flagged]</b>\n"
                f"• <b>Uploader:</b> {nick} (<code>{user_id}</code>)\n"
                f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
                f"• Video upload rejected automatically."
            )
            if notif_msg:
                self.dup_channel_msgs[link_id] = notif_msg.message_id

            prev_user_dup = self.dup_user_msgs.get(user_id)
            if prev_user_dup:
                try:
                    await bot.delete_message(chat_id=chat_id, message_id=prev_user_dup)
                except Exception:
                    pass

            try:
                warn = await bot.send_message(
                    chat_id=chat_id,
                    text="⚠️ <b>Duplicate detected:</b> This video has already been uploaded! Skipped.",
                    parse_mode="HTML"
                )
                self.dup_user_msgs[user_id] = warn.message_id
            except Exception:
                pass

    async def finalize_task(self, bot: Bot, link_id: int, chat_id: int):
        async with self.lock:
            sess = self.sessions.pop(link_id, None)
            if sess:
                if sess["channel_task"]:
                    sess["channel_task"].cancel()
                if sess["user_task"]:
                    sess["user_task"].cancel()

                if sess["channel_msg_id"]:
                    log_channel = await get_setting("notification_log_channel", "") or os.getenv("NOTIFICATION_LOG_CHANNEL", "")
                    if log_channel:
                        target_log = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
                        try:
                            await bot.delete_message(chat_id=target_log, message_id=sess["channel_msg_id"])
                        except Exception:
                            pass

                for uid in sess["user_msg_ids"]:
                    try:
                        await bot.delete_message(chat_id=chat_id, message_id=uid)
                    except Exception:
                        pass

            dup_chan_id = self.dup_channel_msgs.pop(link_id, None)
            if dup_chan_id:
                log_channel = await get_setting("notification_log_channel", "") or os.getenv("NOTIFICATION_LOG_CHANNEL", "")
                if log_channel:
                    target_log = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
                    try:
                        await bot.delete_message(chat_id=target_log, message_id=dup_chan_id)
                    except Exception:
                        pass

upload_manager = LiveUploadManager()

# ---------------------------------------------------------------------------
# URL FORMATTER HELPER
# ---------------------------------------------------------------------------
def make_clickable_url(url: str) -> str:
    clean = url.strip()
    if not clean.startswith(("http://", "https://")):
        return f"https://{clean}"
    return clean

# ---------------------------------------------------------------------------
# NEON DB (POSTGRESQL) CONNECTION POOL & HELPERS
# ---------------------------------------------------------------------------
async def get_pool() -> asyncpg.Pool:
    global db_pool
    if db_pool is None:
        database_url = os.getenv("DATABASE_URL") or os.getenv("DATABASE_PATH")
        if not database_url:
            raise ValueError("DATABASE_URL environment variable is missing! Please configure your Neon DB URI in .env.")
        
        clean_url = database_url.split("?")[0]
        ssl_mode = "require" if ("neon.tech" in database_url or "sslmode=require" in database_url) else None

        db_pool = await asyncpg.create_pool(
            dsn=clean_url,
            ssl=ssl_mode,
            min_size=1,
            max_size=10
        )
    return db_pool

async def execute(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.execute(query, *args)

async def fetch(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetch(query, *args)

async def fetchrow(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(query, *args)

async def fetchval(query: str, *args):
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchval(query, *args)

# ---------------------------------------------------------------------------
# NOTIFICATION LOG DISPATCHER
# ---------------------------------------------------------------------------
async def send_notification_log(bot: Bot, text: str) -> Message | None:
    try:
        log_channel = await get_setting("notification_log_channel", "")
        if not log_channel:
            log_channel = os.getenv("NOTIFICATION_LOG_CHANNEL", "")
        
        if log_channel:
            chat_id = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
            return await bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML")
    except Exception as err:
        logging.error(f"Error dispatching notification log: {err}")
    return None

# ---------------------------------------------------------------------------
# DATABASE INITIALIZATION
# ---------------------------------------------------------------------------
async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY
            );
            CREATE TABLE IF NOT EXISTS employees (
                user_id BIGINT PRIMARY KEY,
                role TEXT DEFAULT 'uploader',
                nickname TEXT DEFAULT 'Employee',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS user_requests (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                status TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            );
            CREATE TABLE IF NOT EXISTS categories (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE,
                use_count INT DEFAULT 0,
                last_used_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS destinations (
                id SERIAL PRIMARY KEY,
                layer_type TEXT DEFAULT 'prelayered',
                target_chat TEXT,
                custom_delay INT DEFAULT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS bot_chats (
                chat_id BIGINT PRIMARY KEY,
                chat_title TEXT,
                chat_type TEXT,
                is_admin BOOLEAN DEFAULT TRUE,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS links (
                id SERIAL PRIMARY KEY,
                url TEXT,
                link_type TEXT,
                is_urgent BOOLEAN DEFAULT FALSE,
                status TEXT DEFAULT 'pending',
                locked_by BIGINT,
                locked_at TIMESTAMPTZ,
                completed_by BIGINT,
                completed_at TIMESTAMPTZ,
                category TEXT,
                video_count INT DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS videos (
                id SERIAL PRIMARY KEY,
                link_id INT REFERENCES links(id) ON DELETE CASCADE,
                uploader_id BIGINT,
                sorter_id BIGINT,
                verifier_id BIGINT,
                file_id TEXT,
                file_unique_id TEXT UNIQUE,
                caption TEXT,
                status TEXT DEFAULT 'pending',
                verification_status TEXT DEFAULT 'pending',
                created_at TIMESTAMPTZ DEFAULT NOW(),
                sorted_at TIMESTAMPTZ,
                posted_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS editor_tasks (
                id SERIAL PRIMARY KEY,
                ref_type TEXT,
                ref_content TEXT,
                ref_caption TEXT,
                status TEXT DEFAULT 'pending',
                assigned_to BIGINT,
                submitted_file_id TEXT,
                video_title TEXT,
                rejection_notes TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                submitted_at TIMESTAMPTZ,
                reviewed_at TIMESTAMPTZ
            );
            CREATE TABLE IF NOT EXISTS flezen_posts (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                content_type TEXT,
                file_unique_id TEXT UNIQUE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE TABLE IF NOT EXISTS duplicate_logs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
        """)

        try:
            await conn.execute("ALTER TABLE links ADD COLUMN IF NOT EXISTS is_urgent BOOLEAN DEFAULT FALSE;")
            await conn.execute("ALTER TABLE destinations ADD COLUMN IF NOT EXISTS layer_type TEXT DEFAULT 'prelayered';")
            await conn.execute("ALTER TABLE destinations DROP CONSTRAINT IF EXISTS destinations_target_chat_key;")
            await conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_dest_layer_chat ON destinations (layer_type, target_chat);")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS caption TEXT;")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS uploader_id BIGINT;")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS sorter_id BIGINT;")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS sorted_at TIMESTAMPTZ;")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS verification_status TEXT DEFAULT 'pending';")
            await conn.execute("ALTER TABLE videos ADD COLUMN IF NOT EXISTS verifier_id BIGINT;")
            await conn.execute("ALTER TABLE editor_tasks ADD COLUMN IF NOT EXISTS video_title TEXT;")
            await conn.execute("ALTER TABLE editor_tasks ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMPTZ;")
            await conn.execute("ALTER TABLE editor_tasks ADD COLUMN IF NOT EXISTS reviewed_at TIMESTAMPTZ;")

            # Automatically rescue existing stuck videos into the sorter queue
            await conn.execute("""
                UPDATE videos
                SET status = 'pending_sort'
                WHERE status = 'pending_broadcast'
                  AND link_id IN (SELECT id FROM links WHERE status = 'completed');
            """)
        except Exception as e:
            logging.warning(f"Migration note: {e}")

        master_admin = os.getenv("ADMIN_ID")
        if master_admin and master_admin.strip().isdigit():
            await conn.execute(
                "INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING",
                int(master_admin.strip())
            )

        await conn.execute("INSERT INTO settings (key, value) VALUES ('delay_mode', 'fixed') ON CONFLICT (key) DO NOTHING")
        await conn.execute("INSERT INTO settings (key, value) VALUES ('fixed_delay', '60') ON CONFLICT (key) DO NOTHING")
        await conn.execute("INSERT INTO settings (key, value) VALUES ('random_min', '30') ON CONFLICT (key) DO NOTHING")
        await conn.execute("INSERT INTO settings (key, value) VALUES ('random_max', '120') ON CONFLICT (key) DO NOTHING")
        await conn.execute("INSERT INTO settings (key, value) VALUES ('earning_per_link', '10') ON CONFLICT (key) DO NOTHING")

        initial_cats = ["Action", "Comedy", "Drama", "Music", "Tutorial", "Trending"]
        for cat in initial_cats:
            await conn.execute("INSERT INTO categories (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", cat)

# ---------------------------------------------------------------------------
# DB HELPERS & FILTERS
# ---------------------------------------------------------------------------
async def is_admin(user_id: int) -> bool:
    master_admin = os.getenv("ADMIN_ID")
    if master_admin and master_admin.strip().isdigit() and int(master_admin.strip()) == user_id:
        return True
    val = await fetchval("SELECT 1 FROM admins WHERE user_id = $1", user_id)
    return val is not None

async def is_employee(user_id: int) -> bool:
    val = await fetchval("SELECT 1 FROM employees WHERE user_id = $1", user_id)
    return val is not None

async def get_employee_role(user_id: int) -> str | None:
    return await fetchval("SELECT role FROM employees WHERE user_id = $1", user_id)

async def is_organizer(bot: Bot, chat_id: int, user_id: int) -> bool:
    if await is_admin(user_id):
        return True
    role = await get_employee_role(user_id)
    if role == "verifier":
        return True
    try:
        member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
        if member.status in ["creator", "administrator"]:
            return True
    except Exception:
        pass
    return False

class IsAdmin(BaseFilter):
    async def __call__(self, event: Message | CallbackQuery) -> bool:
        return await is_admin(event.from_user.id)

class IsFlezenScheduler(BaseFilter):
    async def __call__(self, message: Message) -> bool:
        role = await get_employee_role(message.from_user.id)
        return role == "flezen_scheduler"

async def get_setting(key: str, default: str = "") -> str:
    val = await fetchval("SELECT value FROM settings WHERE key = $1", key)
    return val if val is not None else default

async def set_setting(key: str, value: str):
    await execute(
        "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value",
        key, value
    )

# ---------------------------------------------------------------------------
# KEYBOARDS
# ---------------------------------------------------------------------------
async def get_admin_panel_kb():
    pending_requests = await fetchval("SELECT COUNT(*) FROM user_requests WHERE status = 'pending'") or 0
    pending_edits = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE status = 'submitted'") or 0
    expired_links = await fetchval("SELECT COUNT(*) FROM links WHERE status = 'expired'") or 0
    admin_chats = await fetchval("SELECT COUNT(*) FROM bot_chats WHERE is_admin = TRUE") or 0

    req_badge = f" ({pending_requests})" if pending_requests > 0 else ""
    edit_badge = f" ({pending_edits} Review)" if pending_edits > 0 else ""
    exp_badge = f" ({expired_links} Recheck)" if expired_links > 0 else ""
    chat_badge = f" ({admin_chats})" if admin_chats > 0 else ""

    buttons = [
        [InlineKeyboardButton(text=f"📩 User Requests{req_badge}", callback_data="admin_requests_menu")],
        [InlineKeyboardButton(text=f"⚠️ Expired Links{exp_badge}", callback_data="admin_expired_links_menu")],
        [InlineKeyboardButton(text=f"📡 Connected Admin Chats{chat_badge}", callback_data="admin_connected_chats")],
        [InlineKeyboardButton(text=f"🎬 Editor Tasks & References{edit_badge}", callback_data="admin_editor_hub")],
        [InlineKeyboardButton(text="👥 Manage Employees", callback_data="admin_manage_employees")],
        [InlineKeyboardButton(text="📢 Manage Destinations (All Layers)", callback_data="admin_destinations_menu")],
        [InlineKeyboardButton(text="🏷️ Manage Predefined Categories", callback_data="admin_categories_menu")],
        [InlineKeyboardButton(text="🔔 Notification Log Channel", callback_data="admin_notif_channel_menu")],
        [InlineKeyboardButton(text="⏱️ Universal Delay Settings", callback_data="admin_universal_delay")],
        [InlineKeyboardButton(text="💵 Set Link Rate", callback_data="admin_set_rate")],
        [InlineKeyboardButton(text="📊 Detailed Performance Reports", callback_data="admin_reports")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def get_employee_roles_menu_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👮 Manage Admins", callback_data="manage_role:admin")],
        [InlineKeyboardButton(text="📅 Manage Flezen Schedulers", callback_data="manage_role:flezen_scheduler")],
        [InlineKeyboardButton(text="🎬 Manage Editors", callback_data="manage_role:editor")],
        [InlineKeyboardButton(text="🏷️ Manage Sorters", callback_data="manage_role:sorter")],
        [InlineKeyboardButton(text="📤 Manage Uploaders", callback_data="manage_role:uploader")],
        [InlineKeyboardButton(text="🔍 Manage Verifiers", callback_data="manage_role:verifier")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")]
    ])

def get_destinations_hub_kb():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📡 Select from Connected Admin Chats", callback_data="admin_connected_chats")],
        [InlineKeyboardButton(text="📁 1. Prelayered Groups", callback_data="dest_layer:prelayered")],
        [InlineKeyboardButton(text="🥇 2. 1st Layer Groups / Channels", callback_data="dest_layer:first_layer")],
        [InlineKeyboardButton(text="🥈 3. 2nd Layer (Verifier) Groups", callback_data="dest_layer:second_layer")],
        [InlineKeyboardButton(text="🥉 4. 3rd Layer (Flezen) Groups", callback_data="dest_layer:third_layer")],
        [InlineKeyboardButton(text="👑 5. Inc Master Destination", callback_data="dest_layer:inc_master")],
        [InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")]
    ])

def get_uploader_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="▶️ Start Task"), KeyboardButton(text="💰 Check Earning")]
        ],
        resize_keyboard=True
    )

def get_sorter_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="▶️ Start Sorting Task"), KeyboardButton(text="📊 My Sort Stats")]
        ],
        resize_keyboard=True
    )

def get_editor_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="▶️ Start Task"), KeyboardButton(text="📊 Status")]
        ],
        resize_keyboard=True
    )

def get_flezen_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="▶️ Start Task"), KeyboardButton(text="📊 Progress")]
        ],
        resize_keyboard=True
    )

def get_verifier_reply_kb():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔍 Check Verification Stats")]
        ],
        resize_keyboard=True
    )

# ---------------------------------------------------------------------------
# WORKFLOW TASK ROUTINES
# ---------------------------------------------------------------------------
async def start_uploader_task(user_id: int, message: Message, state: FSMContext, bot: Bot):
    role = await get_employee_role(user_id)
    if role != "uploader" and not await is_admin(user_id):
        return await message.answer("⛔ You are not registered as an uploader.")

    existing = await fetchrow(
        "SELECT id, url, link_type, is_urgent FROM links WHERE status = 'locked' AND locked_by = $1",
        user_id
    )

    if existing:
        link_id, url, link_type, is_urgent = existing["id"], existing["url"], existing["link_type"], existing["is_urgent"]
    else:
        locked_row = await fetchrow("""
            UPDATE links
            SET status = 'locked', locked_by = $1, locked_at = NOW()
            WHERE id = (
                SELECT id FROM links
                WHERE status = 'pending'
                ORDER BY is_urgent DESC, id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, url, link_type, is_urgent
        """, user_id)

        if not locked_row:
            return await message.answer("ℹ️ Queue is currently empty. No links are available right now!")

        link_id, url, link_type, is_urgent = locked_row["id"], locked_row["url"], locked_row["link_type"], locked_row["is_urgent"]

        nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Uploader"
        urg_flag = " 🚨 [URGENT LINK]" if is_urgent else ""
        await send_notification_log(
            bot,
            f"🎯 <b>[Task Locked & Started]{urg_flag}</b>\n"
            f"• <b>Uploader:</b> {nick} (<code>{user_id}</code>)\n"
            f"• <b>Link:</b> {url}\n"
            f"• <b>Type:</b> <code>{link_type}</code>"
        )

    curr_count = await fetchval("SELECT COUNT(*) FROM videos WHERE link_id = $1", link_id)

    await state.set_state(UploaderStates.uploading_videos)
    await state.update_data(
        link_id=link_id,
        count=curr_count
    )

    task_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Done Uploading All Videos", callback_data="uploader_done")],
        [InlineKeyboardButton(text="⚠️ Link Expired (Report to Admin)", callback_data=f"uploader_expired:{link_id}")]
    ])

    clickable_task_url = make_clickable_url(url)
    urg_header = "🚨 <b>[URGENT PRIORITY TASK]</b>\n" if is_urgent else ""

    sent_msg = await message.answer(
        f"{urg_header}"
        f"🎯 <b>Task Assigned & Locked!</b>\n\n"
        f'🔗 <b>Link:</b> <a href="{clickable_task_url}"><b>{clickable_task_url}</b></a>\n'
        f"📁 <b>Type:</b> <code>{link_type}</code>\n\n"
        "<i>(This link is locked exclusively to you until completed or reported expired)</i>\n\n"
        "Please download/forward all videos from this link and send them directly into this chat.\n"
        "<i>When finished, tap Done or send <b>/done</b>.</i>\n\n"
        f"📹 <b>Videos uploaded for this link count:</b> <code>{curr_count}</code>",
        reply_markup=task_kb,
        parse_mode="HTML"
    )

    nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Uploader"
    upload_manager.init_session(link_id, user_id, message.chat.id, nick, curr_count, sent_msg.message_id)

async def start_editor_task(user_id: int, message: Message, state: FSMContext, bot: Bot):
    task = await fetchrow(
        "SELECT id, ref_type, ref_content, ref_caption, rejection_notes, status FROM editor_tasks WHERE assigned_to = $1 AND status IN ('assigned', 'rejected') ORDER BY id ASC LIMIT 1",
        user_id
    )

    if not task:
        task = await fetchrow("""
            UPDATE editor_tasks
            SET status = 'assigned', assigned_to = $1
            WHERE id = (
                SELECT id FROM editor_tasks
                WHERE status = 'pending'
                ORDER BY id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, ref_type, ref_content, ref_caption, rejection_notes, status
        """, user_id)

    if not task:
        return await message.answer("ℹ️ No reference tasks available right now! Check back shortly.")

    task_id = task["id"]
    await state.set_state(EditorStates.waiting_for_edited_video)
    await state.update_data(current_editor_task_id=task_id)

    mod_header = f"⚠️ <b>Modification Requested!</b>\n<b>Instructions:</b> {task['rejection_notes']}\n\n" if task["rejection_notes"] else ""
    prompt_caption = (
        f"{mod_header}"
        f"🎬 <b>[Editing Task #{task_id}]</b>\n\n"
        f"📝 <b>Reference Instructions:</b>\n{task['ref_caption'] or 'No instructions provided.'}\n\n"
        "👉 <i>Please create and upload the final edited video according to this reference:</i>"
    )

    if task["ref_type"] == "photo":
        await message.answer_photo(photo=task["ref_content"], caption=prompt_caption, parse_mode="HTML")
    elif task["ref_type"] == "video":
        await message.answer_video(video=task["ref_content"], caption=prompt_caption, parse_mode="HTML")
    elif task["ref_type"] == "document":
        await message.answer_document(document=task["ref_content"], caption=prompt_caption, parse_mode="HTML")
    else:
        await message.answer(f"🔗 <b>Reference Content / Link:</b>\n{task['ref_content']}\n\n{prompt_caption}", parse_mode="HTML")

async def build_sorter_category_kb(vid_id: int) -> InlineKeyboardMarkup:
    recent_cats = await fetch("SELECT name FROM categories ORDER BY last_used_at DESC NULLS LAST, use_count DESC LIMIT 2")
    buttons = []

    if recent_cats:
        row = [InlineKeyboardButton(text=f"⚡ {rc['name']}", callback_data=f"sort_pick:{vid_id}:{rc['name']}") for rc in recent_cats]
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="📂 Show More Categories", callback_data=f"sort_more:{vid_id}")])
    buttons.append([InlineKeyboardButton(text="⏭️ Skip Video", callback_data=f"sort_skip:{vid_id}")])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

async def sorter_fetch_next_task(user_id: int, message: Message, state: FSMContext, bot: Bot):
    role = await get_employee_role(user_id)
    if role != "sorter" and not await is_admin(user_id):
        return await message.answer("⛔ Access Denied. You are not registered as a Sorter.")

    video_row = await fetchrow("""
        UPDATE videos
        SET status = 'sorting', sorter_id = $1
        WHERE id = (
            SELECT id FROM videos
            WHERE status = 'pending_sort'
            ORDER BY id ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        )
        RETURNING id, file_id, caption
    """, user_id)

    if not video_row:
        return await message.answer("ℹ️ No videos available in the sorting queue right now! Check back shortly.")

    vid_id = video_row["id"]
    file_id = video_row["file_id"]
    curr_caption = video_row["caption"] or "None"

    await state.set_state(SorterStates.active_sorting)
    await state.update_data(current_vid_id=vid_id)

    kb = await build_sorter_category_kb(vid_id)

    await message.answer_video(
        video=file_id,
        caption=(
            f"🎬 <b>Task Video #{vid_id}</b>\n\n"
            f"📝 <b>Uploader Caption:</b> <code>{curr_caption}</code>\n\n"
            "👉 <i>Which category is more preferable for this video?</i>"
        ),
        reply_markup=kb,
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# GENERAL EMPLOYEE START TASK ROUTER
# ---------------------------------------------------------------------------
@router.message(F.text == "▶️ Start Task", StateFilter("*"))
async def msg_role_start_task(message: Message, state: FSMContext, bot: Bot):
    user_id = message.from_user.id
    role = await get_employee_role(user_id)

    if role == "uploader":
        await start_uploader_task(user_id, message, state, bot)
    elif role == "editor":
        await start_editor_task(user_id, message, state, bot)
    elif role == "sorter":
        await sorter_fetch_next_task(user_id, message, state, bot)
    elif role == "flezen_scheduler":
        await message.answer(
            "📤 <b>Flezen Task Active!</b>\n\n"
            "Please send your posts (text, photos, videos, or documents).\n"
            "<i>(Posts are also automatically detected and routed to 3rd Layer at any time!)</i>",
            parse_mode="HTML"
        )
    else:
        if await is_admin(user_id):
            await message.answer("👑 Admin mode: choose tasks from your /admin panel.")
        else:
            await message.answer("⛔ Access Denied.")

# ---------------------------------------------------------------------------
# AUTOMATIC BOT CHAT & ADMIN ROLE TRACKER
# ---------------------------------------------------------------------------
@router.my_chat_member()
async def on_bot_chat_member_updated(event: ChatMemberUpdated, bot: Bot):
    chat = event.chat
    new_status = event.new_chat_member.status
    chat_title = chat.title or chat.full_name or f"Chat {chat.id}"

    if new_status in ["administrator", "creator"]:
        await execute(
            """
            INSERT INTO bot_chats (chat_id, chat_title, chat_type, is_admin, updated_at)
            VALUES ($1, $2, $3, TRUE, NOW())
            ON CONFLICT (chat_id) DO UPDATE SET
                is_admin = TRUE,
                chat_title = EXCLUDED.chat_title,
                chat_type = EXCLUDED.chat_type,
                updated_at = NOW()
            """,
            chat.id, chat_title, chat.type
        )
        await send_notification_log(
            bot,
            f"🤖 <b>[Bot Promoted to Administrator]</b>\n"
            f"• <b>Title:</b> {chat_title}\n"
            f"• <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"• <b>Type:</b> <code>{chat.type}</code>"
        )
    else:
        await execute("UPDATE bot_chats SET is_admin = FALSE, updated_at = NOW() WHERE chat_id = $1", chat.id)
        await send_notification_log(
            bot,
            f"⚠️ <b>[Bot Demoted or Removed from Chat]</b>\n"
            f"• <b>Title:</b> {chat_title}\n"
            f"• <b>Chat ID:</b> <code>{chat.id}</code>\n"
            f"• <b>New Status:</b> <code>{new_status}</code>"
        )

# ---------------------------------------------------------------------------
# CONNECTED ADMIN CHATS MENU
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_connected_chats")
async def cb_admin_connected_chats(call: CallbackQuery, state: FSMContext):
    await state.clear()
    chats = await fetch("SELECT chat_id, chat_title, chat_type FROM bot_chats WHERE is_admin = TRUE ORDER BY chat_title ASC")

    buttons = []
    if chats:
        for c in chats:
            icon = "📢" if c["chat_type"] == "channel" else "👥"
            buttons.append([InlineKeyboardButton(text=f"{icon} {c['chat_title']}", callback_data=f"chat_detail:{c['chat_id']}")])

    buttons.append([InlineKeyboardButton(text="📢 Destination Hub", callback_data="admin_destinations_menu")])
    buttons.append([InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")])

    text = (
        "📡 <b>Connected Channels & Groups (Bot Admin)</b>\n\n"
        f"Total available: <code>{len(chats)}</code>\n\n"
        + ("Tap any channel or group below to assign it as any destination layer:" if chats else "<i>No connected admin chats found. Add the bot as an administrator to your channel/group and it will appear here.</i>")
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("chat_detail:"))
async def cb_chat_detail(call: CallbackQuery):
    chat_id = int(call.data.split(":")[1])
    chat_info = await fetchrow("SELECT chat_id, chat_title, chat_type FROM bot_chats WHERE chat_id = $1", chat_id)

    if not chat_info:
        return await call.answer("⚠️ Chat record not found!", show_alert=True)

    assigned_layers = await fetch("SELECT layer_type FROM destinations WHERE target_chat = $1", str(chat_id))
    layer_tags = ", ".join([al["layer_type"].replace('_', ' ').title() for al in assigned_layers]) if assigned_layers else "None"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📁 Set as Prelayered Group", callback_data=f"set_chat_dest:{chat_id}:prelayered")],
        [InlineKeyboardButton(text="🥇 Set as 1st Layer Group/Channel", callback_data=f"set_chat_dest:{chat_id}:first_layer")],
        [InlineKeyboardButton(text="🥈 Set as 2nd Layer (Verifier) Group", callback_data=f"set_chat_dest:{chat_id}:second_layer")],
        [InlineKeyboardButton(text="🥉 Set as 3rd Layer (Flezen) Group", callback_data=f"set_chat_dest:{chat_id}:third_layer")],
        [InlineKeyboardButton(text="👑 Set as Inc Master Destination", callback_data=f"set_chat_dest:{chat_id}:inc_master")],
        [InlineKeyboardButton(text="🔙 Back to Connected Chats", callback_data="admin_connected_chats")]
    ])

    await call.message.edit_text(
        f"📡 <b>Connected Chat Configuration</b>\n\n"
        f"• <b>Title:</b> {chat_info['chat_title']}\n"
        f"• <b>Chat ID:</b> <code>{chat_id}</code>\n"
        f"• <b>Type:</b> <code>{chat_info['chat_type'].capitalize()}</code>\n"
        f"• <b>Currently Assigned As:</b> <code>{layer_tags}</code>\n\n"
        "Tap a destination layer below to link this chat immediately:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("set_chat_dest:"))
async def cb_set_chat_dest(call: CallbackQuery, bot: Bot):
    parts = call.data.split(":")
    chat_id = int(parts[1])
    layer = parts[2]

    _, confirmation_text = await register_and_verify_destination(bot, layer, str(chat_id), call.from_user.id)

    layer_title = layer.replace("_", " ").title()
    dest_row = await fetchrow("SELECT id FROM destinations WHERE layer_type = $1 AND target_chat = $2", layer, str(chat_id))
    dest_id = dest_row["id"] if dest_row else None

    buttons = []
    if layer == "prelayered" and dest_id:
        buttons.append([InlineKeyboardButton(text="⏱️ Set Custom Delay for It", callback_data=f"dest_set_delay:{dest_id}")])
    buttons.append([InlineKeyboardButton(text=f"📂 View {layer_title} List", callback_data=f"dest_layer:{layer}")])
    buttons.append([InlineKeyboardButton(text="📡 Connected Admin Chats", callback_data="admin_connected_chats")])
    buttons.append([InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")])

    await call.message.edit_text(confirmation_text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")
    await call.answer("Destination linked successfully!")

# ---------------------------------------------------------------------------
# BACKGROUND BROADCASTER (DECOUPLED FROM SORTER QUEUE)
# ---------------------------------------------------------------------------
async def broadcast_worker(bot: Bot):
    while True:
        try:
            dests = await fetch("SELECT id, target_chat, custom_delay FROM destinations WHERE layer_type = 'prelayered' ORDER BY id ASC")
            
            if dests:
                video_row = await fetchrow("""
                    SELECT v.id, v.file_id, v.link_id, l.category, l.video_count 
                    FROM videos v
                    JOIN links l ON v.link_id = l.id
                    WHERE v.posted_at IS NULL AND l.status = 'completed'
                    ORDER BY v.id ASC 
                    LIMIT 1
                """)

                if video_row:
                    vid_id = video_row["id"]
                    file_id = video_row["file_id"]
                    link_id = video_row["link_id"]
                    total_vids = video_row["video_count"] or 1
                    category_caption = video_row["category"] or ""

                    mode = await get_setting("delay_mode", "fixed")
                    fixed_delay = int(await get_setting("fixed_delay", "60"))
                    rnd_min = int(await get_setting("random_min", "30"))
                    rnd_max = int(await get_setting("random_max", "120"))

                    max_custom_delay = None
                    for d in dests:
                        target_chat = d["target_chat"]
                        c_delay = d["custom_delay"]
                        if c_delay and (max_custom_delay is None or c_delay > max_custom_delay):
                            max_custom_delay = c_delay
                        try:
                            chat_id = int(target_chat) if target_chat.lstrip('-').isdigit() else target_chat
                            await bot.send_video(chat_id=chat_id, video=file_id, caption=category_caption)
                        except Exception as send_err:
                            logging.error(f"Broadcast error on {target_chat}: {send_err}")

                    await execute("UPDATE videos SET posted_at = NOW() WHERE id = $1", vid_id)

                    posted_count = await fetchval(
                        "SELECT COUNT(*) FROM videos WHERE link_id = $1 AND posted_at IS NOT NULL",
                        link_id
                    ) or 1

                    log_channel = await get_setting("notification_log_channel", "") or os.getenv("NOTIFICATION_LOG_CHANNEL", "")
                    if log_channel:
                        target_log_chat = int(log_channel) if log_channel.lstrip('-').isdigit() else log_channel
                        tracker_key = f"prelayered:{link_id}"
                        prev_msg_id = broadcast_notif_tracker.get(tracker_key)

                        if prev_msg_id:
                            try:
                                await bot.delete_message(chat_id=target_log_chat, message_id=prev_msg_id)
                            except Exception:
                                pass

                        if posted_count < total_vids:
                            notif_text = (
                                f"📢 <b>[Prelayered Broadcast Progress]</b>\n"
                                f"• <b>Caption:</b> <code>{category_caption}</code>\n"
                                f"• <b>Progress:</b> <code>{posted_count} / {total_vids} videos</code>"
                            )
                        else:
                            notif_text = (
                                f"📢 <b>[Prelayered Broadcast Completed]</b>\n"
                                f"• <b>Caption:</b> <code>{category_caption}</code>\n"
                                f"• <b>Total Videos:</b> <code>{total_vids}</code>"
                            )

                        sent_notif = await send_notification_log(bot, notif_text)
                        if sent_notif:
                            if posted_count < total_vids:
                                broadcast_notif_tracker[tracker_key] = sent_notif.message_id
                            else:
                                broadcast_notif_tracker.pop(tracker_key, None)

                    sleep_duration = max_custom_delay if (max_custom_delay and max_custom_delay > 0) else (
                        random.randint(rnd_min, rnd_max) if mode == "random" else fixed_delay
                    )
                    await asyncio.sleep(sleep_duration)
                else:
                    await asyncio.sleep(10)
            else:
                await asyncio.sleep(10)
        except Exception as loop_err:
            logging.error(f"Broadcast worker loop error: {loop_err}")
            await asyncio.sleep(10)

@router.startup()
async def on_startup(bot: Bot):
    await init_db()
    asyncio.create_task(broadcast_worker(bot))
    logging.info("Connected to Neon DB and background services running.")

# ---------------------------------------------------------------------------
# COMMANDS: /start & /admin
# ---------------------------------------------------------------------------
@router.message(CommandStart(), StateFilter("*"))
async def cmd_start(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    user_id = message.from_user.id

    if await is_admin(user_id):
        await message.answer(
            "👑 <b>Welcome Admin!</b>\n\n"
            "Use the control panel below to review user requests, connected admin chats, editor tasks, and destinations:",
            reply_markup=await get_admin_panel_kb(),
            parse_mode="HTML"
        )
        return

    role = await get_employee_role(user_id)
    if role == "uploader":
        await message.answer("👋 <b>Welcome Uploader!</b>\nChoose an option below:", reply_markup=get_uploader_reply_kb(), parse_mode="HTML")
        return
    elif role == "sorter":
        await message.answer("👋 <b>Welcome Sorter!</b>\nChoose an option below:", reply_markup=get_sorter_reply_kb(), parse_mode="HTML")
        return
    elif role == "editor":
        await message.answer("👋 <b>Welcome Editor!</b>\nChoose an option below to begin video editing tasks:", reply_markup=get_editor_reply_kb(), parse_mode="HTML")
        return
    elif role == "flezen_scheduler":
        await message.answer("👋 <b>Welcome Flezen Scheduler!</b>\nChoose an option below or send your posts directly:", reply_markup=get_flezen_reply_kb(), parse_mode="HTML")
        return
    elif role == "verifier":
        await message.answer("👋 <b>Welcome Verifier!</b>\nVideos sent to 2nd Layer will be verified using action buttons.", reply_markup=get_verifier_reply_kb(), parse_mode="HTML")
        return

    username = message.from_user.username or ""
    full_name = message.from_user.full_name or "Telegram User"

    await execute(
        """
        INSERT INTO user_requests (user_id, username, full_name, status, created_at)
        VALUES ($1, $2, $3, 'pending', NOW())
        ON CONFLICT (user_id) DO UPDATE SET
            status = 'pending',
            username = EXCLUDED.username,
            full_name = EXCLUDED.full_name,
            created_at = NOW()
        """,
        user_id, username, full_name
    )

    alert_text = (
        f"📩 <b>[New Access Request Received]</b>\n\n"
        f"• <b>Name:</b> {full_name}\n"
        f"• <b>Username:</b> @{username if username else 'None'}\n"
        f"• <b>User ID:</b> <code>{user_id}</code>\n"
        "Tap below to review and assign a role:"
    )
    admin_alert_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 Review Request", callback_data=f"req_view:{user_id}")]
    ])

    master_admin = os.getenv("ADMIN_ID")
    if master_admin and master_admin.strip().isdigit():
        try:
            await bot.send_message(chat_id=int(master_admin.strip()), text=alert_text, reply_markup=admin_alert_kb, parse_mode="HTML")
        except Exception as err:
            logging.warning(f"Could not alert master admin directly: {err}")

    await send_notification_log(bot, alert_text)

    await message.answer(
        "⏳ <b>Access Request Submitted!</b>\n\n"
        "Your request has been automatically sent to the Administrator.\n"
        "You will be notified once access is approved and your role is assigned.",
        parse_mode="HTML"
    )

@router.message(Command("admin"), StateFilter("*"))
async def cmd_admin(message: Message, state: FSMContext):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ You are not authorized to view the admin panel.")
    await message.answer("👑 <b>Admin Control Panel</b>", reply_markup=await get_admin_panel_kb(), parse_mode="HTML")

# ---------------------------------------------------------------------------
# EDITOR SUBMISSION & STATUS REPORTING
# ---------------------------------------------------------------------------
@router.message(EditorStates.waiting_for_edited_video, F.video | F.document)
async def process_editor_submission_video(message: Message, state: FSMContext):
    video = message.video
    if not video and message.document:
        if message.document.mime_type and message.document.mime_type.startswith("video/"):
            video = message.document
        else:
            return await message.answer("⚠️ Please upload a valid video file.")

    file_id = video.file_id
    await state.update_data(temp_file_id=file_id)

    await message.answer(
        "📝 <b>Video Received!</b>\n\n"
        "Please send the <b>Title</b> for this edited video (this will serve as the final caption):",
        parse_mode="HTML"
    )
    await state.set_state(EditorStates.waiting_for_video_title)

@router.message(EditorStates.waiting_for_video_title)
async def process_editor_submission_title(message: Message, state: FSMContext, bot: Bot):
    title = message.text.strip() if message.text else "Untitled Video"
    data = await state.get_data()
    task_id = data.get("current_editor_task_id")
    file_id = data.get("temp_file_id")
    user_id = message.from_user.id

    if not task_id or not file_id:
        await state.clear()
        return await message.answer("⚠️ Session lost. Please tap ▶️ Start Task again.")

    await execute(
        "UPDATE editor_tasks SET submitted_file_id = $1, video_title = $2, status = 'submitted', submitted_at = NOW() WHERE id = $3",
        file_id, title, task_id
    )
    await state.clear()

    editor_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or f"ID {user_id}"

    review_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve & Post to Inc Master", callback_data=f"edtask_appr:{task_id}")],
        [InlineKeyboardButton(text="✏️ Re-edit Title", callback_data=f"edtask_edittitle:{task_id}")],
        [InlineKeyboardButton(text="❌ Reject with Instructions", callback_data=f"edtask_rej:{task_id}")],
        [InlineKeyboardButton(text="🎬 Edit by Admin (Self Edit)", callback_data=f"edtask_selfedit:{task_id}")]
    ])

    master_admin = os.getenv("ADMIN_ID")
    if master_admin and master_admin.strip().isdigit():
        try:
            await bot.send_video(
                chat_id=int(master_admin.strip()),
                video=file_id,
                caption=(
                    f"🎬 <b>[Editor Task #{task_id} Submitted for Approval]</b>\n\n"
                    f"• <b>Editor:</b> {editor_nick} (<code>{user_id}</code>)\n"
                    f"• <b>Proposed Title:</b> <code>{title}</code>"
                ),
                reply_markup=review_kb,
                parse_mode="HTML"
            )
        except Exception as e:
            logging.warning(f"Could not deliver editor submission to master admin: {e}")

    await send_notification_log(
        bot,
        f"🎬 <b>[Editor Task Submitted for Review]</b>\n"
        f"• <b>Task ID:</b> <code>#{task_id}</code>\n"
        f"• <b>Editor:</b> {editor_nick} (<code>{user_id}</code>)\n"
        f"• <b>Title:</b> {title}"
    )

    await message.answer(
        f"🎉 <b>Task #{task_id} Completed Successfully!</b>\n\n"
        f"📌 <b>Title:</b> <code>{title}</code>\n\n"
        "Your video has been submitted and is currently pending Admin approval.\n"
        "You will be notified once reviewed!",
        reply_markup=get_editor_reply_kb(),
        parse_mode="HTML"
    )

@router.message(StateFilter("*"), F.text.in_(["📊 Status", "📊 My Editor Stats", "Status"]))
async def msg_editor_status_report(message: Message):
    user_id = message.from_user.id
    role = await get_employee_role(user_id)

    if role != "editor" and not await is_admin(user_id):
        return await message.answer("⛔ Access Denied. Only registered Editors can view this report.")

    up_today = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND COALESCE(submitted_at, created_at) >= CURRENT_DATE AND submitted_file_id IS NOT NULL", user_id) or 0
    up_week = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND COALESCE(submitted_at, created_at) >= (CURRENT_DATE - INTERVAL '7 days') AND submitted_file_id IS NOT NULL", user_id) or 0
    up_month = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND COALESCE(submitted_at, created_at) >= (CURRENT_DATE - INTERVAL '30 days') AND submitted_file_id IS NOT NULL", user_id) or 0
    up_total = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND submitted_file_id IS NOT NULL", user_id) or 0

    app_today = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'approved' AND COALESCE(reviewed_at, created_at) >= CURRENT_DATE", user_id) or 0
    app_week = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'approved' AND COALESCE(reviewed_at, created_at) >= (CURRENT_DATE - INTERVAL '7 days')", user_id) or 0
    app_month = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'approved' AND COALESCE(reviewed_at, created_at) >= (CURRENT_DATE - INTERVAL '30 days')", user_id) or 0
    app_total = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'approved'", user_id) or 0

    rej_today = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'rejected' AND COALESCE(reviewed_at, created_at) >= CURRENT_DATE", user_id) or 0
    rej_week = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'rejected' AND COALESCE(reviewed_at, created_at) >= (CURRENT_DATE - INTERVAL '7 days')", user_id) or 0
    rej_month = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'rejected' AND COALESCE(reviewed_at, created_at) >= (CURRENT_DATE - INTERVAL '30 days')", user_id) or 0
    rej_total = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'rejected'", user_id) or 0

    pending_now = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'submitted'", user_id) or 0
    editor_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Editor"

    report_text = (
        f"📊 <b>Editor Performance & Progress Report</b>\n"
        f"👤 <b>Editor:</b> {editor_nick} (<code>{user_id}</code>)\n\n"
        f"📅 <b>Today (Per Day):</b>\n"
        f"• Videos Uploaded: <code>{up_today}</code>\n"
        f"• Approved: <code>{app_today}</code>\n"
        f"• Rejected: <code>{rej_today}</code>\n\n"
        f"🗓️ <b>Past 7 Days (Per Week):</b>\n"
        f"• Videos Uploaded: <code>{up_week}</code>\n"
        f"• Approved: <code>{app_week}</code>\n"
        f"• Rejected: <code>{rej_week}</code>\n\n"
        f"📆 <b>Past 30 Days (Per Month):</b>\n"
        f"• Videos Uploaded: <code>{up_month}</code>\n"
        f"• Approved: <code>{app_month}</code>\n"
        f"• Rejected: <code>{rej_month}</code>\n\n"
        f"📈 <b>All-Time Totals:</b>\n"
        f"• Total Submitted: <code>{up_total} videos</code>\n"
        f"• Total Approved: <code>{app_total} videos</code>\n"
        f"• Pending Admin Review: <code>{pending_now} videos</code>\n"
        f"• Revisions Awaiting Fix: <code>{rej_total} tasks</code>"
    )

    await message.answer(report_text, parse_mode="HTML")

# ---------------------------------------------------------------------------
# FLEZEN SCHEDULER POST HANDLER
# ---------------------------------------------------------------------------
@router.message(F.text == "📊 Progress", StateFilter("*"))
async def msg_flezen_progress(message: Message):
    user_id = message.from_user.id
    role = await get_employee_role(user_id)
    if role != "flezen_scheduler" and not await is_admin(user_id):
        return await message.answer("⛔ Access Denied.")

    p_today = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1 AND created_at >= CURRENT_DATE", user_id) or 0
    p_week = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1 AND created_at >= (CURRENT_DATE - INTERVAL '7 days')", user_id) or 0
    p_month = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1 AND created_at >= (CURRENT_DATE - INTERVAL '30 days')", user_id) or 0
    p_total = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1", user_id) or 0

    await message.answer(
        f"📊 <b>Your Post Publishing Progress:</b>\n\n"
        f"📅 <b>Today:</b> <code>{p_today} posts</code>\n"
        f"🗓️ <b>Past 7 Days (Week):</b> <code>{p_week} posts</code>\n"
        f"📆 <b>Past 30 Days (Month):</b> <code>{p_month} posts</code>\n"
        f"📈 <b>All-Time Total:</b> <code>{p_total} posts</code>",
        parse_mode="HTML"
    )

@router.message(
    IsFlezenScheduler(),
    ~StateFilter(
        AdminStates.waiting_for_admin_id,
        AdminStates.waiting_for_emp_manual_add,
        AdminStates.waiting_for_dest_target,
        AdminStates.waiting_for_dest_custom_delay,
        AdminStates.waiting_for_notif_channel,
        AdminStates.waiting_for_new_category,
        AdminStates.waiting_for_fixed_delay,
        AdminStates.waiting_for_random_delay,
        AdminStates.waiting_for_earning_rate,
        AdminStates.waiting_for_req_nickname,
        AdminStates.waiting_for_editor_ref,
        AdminStates.waiting_for_rejection_notes,
        AdminStates.waiting_for_admin_self_edit,
        AdminStates.waiting_for_reedit_title,
        UploaderStates.uploading_videos,
        UploaderStates.waiting_for_category,
        EditorStates.waiting_for_edited_video,
        EditorStates.waiting_for_video_title
    ),
    ~F.text.startswith("/"),
    ~F.text.in_(["▶️ Start Task", "📊 Progress", "📊 Status", "📊 My Editor Stats", "Status"])
)
async def flezen_auto_post_handler(message: Message, bot: Bot):
    user_id = message.from_user.id
    unique_key = None
    content_type = "text"

    if message.video:
        unique_key = message.video.file_unique_id
        content_type = "video"
    elif message.photo:
        unique_key = message.photo[-1].file_unique_id
        content_type = "photo"
    elif message.document:
        unique_key = message.document.file_unique_id
        content_type = "document"
    elif message.audio:
        unique_key = message.audio.file_unique_id
        content_type = "audio"
    elif message.animation:
        unique_key = message.animation.file_unique_id
        content_type = "animation"
    elif message.text:
        unique_key = hashlib.sha256(message.text.strip().encode()).hexdigest()
        content_type = "text"
    else:
        return

    is_duplicate = await fetchval("SELECT 1 FROM flezen_posts WHERE file_unique_id = $1", unique_key)
    if is_duplicate:
        await execute("INSERT INTO duplicate_logs (user_id, created_at) VALUES ($1, NOW())", user_id)
        warn = await message.reply("⚠️ <b>Duplicate Detected:</b> This post was already submitted before! Skipped.", parse_mode="HTML")
        await asyncio.sleep(3)
        try:
            await warn.delete()
            await message.delete()
        except Exception:
            pass
        return

    await execute(
        "INSERT INTO flezen_posts (user_id, content_type, file_unique_id, created_at) VALUES ($1, $2, $3, NOW())",
        user_id, content_type, unique_key
    )
    posts_today = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1 AND created_at >= CURRENT_DATE", user_id) or 1

    third_layers = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'third_layer'")
    delivered_count = 0
    for tl in third_layers:
        try:
            target = int(tl["target_chat"]) if tl["target_chat"].lstrip('-').isdigit() else tl["target_chat"]
            await bot.copy_message(chat_id=target, from_chat_id=message.chat.id, message_id=message.message_id)
            delivered_count += 1
        except Exception as e:
            logging.error(f"Error copying post to 3rd Layer {tl['target_chat']}: {e}")

    prev_msg_id = flezen_conf_tracker.get(user_id)
    if prev_msg_id:
        try:
            await bot.delete_message(chat_id=message.chat.id, message_id=prev_msg_id)
        except Exception:
            pass

    new_conf = await message.answer(
        f"✅ <b>Post Successfully Submitted & Delivered to 3rd Layer!</b>\n\n"
        f"• <b>Type:</b> <code>{content_type.capitalize()}</code>\n"
        f"• <b>Your Posts Today:</b> <code>{posts_today}</code>\n"
        f"• <b>Destinations Reached:</b> <code>{delivered_count}</code>",
        parse_mode="HTML"
    )
    flezen_conf_tracker[user_id] = new_conf.message_id

    scheduler_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or f"ID {user_id}"
    await send_notification_log(
        bot,
        f"🚀 <b>[Flezen Post Delivered to 3rd Layer]</b>\n"
        f"• <b>Scheduler:</b> {scheduler_nick} (<code>{user_id}</code>)\n"
        f"• <b>Content:</b> <code>{content_type.capitalize()}</code>\n"
        f"• <b>Today Total:</b> <code>{posts_today}</code>\n"
        f"• <b>Delivered to 3rd Layer:</b> {delivered_count} group(s)"
    )

# ---------------------------------------------------------------------------
# USER ACCESS REQUESTS MANAGEMENT
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_requests_menu")
async def cb_admin_requests_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    requests = await fetch("SELECT user_id, username, full_name, created_at FROM user_requests WHERE status = 'pending' ORDER BY created_at DESC")

    buttons = []
    if requests:
        for r in requests:
            uname = f"@{r['username']}" if r["username"] else f"ID: {r['user_id']}"
            buttons.append([InlineKeyboardButton(text=f"👤 {r['full_name']} ({uname})", callback_data=f"req_view:{r['user_id']}")])

    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")])

    text = (
        "📩 <b>Pending User Access Requests</b>\n\n"
        f"Total waiting: <code>{len(requests)}</code>\n\n"
        + ("Tap any user to approve into a role, set a nickname, or reject:" if requests else "<i>No pending requests right now.</i>")
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("req_view:"))
async def cb_req_view(call: CallbackQuery, state: FSMContext):
    req_uid = int(call.data.split(":")[1])
    req = await fetchrow("SELECT user_id, username, full_name, created_at FROM user_requests WHERE user_id = $1", req_uid)

    if not req:
        return await call.answer("⚠️ Request record not found!", show_alert=True)

    data = await state.get_data()
    assigned_nick = data.get(f"custom_nick_{req_uid}") or req["full_name"] or f"User_{req_uid}"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📤 As Uploader", callback_data=f"req_approve:{req_uid}:uploader"),
            InlineKeyboardButton(text="📅 As Flezen Scheduler", callback_data=f"req_approve:{req_uid}:flezen_scheduler")
        ],
        [
            InlineKeyboardButton(text="🎬 As Editor", callback_data=f"req_approve:{req_uid}:editor"),
            InlineKeyboardButton(text="🏷️ As Sorter", callback_data=f"req_approve:{req_uid}:sorter")
        ],
        [
            InlineKeyboardButton(text="🔍 As Verifier", callback_data=f"req_approve:{req_uid}:verifier"),
            InlineKeyboardButton(text="👮 As Admin", callback_data=f"req_approve:{req_uid}:admin")
        ],
        [InlineKeyboardButton(text="✏️ Rename / Set Custom Nickname", callback_data=f"req_rename_prompt:{req_uid}")],
        [InlineKeyboardButton(text="❌ Reject Request", callback_data=f"req_reject:{req_uid}")],
        [InlineKeyboardButton(text="🔙 Back to Requests", callback_data="admin_requests_menu")]
    ])

    await call.message.edit_text(
        f"👤 <b>Review User Access Request:</b>\n\n"
        f"• <b>Full Name:</b> {req['full_name']}\n"
        f"• <b>Username:</b> @{req['username'] if req['username'] else 'None'}\n"
        f"• <b>User ID:</b> <code>{req_uid}</code>\n"
        f"• <b>Active Nickname:</b> <code>{assigned_nick}</code>\n\n"
        "Select a role to approve immediately, or rename them with a nickname:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("req_rename_prompt:"))
async def cb_req_rename_prompt(call: CallbackQuery, state: FSMContext):
    req_uid = int(call.data.split(":")[1])
    await state.update_data(target_req_uid=req_uid)

    await call.message.edit_text(
        f"✏️ <b>Set Nickname for User:</b> <code>{req_uid}</code>\n\n"
        "Send the nickname you want to assign to this user:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"req_view:{req_uid}")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_req_nickname)

@router.message(AdminStates.waiting_for_req_nickname)
async def process_req_nickname_input(message: Message, state: FSMContext):
    nickname = message.text.strip()
    data = await state.get_data()
    req_uid = data.get("target_req_uid")

    if not nickname:
        return await message.answer("⚠️ Nickname cannot be empty.")

    await state.update_data({f"custom_nick_{req_uid}": nickname})
    await state.set_state(None)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📤 Approve Uploader", callback_data=f"req_approve:{req_uid}:uploader"),
            InlineKeyboardButton(text="📅 Approve Flezen", callback_data=f"req_approve:{req_uid}:flezen_scheduler")
        ],
        [
            InlineKeyboardButton(text="🎬 Approve Editor", callback_data=f"req_approve:{req_uid}:editor"),
            InlineKeyboardButton(text="🏷️ Approve Sorter", callback_data=f"req_approve:{req_uid}:sorter")
        ],
        [InlineKeyboardButton(text="🔙 Back to Request", callback_data=f"req_view:{req_uid}")]
    ])

    await message.answer(
        f"✅ <b>Nickname Stored:</b> <code>{nickname}</code>\n\n"
        "Now tap the role you want to grant this user:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("req_approve:"))
async def cb_req_approve(call: CallbackQuery, state: FSMContext, bot: Bot):
    parts = call.data.split(":")
    req_uid = int(parts[1])
    role = parts[2].lower()

    data = await state.get_data()
    user_row = await fetchrow("SELECT full_name, username FROM user_requests WHERE user_id = $1", req_uid)
    default_name = user_row["full_name"] if user_row else f"{role.capitalize()}_{req_uid}"
    nickname = data.get(f"custom_nick_{req_uid}") or default_name

    if role == "admin":
        await execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", req_uid)
    else:
        await execute(
            """
            INSERT INTO employees (user_id, role, nickname, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                role = EXCLUDED.role,
                nickname = EXCLUDED.nickname
            """,
            req_uid, role, nickname
        )

    await execute("UPDATE user_requests SET status = 'approved' WHERE user_id = $1", req_uid)

    try:
        user_kbs = {
            "uploader": get_uploader_reply_kb(),
            "sorter": get_sorter_reply_kb(),
            "editor": get_editor_reply_kb(),
            "flezen_scheduler": get_flezen_reply_kb(),
            "verifier": get_verifier_reply_kb()
        }
        await bot.send_message(
            chat_id=req_uid,
            text=(
                f"🎉 <b>Congratulations! Your access has been approved!</b>\n\n"
                f"• <b>Assigned Role:</b> <code>{role.replace('_', ' ').capitalize()}</code>\n"
                f"• <b>Nickname:</b> <code>{nickname}</code>\n\n"
                "Tap /start or choose an option from the menu to begin!"
            ),
            reply_markup=user_kbs.get(role),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.warning(f"Could not notify user {req_uid}: {e}")

    await send_notification_log(
        bot,
        f"✅ <b>[Access Request Approved]</b>\n"
        f"• <b>Admin:</b> <code>{call.from_user.id}</code>\n"
        f"• <b>User ID:</b> <code>{req_uid}</code>\n"
        f"• <b>Nickname:</b> <code>{nickname}</code>\n"
        f"• <b>Role:</b> <code>{role.replace('_', ' ').capitalize()}</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📩 View Remaining Requests", callback_data="admin_requests_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])

    await call.message.edit_text(
        f"🎉 <b>User Approved Successfully!</b>\n\n"
        f"• <b>User ID:</b> <code>{req_uid}</code>\n"
        f"• <b>Nickname:</b> <code>{nickname}</code>\n"
        f"• <b>Role:</b> <code>{role.replace('_', ' ').capitalize()}</code>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("req_reject:"))
async def cb_req_reject(call: CallbackQuery, bot: Bot):
    req_uid = int(call.data.split(":")[1])
    await execute("UPDATE user_requests SET status = 'rejected' WHERE user_id = $1", req_uid)

    try:
        await bot.send_message(
            chat_id=req_uid,
            text="⛔ <b>Access Request Update:</b>\nYour access request has been declined by the Administrator.",
            parse_mode="HTML"
        )
    except Exception as e:
        logging.warning(f"Could not deliver rejection message to {req_uid}: {e}")

    await send_notification_log(
        bot,
        f"❌ <b>[Access Request Rejected]</b>\n"
        f"• <b>Admin:</b> <code>{call.from_user.id}</code>\n"
        f"• <b>Rejected User ID:</b> <code>{req_uid}</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📩 View Remaining Requests", callback_data="admin_requests_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await call.message.edit_text("❌ <b>Request Marked as Rejected.</b>", reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# EMPLOYEE MANAGEMENT HELPERS & SHORTCUTS
# ---------------------------------------------------------------------------
async def register_employee(emp_id: int, role: str, nickname: str, admin_id: int, bot: Bot) -> str:
    role = role.lower()
    if role in ["flezen", "scheduler", "flezen_scheduler"]:
        role = "flezen_scheduler"

    if role == "admin":
        await execute("INSERT INTO admins (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING", emp_id)
        role_title = "Admin"
    else:
        await execute(
            """
            INSERT INTO employees (user_id, role, nickname, created_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (user_id) DO UPDATE SET
                role = EXCLUDED.role,
                nickname = EXCLUDED.nickname
            """,
            emp_id, role, nickname
        )
        role_title = role.replace("_", " ").title()

    await send_notification_log(
        bot,
        f"👥 <b>[{role_title} Registered/Updated]</b>\n"
        f"• <b>Admin:</b> <code>{admin_id}</code>\n"
        f"• <b>Target ID:</b> <code>{emp_id}</code>\n"
        f"• <b>Nickname:</b> {nickname}\n"
        f"• <b>Role:</b> <code>{role_title}</code>"
    )

    return (
        f"✅ <b>{role_title} Configured Successfully!</b>\n\n"
        f"👤 <b>Nickname:</b> <code>{nickname}</code>\n"
        f"🆔 <b>User ID:</b> <code>{emp_id}</code>\n"
        f"💼 <b>Assigned Role:</b> <code>{role_title}</code>"
    )

@router.message(Command("setflezen"), StateFilter("*"))
async def cmd_setflezen_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("⚠️ <b>Usage:</b> <code>/setflezen (NUMERIC_ID) [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[1])
    nickname = parts[2].strip() if len(parts) > 2 else f"Flezen_{emp_id}"
    result_text = await register_employee(emp_id, "flezen_scheduler", nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 View Flezen Schedulers", callback_data="manage_role:flezen_scheduler")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("seteditor"), StateFilter("*"))
async def cmd_seteditor_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("⚠️ <b>Usage:</b> <code>/seteditor (NUMERIC_ID) [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[1])
    nickname = parts[2].strip() if len(parts) > 2 else f"Editor_{emp_id}"
    result_text = await register_employee(emp_id, "editor", nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 View Editors", callback_data="manage_role:editor")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("setsorter"), StateFilter("*"))
async def cmd_setsorter_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("⚠️ <b>Usage:</b> <code>/setsorter (NUMERIC_ID) [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[1])
    nickname = parts[2].strip() if len(parts) > 2 else f"Sorter_{emp_id}"
    result_text = await register_employee(emp_id, "sorter", nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏷️ View Sorters", callback_data="manage_role:sorter")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("setuploader"), StateFilter("*"))
async def cmd_setuploader_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("⚠️ <b>Usage:</b> <code>/setuploader (NUMERIC_ID) [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[1])
    nickname = parts[2].strip() if len(parts) > 2 else f"Uploader_{emp_id}"
    result_text = await register_employee(emp_id, "uploader", nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📤 View Uploaders", callback_data="manage_role:uploader")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("setverifier"), StateFilter("*"))
async def cmd_setverifier_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=2)
    if len(parts) < 2 or not parts[1].isdigit():
        return await message.answer("⚠️ <b>Usage:</b> <code>/setverifier (NUMERIC_ID) [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[1])
    nickname = parts[2].strip() if len(parts) > 2 else f"Verifier_{emp_id}"
    result_text = await register_employee(emp_id, "verifier", nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔍 View Verifiers", callback_data="manage_role:verifier")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

@router.message(Command("setemp"), StateFilter("*"))
async def cmd_setemp(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split(maxsplit=3)
    if len(parts) < 3 or not parts[2].isdigit():
        return await message.answer(
            "⚠️ <b>Format:</b> <code>/setemp (ROLE) (NUMERIC_ID) [NICKNAME]</code>\n"
            "Roles: <code>admin</code>, <code>flezen_scheduler</code>, <code>editor</code>, <code>sorter</code>, <code>uploader</code>, <code>verifier</code>",
            parse_mode="HTML"
        )

    role = parts[1].lower()
    if role not in ["admin", "editor", "sorter", "uploader", "verifier", "flezen", "flezen_scheduler", "scheduler"]:
        return await message.answer("⚠️ Invalid role! Choose: admin, flezen_scheduler, editor, sorter, uploader, or verifier.", parse_mode="HTML")

    emp_id = int(parts[2])
    nickname = parts[3].strip() if len(parts) > 3 else f"{role.capitalize()}_{emp_id}"
    result_text = await register_employee(emp_id, role, nickname, message.from_user.id, bot)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👥 View Role", callback_data=f"manage_role:{role}")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# EMPLOYEE MANAGEMENT SUB-MENUS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_manage_employees")
async def cb_manage_employees_hub(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(
        "👥 <b>Employee & Role Management</b>\n\nSelect a role to view roster, add members, or revoke access:",
        reply_markup=get_employee_roles_menu_kb(),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("manage_role:"))
async def cb_manage_specific_role(call: CallbackQuery, state: FSMContext):
    await state.clear()
    role = call.data.split(":")[1].lower()
    role_display = role.replace("_", " ").title()

    if role == "admin":
        rows = await fetch("SELECT user_id FROM admins ORDER BY user_id ASC")
        title = "👮 Admin Roster"
    else:
        rows = await fetch("SELECT user_id, nickname FROM employees WHERE role = $1 ORDER BY user_id ASC", role)
        title = f"👥 {role_display} Roster"

    buttons = []
    if rows:
        for r in rows:
            uid = r["user_id"]
            nick = r.get("nickname") or f"User {uid}"
            buttons.append([InlineKeyboardButton(text=f"👤 {nick} ({uid})", callback_data=f"emp_view:{role}:{uid}")])

    buttons.append([InlineKeyboardButton(text=f"➕ Add New {role_display}", callback_data=f"emp_add_btn:{role}")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Employee Roles", callback_data="admin_manage_employees")])

    await call.message.edit_text(
        f"<b>{title}</b>\n\nTotal registered: <code>{len(rows)}</code>\nTap a member to inspect or revoke:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("emp_view:"))
async def cb_emp_view_details(call: CallbackQuery):
    parts = call.data.split(":")
    role = parts[1]
    uid = int(parts[2])
    role_display = role.replace("_", " ").title()

    if role == "admin":
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🗑️ Revoke Admin Privileges", callback_data=f"emp_revoke_action:admin:{uid}")],
            [InlineKeyboardButton(text="🔙 Back to Admins", callback_data="manage_role:admin")]
        ])
        await call.message.edit_text(
            f"👮 <b>Administrator Profile:</b>\n\n• <b>User ID:</b> <code>{uid}</code>\n• <b>Status:</b> Full Administrator",
            reply_markup=kb,
            parse_mode="HTML"
        )
        return

    emp = await fetchrow("SELECT user_id, role, nickname, created_at FROM employees WHERE user_id = $1", uid)
    if not emp:
        return await call.answer("⚠️ Record not found!", show_alert=True)

    nick, created = emp["nickname"], emp["created_at"]
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"🗑️ Revoke / Remove {role_display}", callback_data=f"emp_revoke_action:{role}:{uid}")],
        [InlineKeyboardButton(text=f"🔙 Back to {role_display}s", callback_data=f"manage_role:{role}")]
    ])

    await call.message.edit_text(
        f"👤 <b>{role_display} Profile:</b>\n\n"
        f"• <b>Nickname:</b> <code>{nick}</code>\n"
        f"• <b>Telegram ID:</b> <code>{uid}</code>\n"
        f"• <b>Role:</b> <code>{role_display}</code>\n"
        f"• <b>Joined:</b> <code>{created.strftime('%Y-%m-%d') if created else 'N/A'}</code>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("emp_revoke_action:"))
async def cb_emp_revoke_action(call: CallbackQuery, bot: Bot):
    parts = call.data.split(":")
    role = parts[1]
    uid = int(parts[2])
    role_display = role.replace("_", " ").title()

    if role == "admin":
        master = os.getenv("ADMIN_ID")
        if master and master.strip().isdigit() and int(master.strip()) == uid:
            return await call.answer("⚠️ Cannot revoke Master Admin!", show_alert=True)
        await execute("DELETE FROM admins WHERE user_id = $1", uid)
    else:
        await execute("DELETE FROM employees WHERE user_id = $1", uid)

    await send_notification_log(
        bot,
        f"🗑️ <b>[{role_display} Revoked]</b>\n"
        f"• <b>Admin:</b> <code>{call.from_user.id}</code>\n"
        f"• <b>Target ID:</b> <code>{uid}</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👥 View {role_display}s", callback_data=f"manage_role:{role}")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])

    await call.message.edit_text(
        f"✅ <b>{role_display} Revoked Successfully!</b>\n\nUser <code>{uid}</code> removed from roster.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("emp_add_btn:"))
async def cb_emp_add_btn(call: CallbackQuery, state: FSMContext):
    role = call.data.split(":")[1]
    await state.update_data(target_role=role)
    role_display = role.replace("_", " ").title()

    await call.message.edit_text(
        f"➕ <b>Add New {role_display}:</b>\n\n"
        "Send the User ID and optional Nickname:\n"
        "<code>&lt;NUMERIC_ID&gt; [NICKNAME]</code>\n\n"
        "<b>Example:</b> <code>123456789 Rahul</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"manage_role:{role}")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_emp_manual_add)

@router.message(AdminStates.waiting_for_emp_manual_add)
async def process_emp_add_interactive(message: Message, state: FSMContext, bot: Bot):
    text = message.text.strip()
    data = await state.get_data()
    role = data.get("target_role", "uploader")
    role_display = role.replace("_", " ").title()

    parts = text.split(maxsplit=1)
    if not parts[0].isdigit():
        return await message.answer("⚠️ The Telegram ID must be numeric: <code>&lt;NUMERIC_ID&gt; [NICKNAME]</code>", parse_mode="HTML")

    emp_id = int(parts[0])
    nickname = parts[1].strip() if len(parts) > 1 else f"{role_display}_{emp_id}"

    result_text = await register_employee(emp_id, role, nickname, message.from_user.id, bot)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"👥 View {role_display}s", callback_data=f"manage_role:{role}")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(result_text, reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# ADMIN EXPIRED LINKS MANAGEMENT MENU
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_expired_links_menu")
async def cb_admin_expired_links_menu(call: CallbackQuery):
    expired_links = await fetch("SELECT id, url, link_type, completed_by FROM links WHERE status = 'expired' ORDER BY id DESC")

    buttons = []
    if expired_links:
        for el in expired_links:
            buttons.append([InlineKeyboardButton(text=f"⚠️ Link #{el['id']} ({el['link_type']})", callback_data=f"exp_inspect:{el['id']}")])

    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")])

    text = (
        "⚠️ <b>Reported Expired Links (Recheck Needed)</b>\n\n"
        f"Total waiting: <code>{len(expired_links)}</code>\n\n"
        + ("Tap any link below to recheck, re-queue as Normal or Urgent, or delete:" if expired_links else "<i>No expired links reported.</i>")
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data.startswith("exp_inspect:"))
async def cb_exp_inspect(call: CallbackQuery):
    link_id = int(call.data.split(":")[1])
    link = await fetchrow("SELECT id, url, link_type, completed_by, locked_by FROM links WHERE id = $1", link_id)
    if not link:
        return await call.answer("⚠️ Link not found!", show_alert=True)

    reporter_id = link["completed_by"] or link["locked_by"] or "Unknown"
    reporter_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", reporter_id) if isinstance(reporter_id, int) else "Uploader"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Re-queue (Normal)", callback_data=f"exp_act:requeue_norm:{link_id}"),
            InlineKeyboardButton(text="🚨 Re-queue (URGENT)", callback_data=f"exp_act:requeue_urg:{link_id}")
        ],
        [InlineKeyboardButton(text="🗑️ Delete Link", callback_data=f"exp_act:delete:{link_id}")],
        [InlineKeyboardButton(text="🔙 Back to Expired Links", callback_data="admin_expired_links_menu")]
    ])

    await call.message.edit_text(
        f"⚠️ <b>Expired Link Recheck File:</b>\n\n"
        f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
        f"• <b>URL:</b> {link['url']}\n"
        f"• <b>Type:</b> <code>{link['link_type']}</code>\n"
        f"• <b>Reported By:</b> {reporter_nick} (<code>{reporter_id}</code>)\n\n"
        "Choose an action to resolve this link:",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("exp_act:"))
async def cb_exp_act(call: CallbackQuery, bot: Bot):
    parts = call.data.split(":")
    action = parts[1]
    link_id = int(parts[2])

    if action == "delete":
        await execute("DELETE FROM links WHERE id = $1", link_id)
        action_title = "Deleted Permanently"
    elif action == "requeue_urg":
        await execute("UPDATE links SET status = 'pending', is_urgent = TRUE, locked_by = NULL, locked_at = NULL WHERE id = $1", link_id)
        action_title = "Re-queued as URGENT (Top of Queue)"
    else:
        await execute("UPDATE links SET status = 'pending', is_urgent = FALSE, locked_by = NULL, locked_at = NULL WHERE id = $1", link_id)
        action_title = "Re-queued as Normal"

    await send_notification_log(
        bot,
        f"⚠️ <b>[Expired Link Rechecked & Resolved]</b>\n"
        f"• <b>Admin:</b> <code>{call.from_user.id}</code>\n"
        f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
        f"• <b>Resolution:</b> {action_title}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⚠️ Expired Links Menu", callback_data="admin_expired_links_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])

    await call.message.edit_text(f"✅ <b>Link #{link_id} Resolved!</b>\nStatus: <b>{action_title}</b>", reply_markup=kb, parse_mode="HTML")
    await call.answer()

# ---------------------------------------------------------------------------
# UPLOADER TASK ENGINE (EXPIRED LINK REPORTING)
# ---------------------------------------------------------------------------
@router.callback_query(F.data.startswith("uploader_expired:"))
async def cb_uploader_expired(call: CallbackQuery, state: FSMContext, bot: Bot):
    link_id = int(call.data.split(":")[1])
    user_id = call.from_user.id

    await execute(
        "UPDATE links SET status = 'expired', completed_by = $1, locked_by = NULL, locked_at = NULL WHERE id = $2",
        user_id, link_id
    )

    await upload_manager.finalize_task(bot, link_id, call.message.chat.id)

    link_data = await fetchrow("SELECT url, link_type FROM links WHERE id = $1", link_id)
    url_str = link_data["url"] if link_data else "Unknown URL"
    uploader_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or f"ID {user_id}"

    admin_alert_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="🔄 Re-queue (Normal)", callback_data=f"exp_act:requeue_norm:{link_id}"),
            InlineKeyboardButton(text="🚨 Re-queue (URGENT)", callback_data=f"exp_act:requeue_urg:{link_id}")
        ],
        [InlineKeyboardButton(text="🗑️ Delete Link", callback_data=f"exp_act:delete:{link_id}")]
    ])

    master_admin = os.getenv("ADMIN_ID")
    if master_admin and master_admin.strip().isdigit():
        try:
            await bot.send_message(
                chat_id=int(master_admin.strip()),
                text=(
                    f"⚠️ <b>[Link Reported as Expired]</b>\n\n"
                    f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
                    f"• <b>URL:</b> {url_str}\n"
                    f"• <b>Uploader:</b> {uploader_nick} (<code>{user_id}</code>)\n\n"
                    "Tap below to resolve this link:"
                ),
                reply_markup=admin_alert_kb,
                parse_mode="HTML"
            )
        except Exception as err:
            logging.warning(f"Could not alert master admin of expired link: {err}")

    await send_notification_log(
        bot,
        f"⚠️ <b>[Link Reported as Expired by Uploader]</b>\n"
        f"• <b>Uploader:</b> {uploader_nick} (<code>{user_id}</code>)\n"
        f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
        f"• <b>URL:</b> {url_str}\n"
        "Sent to Admin for recheck."
    )

    await state.clear()
    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.answer(
        "⚠️ <b>Link Reported as Expired!</b>\n\n"
        "This link has been removed from your active task and sent to the Administrator for verification.\n\n"
        "You can now start your next task by tapping <b>▶️ Start Task</b> below:",
        reply_markup=get_uploader_reply_kb(),
        parse_mode="HTML"
    )
    await call.answer("Link reported as expired!")

@router.callback_query(F.data == "task_earning")
async def cb_task_earning(call: CallbackQuery):
    await show_uploader_earnings(call.from_user.id, call.message)
    await call.answer()

@router.message(F.text == "💰 Check Earning")
async def msg_task_earning(message: Message):
    await show_uploader_earnings(message.from_user.id, message)

async def show_uploader_earnings(user_id: int, target_msg: Message):
    rate_str = await get_setting("earning_per_link", "10")
    rate = float(rate_str) if rate_str.replace('.', '', 1).isdigit() else 10.0

    today_row = await fetchrow("""
        SELECT COUNT(*) as count, COALESCE(SUM(video_count), 0) as vids
        FROM links
        WHERE completed_by = $1 AND completed_at::date = CURRENT_DATE
    """, user_id)
    today_links = today_row["count"] or 0
    today_videos = today_row["vids"] or 0

    total_row = await fetchrow("""
        SELECT COUNT(*) as count, COALESCE(SUM(video_count), 0) as vids
        FROM links
        WHERE completed_by = $1
    """, user_id)
    total_links = total_row["count"] or 0
    total_videos = total_row["vids"] or 0

    today_earnings = today_links * rate
    total_earnings = total_links * rate

    await target_msg.answer(
        f"💼 <b>Your Earnings & Performance Overview:</b>\n\n"
        f"📅 <b>Today:</b>\n"
        f"• Completed Links: <code>{today_links}</code>\n"
        f"• Videos Uploaded: <code>{today_videos}</code>\n"
        f"• Earnings Today: <code>${today_earnings:.2f}</code>\n\n"
        f"📈 <b>All Time:</b>\n"
        f"• Completed Links: <code>{total_links}</code>\n"
        f"• Videos Uploaded: <code>{total_videos}</code>\n"
        f"• Total Earnings: <code>${total_earnings:.2f}</code>",
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# VIDEO UPLOAD HANDLER
# ---------------------------------------------------------------------------
@router.message(UploaderStates.uploading_videos, F.video | F.document)
async def process_video_upload(message: Message, state: FSMContext, bot: Bot):
    video = message.video
    if not video and message.document:
        if message.document.mime_type and message.document.mime_type.startswith("video/"):
            video = message.document
        else:
            return

    caption = message.caption or None
    file_id = video.file_id
    file_unique_id = video.file_unique_id
    data = await state.get_data()
    link_id = data.get("link_id")
    user_id = message.from_user.id

    if not link_id:
        return await message.answer("⚠️ Session expired. Please click ▶️ Start Task again.")

    nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Uploader"

    # Deduplication check
    is_duplicate = await fetchval("SELECT 1 FROM videos WHERE file_unique_id = $1", file_unique_id)
    if is_duplicate:
        await execute("INSERT INTO duplicate_logs (user_id, created_at) VALUES ($1, NOW())", user_id)
        await upload_manager.log_duplicate_attempt(bot, link_id, user_id, nick, message.chat.id)
        return

    # Valid video upload - saved with posted_at = NULL
    await execute(
        "INSERT INTO videos (link_id, uploader_id, file_id, file_unique_id, caption, status, created_at) VALUES ($1, $2, $3, $4, $5, 'pending_broadcast', NOW())",
        link_id, user_id, file_id, file_unique_id, caption
    )
    await execute("UPDATE links SET video_count = video_count + 1 WHERE id = $1", link_id)
    new_count = await fetchval("SELECT video_count FROM links WHERE id = $1", link_id)
    await state.update_data(count=new_count)

    # Throttled update to both Notification Log and Uploader chat
    await upload_manager.record_upload(bot, link_id, user_id, message.chat.id, nick, new_count)

# ---------------------------------------------------------------------------
# FINISH UPLOAD HANDLERS (BUTTON OR /done COMMAND)
# ---------------------------------------------------------------------------
async def finish_uploader_upload(bot: Bot, state: FSMContext, chat_id: int, user_id: int):
    data = await state.get_data()
    link_id = data.get("link_id")
    count = data.get("count", 0)

    if link_id:
        actual_db_count = await fetchval("SELECT video_count FROM links WHERE id = $1", link_id) or 0
        if actual_db_count > count:
            count = actual_db_count
            await state.update_data(count=count)

    if count == 0:
        return await bot.send_message(chat_id=chat_id, text="⚠️ You must upload at least 1 video before finishing this link!")

    await upload_manager.finalize_task(bot, link_id, chat_id)

    await bot.send_message(
        chat_id=chat_id,
        text="📝 Please send the initial <b>Category Name</b> for these uploaded videos:",
        parse_mode="HTML"
    )
    await state.set_state(UploaderStates.waiting_for_category)

@router.callback_query(UploaderStates.uploading_videos, F.data == "uploader_done")
async def process_uploader_done(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.answer()
    try:
        await call.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await finish_uploader_upload(bot, state, call.message.chat.id, call.from_user.id)

@router.message(Command("done"), UploaderStates.uploading_videos)
async def cmd_uploader_done(message: Message, state: FSMContext, bot: Bot):
    await finish_uploader_upload(bot, state, message.chat.id, message.from_user.id)

@router.message(UploaderStates.waiting_for_category)
async def process_category_finish(message: Message, state: FSMContext, bot: Bot):
    category = message.text.strip() if message.text else "General"
    data = await state.get_data()
    link_id = data.get("link_id")
    count = data.get("count", 0)
    user_id = message.from_user.id

    await execute(
        """
        UPDATE links
        SET status = 'completed', category = $1, completed_by = $2, completed_at = NOW(), video_count = $3
        WHERE id = $4
        """,
        category, user_id, count, link_id
    )

    # Immediately queue all uploaded videos for Sorters
    await execute(
        """
        UPDATE videos
        SET status = 'pending_sort'
        WHERE link_id = $1 AND (status = 'pending_broadcast' OR status = 'pending')
        """,
        link_id
    )

    daily_count = await fetchval(
        "SELECT COUNT(*) FROM links WHERE completed_by = $1 AND completed_at::date = CURRENT_DATE",
        user_id
    )

    nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Uploader"
    await send_notification_log(
        bot,
        f"🎉 <b>[Upload Completed]</b>\n"
        f"• <b>Uploader:</b> {nick} (<code>{user_id}</code>)\n"
        f"• <b>Link ID:</b> <code>#{link_id}</code>\n"
        f"• <b>Initial Category:</b> <code>{category}</code>\n"
        f"• <b>Total Videos:</b> <code>{count}</code>\n"
        f"• <b>Status:</b> Ready for Sorters & Queued for Prelayered Broadcast"
    )

    await state.clear()
    await message.answer(
        f"🎉 <b>Task Completed Successfully!</b>\n\n"
        f"🏷️ <b>Category:</b> <code>{category}</code>\n"
        f"📹 <b>Videos Uploaded:</b> <code>{count}</code>\n"
        f"📅 <b>Total Links Completed Today:</b> <code>{daily_count}</code>\n\n"
        "Your uploaded videos have been queued for the Sorters and scheduled for background prelayered broadcast.",
        reply_markup=get_uploader_reply_kb(),
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# ADMIN LINK CATEGORIZATION
# ---------------------------------------------------------------------------
async def start_link_queue_flow(detected_raw_links: list[str], message: Message, state: FSMContext):
    seen = set()
    links = []
    for l in detected_raw_links:
        clean = l.strip()
        if clean and clean not in seen:
            seen.add(clean)
            links.append(clean)

    if not links:
        return

    await state.set_state(AdminStates.multi_link_processing)
    await state.update_data(
        detected_links=links,
        current_link_index=0,
        total_links=len(links),
        added_count=0
    )
    await ask_link_categorization(message, state, duplicate_confirmed=False)

@router.message(IsAdmin(), StateFilter(None), F.document)
async def handle_document_links(message: Message, state: FSMContext, bot: Bot):
    if message.document.file_name and message.document.file_name.lower().endswith(".txt"):
        file_io = io.BytesIO()
        file = await bot.get_file(message.document.file_id)
        await bot.download_file(file.file_path, destination=file_io)
        content = file_io.getvalue().decode("utf-8", errors="ignore")
        
        matches = TG_LINK_REGEX.findall(content)
        if matches:
            await message.reply(f"📄 Found <code>{len(matches)}</code> Telegram link(s) in uploaded file.", parse_mode="HTML")
            await start_link_queue_flow(matches, message, state)

@router.message(IsAdmin(), StateFilter(None), F.text | F.caption)
async def handle_text_or_caption_links(message: Message, state: FSMContext):
    text_content = message.text or message.caption or ""
    if text_content.startswith("/"):
        return

    matches = TG_LINK_REGEX.findall(text_content)
    if matches:
        await start_link_queue_flow(matches, message, state)

async def ask_link_categorization(message_or_call, state: FSMContext, duplicate_confirmed: bool = False, show_urgent_options: bool = False):
    data = await state.get_data()
    links = data.get("detected_links", [])
    index = data.get("current_link_index", 0)
    total = data.get("total_links", 0)

    if index >= total or index >= len(links):
        added = data.get("added_count", 0)
        await state.clear()
        completion_text = f"🎉 <b>All Links Processed Successfully!</b>\n\nQueued <code>{added}</code> of <code>{total}</code> link(s) for uploaders."
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Back to Admin Menu", callback_data="admin_menu")]
        ])
        if isinstance(message_or_call, CallbackQuery):
            await message_or_call.message.edit_text(completion_text, reply_markup=kb, parse_mode="HTML")
        else:
            await message_or_call.answer(completion_text, reply_markup=kb, parse_mode="HTML")
        return

    raw_url = links[index]
    clickable_url = make_clickable_url(raw_url)

    if not duplicate_confirmed:
        existing_link = await fetchrow(
            "SELECT id, status, video_count FROM links WHERE url = $1 OR url = $2 ORDER BY id DESC LIMIT 1",
            clickable_url, raw_url
        )
        if existing_link:
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="⚠️ Yes, Add This Again", callback_data="mlink_dup_confirm")],
                [InlineKeyboardButton(text="⏭️ No, Skip This Link", callback_data="mlink_skip")],
                [InlineKeyboardButton(text="❌ Cancel Remaining", callback_data="mlink_cancel")]
            ])
            prompt = (
                f"⚠️ <b>Duplicate Link Detected ({index + 1}/{total})!</b>\n\n"
                f'👉 <a href="{clickable_url}"><b>{clickable_url}</b></a>\n\n'
                f"• <b>Previous Status:</b> <code>{existing_link['status']}</code>\n"
                f"• <b>Videos Recorded:</b> <code>{existing_link['video_count']}</code>\n\n"
                "<i>This link was already sent before. Are you sure you want to add this again?</i>"
            )
            if isinstance(message_or_call, CallbackQuery):
                await message_or_call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
            else:
                await message_or_call.reply(prompt, reply_markup=kb, parse_mode="HTML")
            return

    if show_urgent_options:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="🚨 Urgent Downloadable", callback_data="mlink_cat:Downloadable:1"),
                InlineKeyboardButton(text="🚨 Urgent Forwardable", callback_data="mlink_cat:Forwardable:1")
            ],
            [InlineKeyboardButton(text="🔙 Back to Normal Options", callback_data="mlink_back_normal")]
        ])
        prompt = (
            f"🚨 <b>Mark Link as URGENT ({index + 1}/{total}):</b>\n"
            f'👉 <a href="{clickable_url}"><b>{clickable_url}</b></a>\n\n'
            "<i>(Urgent links are placed at the absolute front of the queue ahead of all other links)</i>\n\n"
            "Select the link type for this urgent task:"
        )
    else:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="📥 Downloadable", callback_data="mlink_cat:Downloadable:0"),
                InlineKeyboardButton(text="⏩ Forwardable", callback_data="mlink_cat:Forwardable:0")
            ],
            [
                InlineKeyboardButton(text="🚨 URGENT (Priority Queue)", callback_data="mlink_urgent_menu")
            ],
            [
                InlineKeyboardButton(text="⏭️ Skip Link", callback_data="mlink_skip"),
                InlineKeyboardButton(text="❌ Cancel Remaining", callback_data="mlink_cancel")
            ]
        ])
        prompt = (
            f"🔗 <b>Categorize Link ({index + 1}/{total}):</b>\n"
            f'👉 <a href="{clickable_url}"><b>{clickable_url}</b></a>\n\n'
            "Please categorize this link for the uploaders queue:"
        )

    if isinstance(message_or_call, CallbackQuery):
        await message_or_call.message.edit_text(prompt, reply_markup=kb, parse_mode="HTML")
    else:
        await message_or_call.reply(prompt, reply_markup=kb, parse_mode="HTML")

@router.callback_query(AdminStates.multi_link_processing, F.data == "mlink_urgent_menu")
async def cb_mlink_urgent_menu(call: CallbackQuery, state: FSMContext):
    await ask_link_categorization(call, state, duplicate_confirmed=True, show_urgent_options=True)
    await call.answer()

@router.callback_query(AdminStates.multi_link_processing, F.data == "mlink_back_normal")
async def cb_mlink_back_normal(call: CallbackQuery, state: FSMContext):
    await ask_link_categorization(call, state, duplicate_confirmed=True, show_urgent_options=False)
    await call.answer()

@router.callback_query(AdminStates.multi_link_processing, F.data.startswith("mlink_cat:"))
async def process_multi_link_choice(call: CallbackQuery, state: FSMContext, bot: Bot):
    parts = call.data.split(":")
    link_type = parts[1]
    is_urgent = bool(int(parts[2])) if len(parts) > 2 else False

    data = await state.get_data()
    links = data.get("detected_links", [])
    index = data.get("current_link_index", 0)
    added = data.get("added_count", 0)

    raw_url = links[index]
    clickable_url = make_clickable_url(raw_url)

    await execute(
        "INSERT INTO links (url, link_type, is_urgent, status) VALUES ($1, $2, $3, 'pending')",
        clickable_url, link_type, is_urgent
    )
    await state.update_data(current_link_index=index + 1, added_count=added + 1)

    urg_label = " 🚨 [URGENT]" if is_urgent else ""
    await send_notification_log(
        bot,
        f"🔗 <b>[Link Added to Queue{urg_label}]</b>\n"
        f"• <b>URL:</b> {clickable_url}\n"
        f"• <b>Type:</b> <code>{link_type}</code>\n"
        f"• <b>Priority:</b> {'High (First in Queue)' if is_urgent else 'Standard'}"
    )

    await call.answer(f"Added as {link_type} (Urgent: {is_urgent})!")
    await ask_link_categorization(call, state, duplicate_confirmed=False)

@router.callback_query(AdminStates.multi_link_processing, F.data == "mlink_dup_confirm")
async def process_multi_link_dup_confirm(call: CallbackQuery, state: FSMContext):
    await call.answer("Duplicate confirmed. Categorize link:")
    await ask_link_categorization(call, state, duplicate_confirmed=True)

@router.callback_query(AdminStates.multi_link_processing, F.data == "mlink_skip")
async def process_multi_link_skip(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    index = data.get("current_link_index", 0)
    await state.update_data(current_link_index=index + 1)
    await call.answer("Link skipped!")
    await ask_link_categorization(call, state, duplicate_confirmed=False)

@router.callback_query(AdminStates.multi_link_processing, F.data == "mlink_cancel")
async def process_multi_link_cancel(call: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    added = data.get("added_count", 0)
    await state.clear()
    await call.message.edit_text(
        f"❌ <b>Queue setup cancelled.</b>\nAdded <code>{added}</code> link(s) before cancellation.",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# ADMIN CONFIGURATIONS & REPORTS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_menu")
async def cb_admin_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text("👑 <b>Admin Control Panel</b>", reply_markup=await get_admin_panel_kb(), parse_mode="HTML")

@router.callback_query(F.data == "admin_set_rate")
async def cb_set_rate(call: CallbackQuery, state: FSMContext):
    rate = await get_setting("earning_per_link", "10")
    await call.message.edit_text(
        f"💵 <b>Current Earning Rate:</b> <code>${rate}</code> per completed link.\n\nSend new rate:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_earning_rate)

@router.message(AdminStates.waiting_for_earning_rate)
async def process_earning_rate(message: Message, state: FSMContext):
    rate = message.text.strip()
    await set_setting("earning_per_link", rate)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Rate Updated!</b>\nNew Rate: <code>${rate}</code>", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "admin_reports")
async def cb_admin_reports(call: CallbackQuery):
    pending_links = await fetchval("SELECT COUNT(*) FROM links WHERE status = 'pending'")
    urgent_pending = await fetchval("SELECT COUNT(*) FROM links WHERE status = 'pending' AND is_urgent = TRUE") or 0
    completed_links = await fetchval("SELECT COUNT(*) FROM links WHERE status = 'completed'")
    pending_broadcast = await fetchval("SELECT COUNT(*) FROM videos v JOIN links l ON v.link_id = l.id WHERE v.posted_at IS NULL AND l.status = 'completed'") or 0
    pending_sort = await fetchval("SELECT COUNT(*) FROM videos WHERE status = 'pending_sort'") or 0
    sorted_vids = await fetchval("SELECT COUNT(*) FROM videos WHERE status = 'sorted'") or 0
    approved_edits = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE status = 'approved'") or 0
    flezen_count = await fetchval("SELECT COUNT(*) FROM flezen_posts") or 0
    connected_chats = await fetchval("SELECT COUNT(*) FROM bot_chats WHERE is_admin = TRUE") or 0

    employees = await fetch("SELECT user_id, role, nickname FROM employees ORDER BY role ASC, user_id ASC")

    text = (
        "📊 <b>Detailed Performance Reports & Analytics</b>\n\n"
        f"• <b>Links:</b> <code>{pending_links} pending</code> ({urgent_pending} Urgent) | <code>{completed_links} completed</code>\n"
        f"• <b>Videos Pending Prelayered:</b> <code>{pending_broadcast}</code>\n"
        f"• <b>Videos Pending Sorting:</b> <code>{pending_sort}</code>\n"
        f"• <b>Videos Sorted & Sent:</b> <code>{sorted_vids}</code>\n"
        f"• <b>Editor Videos Approved:</b> <code>{approved_edits}</code>\n"
        f"• <b>Flezen Posts to 3rd Layer:</b> <code>{flezen_count}</code>\n"
        f"• <b>Connected Admin Channels/Groups:</b> <code>{connected_chats}</code>\n\n"
        "👥 <b>Active Team Roster:</b>\n"
    )

    if employees:
        for emp in employees:
            uid = emp["user_id"]
            nick = emp["nickname"] or f"ID {uid}"
            role = emp["role"]

            if role == "uploader":
                v_today = await fetchval("SELECT COUNT(*) FROM videos WHERE uploader_id = $1 AND created_at >= CURRENT_DATE", uid) or 0
                flag = " 🚩 <b>[0 VIDS TODAY]</b>" if v_today == 0 else ""
                text += f"• 📤 <b>{nick}</b> (Uploader): <code>{v_today} vids today</code>{flag}\n"
            elif role == "sorter":
                s_today = await fetchval("SELECT COUNT(*) FROM videos WHERE sorter_id = $1 AND sorted_at >= CURRENT_DATE", uid) or 0
                text += f"• 🏷️ <b>{nick}</b> (Sorter): <code>{s_today} vids sorted today</code>\n"
            elif role == "editor":
                e_today = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE assigned_to = $1 AND status = 'approved' AND COALESCE(reviewed_at, created_at) >= CURRENT_DATE", uid) or 0
                text += f"• 🎬 <b>{nick}</b> (Editor): <code>{e_today} vids approved today</code>\n"
            elif role == "flezen_scheduler":
                f_today = await fetchval("SELECT COUNT(*) FROM flezen_posts WHERE user_id = $1 AND created_at >= CURRENT_DATE", uid) or 0
                text += f"• 📅 <b>{nick}</b> (Flezen): <code>{f_today} posts today</code>\n"
            else:
                text += f"• 🔍 <b>{nick}</b> (Verifier): Active\n"
    else:
        text += "<i>No employees registered yet.</i>\n"

    await call.message.edit_text(
        text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🔄 Refresh", callback_data="admin_reports")],
            [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
        ]),
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# ADMIN EDITOR HUB & REFERENCE QUEUE MANAGEMENT
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_editor_hub")
async def cb_admin_editor_hub(call: CallbackQuery, state: FSMContext):
    await state.clear()
    pending_refs = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE status = 'pending'") or 0
    assigned_refs = await fetchval("SELECT COUNT(*) FROM editor_tasks WHERE status = 'assigned'") or 0
    review_tasks = await fetch("SELECT id, assigned_to, video_title FROM editor_tasks WHERE status = 'submitted' ORDER BY submitted_at ASC")

    buttons = []
    if review_tasks:
        for t in review_tasks:
            title_disp = f" ({t['video_title']})" if t['video_title'] else ""
            buttons.append([InlineKeyboardButton(text=f"🔍 Review Task #{t['id']}{title_disp}", callback_data=f"ed_review_prompt:{t['id']}")])

    buttons.append([InlineKeyboardButton(text="➕ Upload New Reference Task", callback_data="admin_add_editor_ref")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")])

    text = (
        "🎬 <b>Editor Task Management Hub</b>\n\n"
        f"• <b>Available in Queue:</b> <code>{pending_refs} tasks</code>\n"
        f"• <b>Currently In Progress:</b> <code>{assigned_refs} tasks</code>\n"
        f"• <b>Pending Your Review:</b> <code>{len(review_tasks)} tasks</code>\n\n"
        "Tap below to review submitted edited videos or upload new reference material:"
    )
    await call.message.edit_text(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data == "admin_add_editor_ref")
async def cb_admin_add_editor_ref(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "➕ <b>Upload Reference Material for Editors:</b>\n\n"
        "Send the reference material directly into this chat:\n"
        "• <b>Link or Text Description</b>\n"
        "• <b>Photo / Image</b>\n"
        "• <b>Video</b>\n"
        "• <b>Document / PDF</b>\n\n"
        "<i>(Attach any instructions in the caption or text message)</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_editor_hub")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_editor_ref)

@router.message(AdminStates.waiting_for_editor_ref)
async def process_admin_editor_ref_input(message: Message, state: FSMContext, bot: Bot):
    caption = message.caption or ""
    ref_type = "text"
    ref_content = message.text or ""

    if message.photo:
        ref_type = "photo"
        ref_content = message.photo[-1].file_id
        caption = message.caption or ""
    elif message.video:
        ref_type = "video"
        ref_content = message.video.file_id
        caption = message.caption or ""
    elif message.document:
        ref_type = "document"
        ref_content = message.document.file_id
        caption = message.caption or ""

    if not ref_content and not caption:
        return await message.answer("⚠️ Please provide reference text, a link, or a media file.")

    await execute(
        """
        INSERT INTO editor_tasks (ref_type, ref_content, ref_caption, status)
        VALUES ($1, $2, $3, 'pending')
        """,
        ref_type, ref_content, caption
    )
    await state.clear()

    display_content = (ref_content[:60] + "...") if len(ref_content) > 60 else ref_content
    await send_notification_log(
        bot,
        f"🎬 <b>[New Editor Reference Task Queued]</b>\n"
        f"• <b>Admin:</b> <code>{message.from_user.id}</code>\n"
        f"• <b>Type:</b> <code>{ref_type.upper()}</code>\n"
        f"• <b>Reference:</b> {display_content}\n"
        f"• <b>Instructions:</b> {caption if caption else 'None'}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Editor Hub", callback_data="admin_editor_hub")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(
        f"✅ <b>Reference Task Successfully Queued for Editors!</b>\n\n"
        f"• <b>Type:</b> <code>{ref_type.upper()}</code>\n"
        f"• <b>Reference:</b> {display_content}",
        reply_markup=kb,
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# ADMIN REVIEW & EDIT DECISION LOGIC
# ---------------------------------------------------------------------------
@router.callback_query(F.data.startswith("ed_review_prompt:"))
async def cb_ed_review_prompt(call: CallbackQuery, bot: Bot):
    task_id = int(call.data.split(":")[1])
    task = await fetchrow("SELECT id, submitted_file_id, video_title, assigned_to, ref_caption FROM editor_tasks WHERE id = $1", task_id)

    if not task or not task["submitted_file_id"]:
        return await call.answer("⚠️ Video submission not found!", show_alert=True)

    editor_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", task["assigned_to"]) or f"ID {task['assigned_to']}"

    review_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve & Post to Inc Master", callback_data=f"edtask_appr:{task_id}")],
        [InlineKeyboardButton(text="✏️ Re-edit Title", callback_data=f"edtask_edittitle:{task_id}")],
        [InlineKeyboardButton(text="❌ Reject with Instructions", callback_data=f"edtask_rej:{task_id}")],
        [InlineKeyboardButton(text="🎬 Edit by Admin (Self Edit)", callback_data=f"edtask_selfedit:{task_id}")],
        [InlineKeyboardButton(text="🔙 Back to Editor Hub", callback_data="admin_editor_hub")]
    ])

    await bot.send_video(
        chat_id=call.message.chat.id,
        video=task["submitted_file_id"],
        caption=(
            f"🎬 <b>[Editor Submission Review: Task #{task_id}]</b>\n\n"
            f"• <b>Editor:</b> {editor_nick} (<code>{task['assigned_to']}</code>)\n"
            f"• <b>Proposed Title:</b> <code>{task['video_title'] or 'No Title Provided'}</code>\n"
            f"• <b>Reference Instructions:</b> {task['ref_caption'] or 'None'}\n\n"
            "Choose an approval decision below:"
        ),
        reply_markup=review_kb,
        parse_mode="HTML"
    )
    await call.answer()

@router.callback_query(F.data.startswith("edtask_edittitle:"))
async def cb_edtask_edittitle(call: CallbackQuery, state: FSMContext):
    task_id = int(call.data.split(":")[1])
    await state.update_data(target_ed_task_id=task_id)
    curr_title = await fetchval("SELECT video_title FROM editor_tasks WHERE id = $1", task_id) or "None"

    await call.message.reply(
        f"✏️ <b>Re-edit Video Title for Task #{task_id}:</b>\n\n"
        f"• <b>Current Title:</b> <code>{curr_title}</code>\n\n"
        "Send the new title below:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_editor_hub")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_reedit_title)
    await call.answer()

@router.message(AdminStates.waiting_for_reedit_title)
async def process_admin_reedit_title(message: Message, state: FSMContext):
    new_title = message.text.strip()
    data = await state.get_data()
    task_id = data.get("target_ed_task_id")

    if not new_title:
        return await message.answer("⚠️ Title cannot be empty.")

    await execute("UPDATE editor_tasks SET video_title = $1 WHERE id = $2", new_title, task_id)
    await state.clear()

    review_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Approve & Post to Inc Master", callback_data=f"edtask_appr:{task_id}")],
        [InlineKeyboardButton(text="✏️ Re-edit Title Again", callback_data=f"edtask_edittitle:{task_id}")],
        [InlineKeyboardButton(text="❌ Reject with Instructions", callback_data=f"edtask_rej:{task_id}")],
        [InlineKeyboardButton(text="🎬 Edit by Admin (Self Edit)", callback_data=f"edtask_selfedit:{task_id}")],
        [InlineKeyboardButton(text="🔙 Back to Editor Hub", callback_data="admin_editor_hub")]
    ])

    await message.answer(
        f"✅ <b>Title Updated Successfully!</b>\n\n"
        f"• <b>Task ID:</b> <code>#{task_id}</code>\n"
        f"• <b>Updated Title:</b> <code>{new_title}</code>\n\n"
        "You can now approve and post to Inc Master with this title as caption:",
        reply_markup=review_kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("edtask_appr:"))
async def cb_edtask_appr(call: CallbackQuery, bot: Bot):
    task_id = int(call.data.split(":")[1])
    task = await fetchrow("SELECT submitted_file_id, video_title, assigned_to FROM editor_tasks WHERE id = $1", task_id)
    if not task:
        return await call.answer("⚠️ Task record missing!", show_alert=True)

    await execute("UPDATE editor_tasks SET status = 'approved', reviewed_at = NOW() WHERE id = $1", task_id)

    title_caption = task["video_title"] or f"🎬 [Master Video #{task_id}]"

    inc_masters = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'inc_master'")
    posted_count = 0
    for im in inc_masters:
        try:
            target = int(im["target_chat"]) if im["target_chat"].lstrip('-').isdigit() else im["target_chat"]
            await bot.send_video(chat_id=target, video=task["submitted_file_id"], caption=title_caption)
            posted_count += 1
        except Exception as e:
            logging.error(f"Error posting to Inc Master {im['target_chat']}: {e}")

    try:
        await bot.send_message(
            chat_id=task["assigned_to"],
            text=(
                f"🎉 <b>Great job! Your edited video for Task #{task_id} has been approved by the Admin and published to Inc Master!</b>\n\n"
                f"📌 <b>Title:</b> <code>{title_caption}</code>"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logging.warning(f"Could not notify editor: {e}")

    await send_notification_log(
        bot,
        f"👑 <b>[Editor Video Approved & Published]</b>\n"
        f"• <b>Task ID:</b> <code>#{task_id}</code>\n"
        f"• <b>Title:</b> {title_caption}\n"
        f"• <b>Editor:</b> <code>{task['assigned_to']}</code>\n"
        f"• <b>Delivered to Inc Master:</b> {posted_count} destination(s)"
    )

    await call.message.edit_reply_markup(reply_markup=None)
    await call.message.reply(f"✅ <b>Task #{task_id} Approved!</b> Video delivered to Inc Master with title as caption.", parse_mode="HTML")
    await call.answer("Approved!")

@router.callback_query(F.data.startswith("edtask_rej:"))
async def cb_edtask_rej(call: CallbackQuery, state: FSMContext):
    task_id = int(call.data.split(":")[1])
    await state.update_data(target_ed_task_id=task_id)

    await call.message.reply(
        f"📝 <b>Modification Instructions for Task #{task_id}:</b>\n\n"
        "Send the revision feedback / instructions for the editor:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_editor_hub")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_rejection_notes)
    await call.answer()

@router.message(AdminStates.waiting_for_rejection_notes)
async def process_rejection_notes(message: Message, state: FSMContext, bot: Bot):
    notes = message.text.strip()
    data = await state.get_data()
    task_id = data.get("target_ed_task_id")

    task = await fetchrow("SELECT assigned_to, ref_type, ref_content, ref_caption FROM editor_tasks WHERE id = $1", task_id)
    if not task:
        await state.clear()
        return await message.answer("⚠️ Task not found.")

    await execute(
        "UPDATE editor_tasks SET status = 'rejected', rejection_notes = $1 WHERE id = $2",
        notes, task_id
    )
    await state.clear()

    editor_id = task["assigned_to"]
    alert_msg = (
        f"⚠️ <b>Modification Requested for Task #{task_id}!</b>\n\n"
        f"📝 <b>Admin Instructions:</b>\n{notes}\n\n"
        "Please review the reference and instructions, then tap <b>Start Task</b> to resubmit."
    )

    try:
        if task["ref_type"] == "photo":
            await bot.send_photo(chat_id=editor_id, photo=task["ref_content"], caption=alert_msg, parse_mode="HTML")
        elif task["ref_type"] == "video":
            await bot.send_video(chat_id=editor_id, video=task["ref_content"], caption=alert_msg, parse_mode="HTML")
        elif task["ref_type"] == "document":
            await bot.send_document(chat_id=editor_id, document=task["ref_content"], caption=alert_msg, parse_mode="HTML")
        else:
            await bot.send_message(chat_id=editor_id, text=f"🔗 <b>Reference:</b> {task['ref_content']}\n\n{alert_msg}", parse_mode="HTML")
    except Exception as err:
        logging.warning(f"Could not alert editor {editor_id}: {err}")

    await send_notification_log(
        bot,
        f"❌ <b>[Editor Task Modification Requested]</b>\n"
        f"• <b>Task ID:</b> <code>#{task_id}</code>\n"
        f"• <b>Editor:</b> <code>{editor_id}</code>\n"
        f"• <b>Feedback:</b> {notes}"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Editor Hub", callback_data="admin_editor_hub")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Modification Request Sent!</b> Task #{task_id} returned to Editor.", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("edtask_selfedit:"))
async def cb_edtask_selfedit(call: CallbackQuery, state: FSMContext):
    task_id = int(call.data.split(":")[1])
    await state.update_data(target_ed_task_id=task_id)

    await call.message.reply(
        f"✏️ <b>Admin Self-Edit for Task #{task_id}:</b>\n\n"
        "Please send your own revised/edited video to finalize this task and post it directly to Inc Master.\n"
        "<i>(Attach any caption to serve as the title, or the existing title will be used)</i>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_editor_hub")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_admin_self_edit)
    await call.answer()

@router.message(AdminStates.waiting_for_admin_self_edit, F.video | F.document)
async def process_admin_self_edit_video(message: Message, state: FSMContext, bot: Bot):
    video = message.video
    if not video and message.document:
        if message.document.mime_type and message.document.mime_type.startswith("video/"):
            video = message.document
        else:
            return await message.answer("⚠️ Please send a valid video file.")

    data = await state.get_data()
    task_id = data.get("target_ed_task_id")
    file_id = video.file_id

    existing_title = await fetchval("SELECT video_title FROM editor_tasks WHERE id = $1", task_id)
    final_title = message.caption or existing_title or f"🎬 [Master Video #{task_id}]"

    await execute(
        "UPDATE editor_tasks SET submitted_file_id = $1, video_title = $2, status = 'approved', reviewed_at = NOW() WHERE id = $3",
        file_id, final_title, task_id
    )
    await state.clear()

    inc_masters = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'inc_master'")
    for im in inc_masters:
        try:
            target = int(im["target_chat"]) if im["target_chat"].lstrip('-').isdigit() else im["target_chat"]
            await bot.send_video(chat_id=target, video=file_id, caption=final_title)
        except Exception as e:
            logging.error(f"Error delivering admin self-edited video: {e}")

    await send_notification_log(
        bot,
        f"✏️ <b>[Admin Self-Edit Finalized & Published]</b>\n"
        f"• <b>Task ID:</b> <code>#{task_id}</code>\n"
        f"• <b>Title:</b> {final_title}\n"
        f"• <b>Published to Inc Master by Admin</b>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🎬 Editor Hub", callback_data="admin_editor_hub")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"🎉 <b>Self-Edit Finalized!</b> Task #{task_id} published to Inc Master.", reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# DESTINATION MANAGEMENT (5 LAYERS)
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_destinations_menu")
async def cb_destinations_hub(call: CallbackQuery, state: FSMContext):
    await state.clear()
    await call.message.edit_text(
        "📢 <b>Manage Destinations (All Layers)</b>\n\n"
        "• <b>Prelayered Groups:</b> Initial broadcast from uploaders.\n"
        "• <b>1st Layer Groups:</b> Final videos delivered after sorting.\n"
        "• <b>2nd Layer Groups:</b> Where verifiers verify category correctness.\n"
        "• <b>3rd Layer Groups:</b> Delivered directly from Flezen Schedulers.\n"
        "• <b>Inc Master:</b> Final approved edited videos from editors.\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "⌨️ <b>Command Shortcut Available:</b>\n"
        "<code>/setdest (LAYER) (TARGET_CHAT) [DELAY]</code>\n\n"
        "<b>Examples:</b>\n"
        "• <code>/setdest third_layer @FlezenChannel</code>\n"
        "• <code>/setdest inc_master @MasterChannel</code>\n\n"
        "Or choose an option below to manage:",
        reply_markup=get_destinations_hub_kb(),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("dest_layer:"))
async def cb_dest_layer_view(call: CallbackQuery, state: FSMContext):
    await state.clear()
    layer = call.data.split(":")[1]
    
    layer_names = {
        "prelayered": "Prelayered Groups",
        "first_layer": "1st Layer Groups/Channels",
        "second_layer": "2nd Layer (Verifier) Groups",
        "third_layer": "3rd Layer (Flezen) Groups",
        "inc_master": "Inc Master Destination"
    }
    title = layer_names.get(layer, layer)

    dests = await fetch("SELECT id, target_chat, custom_delay FROM destinations WHERE layer_type = $1 ORDER BY id ASC", layer)

    buttons = []
    if dests:
        for d in dests:
            extra = f" (Delay: {d['custom_delay']}s)" if d["custom_delay"] else ""
            buttons.append([InlineKeyboardButton(text=f"📍 {d['target_chat']}{extra}", callback_data=f"dest_item:{d['id']}")])

    buttons.append([InlineKeyboardButton(text=f"➕ Add {title}", callback_data=f"dest_add_to:{layer}")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Destination Hub", callback_data="admin_destinations_menu")])

    await call.message.edit_text(
        f"<b>{title} Roster:</b>\n\n"
        f"Total destinations: <code>{len(dests)}</code>\n\n"
        "💡 <i>Command Shortcut:</i>\n"
        f"<code>/setdest {layer} (TARGET_CHAT)</code>\n\n"
        "Tap a destination to view details, configure delays, or revoke:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("dest_item:"))
async def cb_dest_item_inspect(call: CallbackQuery):
    dest_id = int(call.data.split(":")[1])
    dest = await fetchrow("SELECT id, layer_type, target_chat, custom_delay FROM destinations WHERE id = $1", dest_id)
    if not dest:
        return await call.answer("⚠️ Destination not found!", show_alert=True)

    layer = dest["layer_type"]
    chat = dest["target_chat"]
    c_delay = f"<code>{dest['custom_delay']}s</code>" if dest["custom_delay"] else "<i>Universal Default</i>"

    buttons = []
    if layer == "prelayered":
        buttons.append([InlineKeyboardButton(text="⏱️ Set Custom Delay", callback_data=f"dest_set_delay:{dest_id}")])
        buttons.append([InlineKeyboardButton(text="🔄 Reset Delay to Universal", callback_data=f"dest_reset_delay:{dest_id}")])

    buttons.append([InlineKeyboardButton(text="🗑️ Revoke Destination", callback_data=f"dest_revoke_action:{dest_id}")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Layer List", callback_data=f"dest_layer:{layer}")])

    await call.message.edit_text(
        f"📍 <b>Destination Details:</b>\n\n"
        f"• <b>Layer:</b> <code>{layer.upper()}</code>\n"
        f"• <b>Target Chat:</b> <code>{chat}</code>\n"
        f"• <b>Delay:</b> {c_delay}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("dest_set_delay:"))
async def cb_dest_set_delay(call: CallbackQuery, state: FSMContext):
    dest_id = int(call.data.split(":")[1])
    await state.update_data(target_dest_id=dest_id)
    await call.message.edit_text(
        "⏱️ <b>Set Custom Delay:</b>\n\nSend delay in seconds for this destination (e.g. <code>90</code>):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"dest_item:{dest_id}")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_dest_custom_delay)

@router.message(AdminStates.waiting_for_dest_custom_delay)
async def process_dest_custom_delay(message: Message, state: FSMContext, bot: Bot):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        return await message.answer("⚠️ Please provide a valid positive integer.")

    delay_val = int(text)
    data = await state.get_data()
    dest_id = data.get("target_dest_id")

    await execute("UPDATE destinations SET custom_delay = $1 WHERE id = $2", delay_val, dest_id)
    dest = await fetchrow("SELECT layer_type, target_chat FROM destinations WHERE id = $1", dest_id)
    await state.clear()

    await send_notification_log(
        bot,
        f"⏱️ <b>[Destination Delay Configured]</b>\n"
        f"• <b>Destination:</b> <code>{dest['target_chat']}</code>\n"
        f"• <b>Delay:</b> <code>{delay_val} seconds</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📍 View Destination", callback_data=f"dest_item:{dest_id}")],
        [InlineKeyboardButton(text="🔙 Layer Roster", callback_data=f"dest_layer:{dest['layer_type']}")]
    ])
    await message.answer(f"✅ <b>Custom Delay Updated!</b>\nNew delay: <code>{delay_val}s</code>", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("dest_reset_delay:"))
async def cb_dest_reset_delay(call: CallbackQuery):
    dest_id = int(call.data.split(":")[1])
    dest = await fetchrow("SELECT layer_type, target_chat FROM destinations WHERE id = $1", dest_id)
    await execute("UPDATE destinations SET custom_delay = NULL WHERE id = $1", dest_id)
    await call.answer("Reset to universal delay!", show_alert=True)
    call.data = f"dest_item:{dest_id}"
    await cb_dest_item_inspect(call)

@router.callback_query(F.data.startswith("dest_revoke_action:"))
async def cb_dest_revoke_action(call: CallbackQuery, bot: Bot):
    dest_id = int(call.data.split(":")[1])
    dest = await fetchrow("SELECT layer_type, target_chat FROM destinations WHERE id = $1", dest_id)
    if not dest:
        return await call.answer("⚠️ Destination already removed!", show_alert=True)

    layer = dest["layer_type"]
    chat = dest["target_chat"]
    await execute("DELETE FROM destinations WHERE id = $1", dest_id)

    await send_notification_log(
        bot,
        f"🗑️ <b>[Destination Revoked]</b>\n"
        f"• <b>Layer:</b> <code>{layer.upper()}</code>\n"
        f"• <b>Target Chat:</b> <code>{chat}</code>"
    )

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📂 Back to {layer.replace('_', ' ').title()}", callback_data=f"dest_layer:{layer}")],
        [InlineKeyboardButton(text="📢 Destination Hub", callback_data="admin_destinations_menu")]
    ])

    await call.message.edit_text(
        f"✅ <b>Destination Revoked Successfully!</b>\n\n<code>{chat}</code> has been removed from <b>{layer.upper()}</b>.",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data.startswith("dest_add_to:"))
async def cb_dest_add_to(call: CallbackQuery, state: FSMContext):
    layer = call.data.split(":")[1]
    await state.update_data(target_layer=layer)

    layer_display = layer.replace('_', ' ').title()

    await call.message.edit_text(
        f"➕ <b>Add Destination to {layer_display}:</b>\n\n"
        "Send the target Channel/Group ID (e.g. <code>-1001234567890</code>) or public username (e.g. <code>@MyChannel</code>):\n\n"
        "━━━━━━━━━━━━━━━━━━━━\n"
        "💡 <b>Command Shortcut Available:</b>\n"
        f"<code>/setdest {layer} (TARGET_CHAT) [DELAY]</code>",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data=f"dest_layer:{layer}")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_dest_target)

@router.message(AdminStates.waiting_for_dest_target)
async def process_dest_target_input(message: Message, state: FSMContext, bot: Bot):
    dest = message.text.strip()
    data = await state.get_data()
    layer = data.get("target_layer", "prelayered")
    await state.clear()

    _, confirmation_text = await register_and_verify_destination(bot, layer, dest, message.from_user.id)

    layer_title = layer.replace('_', ' ').title()
    dest_row = await fetchrow("SELECT id FROM destinations WHERE layer_type = $1 AND target_chat = $2", layer, dest)
    dest_id = dest_row["id"] if dest_row else None

    buttons = []
    if layer == "prelayered" and dest_id:
        buttons.append([InlineKeyboardButton(text="⏱️ Set Custom Delay for It", callback_data=f"dest_set_delay:{dest_id}")])
    buttons.append([InlineKeyboardButton(text=f"📂 View {layer_title} List", callback_data=f"dest_layer:{layer}")])
    buttons.append([InlineKeyboardButton(text="📢 Destination Hub", callback_data="admin_destinations_menu")])
    buttons.append([InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")])

    await message.answer(
        confirmation_text,
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# DESTINATION REGISTRATION HELPER
# ---------------------------------------------------------------------------
async def register_and_verify_destination(bot: Bot, layer: str, dest: str, admin_id: int, custom_delay: int | None = None) -> tuple[bool, str]:
    layer_title = layer.replace("_", " ").title()

    ping_success = False
    ping_error = ""
    try:
        chat_id = int(dest) if dest.lstrip('-').isdigit() else dest
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ <b>Destination Connected Successfully!</b>\n\n"
                f"• <b>Assigned Layer:</b> <code>{layer_title}</code>\n"
                "This chat is now linked to receive automated video tasks and broadcasts."
            ),
            parse_mode="HTML"
        )
        ping_success = True
    except Exception as err:
        ping_error = str(err)
        logging.warning(f"Could not ping destination {dest}: {err}")

    existing_id = await fetchval("SELECT id FROM destinations WHERE layer_type = $1 AND target_chat = $2", layer, dest)

    if existing_id:
        if custom_delay is not None:
            await execute("UPDATE destinations SET custom_delay = $1 WHERE id = $2", custom_delay, existing_id)
    else:
        await execute(
            """
            INSERT INTO destinations (layer_type, target_chat, custom_delay)
            VALUES ($1, $2, $3)
            """,
            layer, dest, custom_delay
        )

    delay_info = f"• <b>Custom Delay:</b> <code>{custom_delay} seconds</code>\n" if custom_delay else ""
    status_notice = (
        "🟢 <b>Destination Verified:</b> Ping message delivered directly to chat!"
        if ping_success
        else f"⚠️ <b>Notice:</b> Destination registered, but direct ping message failed.\n<i>(Ensure bot is added as Administrator with message rights: <code>{ping_error}</code>)</i>"
    )

    await send_notification_log(
        bot,
        f"📍 <b>[Destination Added & Verified]</b>\n"
        f"• <b>Admin:</b> <code>{admin_id}</code>\n"
        f"• <b>Layer:</b> <code>{layer_title}</code>\n"
        f"• <b>Target Chat:</b> <code>{dest}</code>\n"
        f"• <b>Status:</b> {'Delivered' if ping_success else 'Pending Bot Rights'}\n"
        f"{delay_info}"
    )

    confirmation_msg = (
        f"🎉 <b>Destination Added Successfully!</b>\n\n"
        f"• <b>Layer:</b> <code>{layer_title}</code>\n"
        f"• <b>Target Chat:</b> <code>{dest}</code>\n"
        f"{delay_info}\n"
        f"{status_notice}\n\n"
        "Ready for automated routing."
    )

    return ping_success, confirmation_msg

# ---------------------------------------------------------------------------
# COMMAND: /setdest
# ---------------------------------------------------------------------------
@router.message(Command("setdest"), StateFilter("*"))
async def cmd_setdest_direct(message: Message, state: FSMContext, bot: Bot):
    await state.clear()
    if not await is_admin(message.from_user.id):
        return await message.answer("⛔ Unauthorized.")

    parts = message.text.strip().split()
    if len(parts) < 3:
        return await message.answer(
            "⚠️ <b>How to Set Destinations via Command:</b>\n\n"
            "Format: <code>/setdest (LAYER) (TARGET_CHAT) [DELAY_SECONDS]</code>\n\n"
            "<b>Available Layers:</b>\n"
            "• <code>prelayered</code>\n"
            "• <code>first_layer</code>\n"
            "• <code>second_layer</code>\n"
            "• <code>third_layer</code> (Flezen)\n"
            "• <code>inc_master</code>\n\n"
            "<b>Example:</b>\n"
            "• <code>/setdest third_layer @FlezenChannel</code>",
            parse_mode="HTML"
        )

    raw_layer = parts[1].lower()
    dest = parts[2].strip()
    custom_delay = int(parts[3]) if (len(parts) > 3 and parts[3].isdigit()) else None

    if raw_layer in ["prelayered", "pre", "1", "prelayer"]:
        layer = "prelayered"
    elif raw_layer in ["first_layer", "first", "1st", "layer1"]:
        layer = "first_layer"
    elif raw_layer in ["second_layer", "second", "2nd", "layer2", "verifier"]:
        layer = "second_layer"
    elif raw_layer in ["third_layer", "third", "3rd", "layer3", "flezen", "flezen_layer"]:
        layer = "third_layer"
    elif raw_layer in ["inc_master", "inc", "master", "5", "incmaster"]:
        layer = "inc_master"
    else:
        return await message.answer("⚠️ Invalid Layer! Choose: <code>prelayered</code>, <code>first_layer</code>, <code>second_layer</code>, <code>third_layer</code>, or <code>inc_master</code>.", parse_mode="HTML")

    _, confirmation_text = await register_and_verify_destination(bot, layer, dest, message.from_user.id, custom_delay)

    layer_title = layer.replace("_", " ").title()
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"📂 View {layer_title} Roster", callback_data=f"dest_layer:{layer}")],
        [InlineKeyboardButton(text="📢 Destination Hub", callback_data="admin_destinations_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(confirmation_text, reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# ADMIN PREDEFINED CATEGORIES MANAGEMENT
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_categories_menu")
async def cb_categories_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    cats = await fetch("SELECT id, name, use_count FROM categories ORDER BY use_count DESC, name ASC")

    buttons = []
    if cats:
        for c in cats:
            buttons.append([
                InlineKeyboardButton(text=f"🏷️ {c['name']} ({c['use_count']} uses)", callback_data="noop"),
                InlineKeyboardButton(text="🗑️ Delete", callback_data=f"cat_del:{c['id']}")
            ])

    buttons.append([InlineKeyboardButton(text="➕ Add New Category", callback_data="cat_add")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Admin Menu", callback_data="admin_menu")])

    await call.message.edit_text(
        "🏷️ <b>Manage Predefined Sorter Categories</b>\n\nCategories available for Sorters to pick:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons),
        parse_mode="HTML"
    )

@router.callback_query(F.data == "cat_add")
async def cb_cat_add(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "➕ <b>Add Predefined Category:</b>\n\nSend category name (e.g. <code>Highlights</code>):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_categories_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_new_category)

@router.message(AdminStates.waiting_for_new_category)
async def process_cat_add_input(message: Message, state: FSMContext):
    cat_name = message.text.strip()
    if not cat_name:
        return await message.answer("⚠️ Please provide a valid non-empty category name.")

    await execute("INSERT INTO categories (name) VALUES ($1) ON CONFLICT (name) DO NOTHING", cat_name)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏷️ Categories Menu", callback_data="admin_categories_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Category Added Successfully!</b>\n• Name: <code>{cat_name}</code>", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data.startswith("cat_del:"))
async def cb_cat_del(call: CallbackQuery, state: FSMContext):
    cat_id = int(call.data.split(":")[1])
    await execute("DELETE FROM categories WHERE id = $1", cat_id)
    await call.answer("🗑️ Category deleted!", show_alert=True)
    await cb_categories_menu(call, state)

# ---------------------------------------------------------------------------
# SORTER INTERACTIONS
# ---------------------------------------------------------------------------
@router.message(F.text == "▶️ Start Sorting Task")
async def msg_sorter_start_task_btn(message: Message, state: FSMContext, bot: Bot):
    await sorter_fetch_next_task(message.from_user.id, message, state, bot)

@router.callback_query(F.data == "sorter_next_task")
async def cb_sorter_next_task(call: CallbackQuery, state: FSMContext, bot: Bot):
    await call.answer()
    await sorter_fetch_next_task(call.from_user.id, call.message, state, bot)

@router.callback_query(F.data.startswith("sort_more:"))
async def cb_sort_more_categories(call: CallbackQuery):
    vid_id = int(call.data.split(":")[1])
    cats = await fetch("SELECT name FROM categories ORDER BY name ASC")

    buttons = []
    row = []
    for c in cats:
        row.append(InlineKeyboardButton(text=c["name"], callback_data=f"sort_pick:{vid_id}:{c['name']}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)

    buttons.append([InlineKeyboardButton(text="🔙 Back to Quick Options", callback_data=f"sort_back:{vid_id}")])
    await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))

@router.callback_query(F.data.startswith("sort_back:"))
async def cb_sort_back_quick(call: CallbackQuery):
    vid_id = int(call.data.split(":")[1])
    kb = await build_sorter_category_kb(vid_id)
    await call.message.edit_reply_markup(reply_markup=kb)

@router.callback_query(F.data.startswith("sort_skip:"))
async def cb_sort_skip(call: CallbackQuery, state: FSMContext, bot: Bot):
    vid_id = int(call.data.split(":")[1])
    await execute("UPDATE videos SET status = 'pending_sort', sorter_id = NULL WHERE id = $1", vid_id)
    await call.answer("Video returned to queue.")
    await call.message.delete()
    await sorter_fetch_next_task(call.from_user.id, call.message, state, bot)

@router.callback_query(F.data.startswith("sort_pick:"))
async def cb_sort_pick(call: CallbackQuery, state: FSMContext, bot: Bot):
    parts = call.data.split(":")
    vid_id = int(parts[1])
    chosen_cat = parts[2]
    user_id = call.from_user.id

    await execute(
        "UPDATE categories SET use_count = use_count + 1, last_used_at = NOW() WHERE name = $1",
        chosen_cat
    )

    video = await fetchrow("SELECT file_id, caption FROM videos WHERE id = $1", vid_id)
    if not video:
        return await call.answer("⚠️ Video record missing!", show_alert=True)

    file_id = video["file_id"]
    new_caption = chosen_cat

    await execute(
        "UPDATE videos SET status = 'sorted', caption = $1, sorter_id = $2, sorted_at = NOW() WHERE id = $3",
        new_caption, user_id, vid_id
    )

    sorter_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or "Sorter"

    first_layers = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'first_layer'")
    for fl in first_layers:
        try:
            target = int(fl["target_chat"]) if fl["target_chat"].lstrip('-').isdigit() else fl["target_chat"]
            await bot.send_video(chat_id=target, video=file_id, caption=new_caption)
        except Exception as e:
            logging.error(f"Error forwarding to 1st Layer {fl['target_chat']}: {e}")

    second_layers = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'second_layer'")
    verify_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Correct", callback_data=f"verify_act:{vid_id}:correct"),
            InlineKeyboardButton(text="❌ Wrong", callback_data=f"verify_act:{vid_id}:wrong")
        ]
    ])
    verifier_caption = (
        f"🔍 <b>[Verification Check Needed]</b>\n\n"
        f"• <b>Video ID:</b> <code>#{vid_id}</code>\n"
        f"• <b>Sorter:</b> {sorter_nick} (<code>{user_id}</code>)\n"
        f"• <b>Assigned Category:</b> <code>{chosen_cat}</code>\n"
        f"• <b>Previous Caption:</b> <code>{video['caption'] or 'None'}</code>\n\n"
        "👉 <i>Is this category correct or wrong? (Organizer / Verifier Only)</i>"
    )
    for sl in second_layers:
        try:
            target = int(sl["target_chat"]) if sl["target_chat"].lstrip('-').isdigit() else sl["target_chat"]
            await bot.send_video(chat_id=target, video=file_id, caption=verifier_caption, reply_markup=verify_kb, parse_mode="HTML")
        except Exception as e:
            logging.error(f"Error forwarding to 2nd Layer {sl['target_chat']}: {e}")

    await send_notification_log(
        bot,
        f"🏷️ <b>[Video Sorted & Distributed]</b>\n"
        f"• <b>Sorter:</b> {sorter_nick} (<code>{user_id}</code>)\n"
        f"• <b>Video ID:</b> <code>#{vid_id}</code>\n"
        f"• <b>Category:</b> <code>{chosen_cat}</code>\n"
        f"• Forwarded to 1st Layer & 2nd Layer (Verifier)"
    )

    next_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➡️ Next Video", callback_data="sorter_next_task")]
    ])

    await call.message.edit_caption(
        caption=f"✅ <b>Categorized as:</b> <code>{chosen_cat}</code>\nDelivered to 1st Layer and 2nd Layer Groups!",
        reply_markup=next_kb,
        parse_mode="HTML"
    )
    await call.answer(f"Saved as {chosen_cat}!")

# ---------------------------------------------------------------------------
# 2ND LAYER VERIFIER INTERACTIONS
# ---------------------------------------------------------------------------
@router.callback_query(F.data.startswith("verify_act:"))
async def cb_verify_action(call: CallbackQuery, bot: Bot):
    parts = call.data.split(":")
    vid_id = int(parts[1])
    decision = parts[2]
    user_id = call.from_user.id

    if not await is_organizer(bot, call.message.chat.id, user_id):
        return await call.answer("⛔ Access Denied! Only the group organizer or verifier can verify this video.", show_alert=True)

    verifier_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or call.from_user.full_name or f"User {user_id}"

    if decision == "correct":
        await execute("UPDATE videos SET verification_status = 'approved', verifier_id = $1 WHERE id = $2", user_id, vid_id)

        await send_notification_log(
            bot,
            f"✅ <b>[Video Verified: Approved]</b>\n"
            f"• <b>Video ID:</b> <code>#{vid_id}</code>\n"
            f"• <b>Organizer / Verifier:</b> {verifier_nick} (<code>{user_id}</code>)\n"
            f"• <b>Verdict:</b> Approved as Correct"
        )

        current_caption = call.message.caption or ""
        updated_caption = f"{current_caption}\n\n━━━━━━━━━━━━━━━━━━━━\n✅ <b>Verdict:</b> APPROVED by {verifier_nick}"
        await call.message.edit_caption(caption=updated_caption, reply_markup=None, parse_mode="HTML")
        await call.answer("✅ Approved!")

    elif decision == "wrong":
        cats = await fetch("SELECT name FROM categories ORDER BY name ASC")
        buttons = []
        row = []
        for c in cats:
            row.append(InlineKeyboardButton(text=c["name"], callback_data=f"v_set_cat:{vid_id}:{c['name']}"))
            if len(row) == 2:
                buttons.append(row)
                row = []
        if row:
            buttons.append(row)

        buttons.append([InlineKeyboardButton(text="🔙 Cancel", callback_data=f"v_back:{vid_id}")])
        await call.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons))
        await call.answer("❌ Select the correct category below:")

@router.callback_query(F.data.startswith("v_back:"))
async def cb_verify_back(call: CallbackQuery, bot: Bot):
    if not await is_organizer(bot, call.message.chat.id, call.from_user.id):
        return await call.answer("⛔ Access Denied!", show_alert=True)

    vid_id = int(call.data.split(":")[1])
    verify_kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Correct", callback_data=f"verify_act:{vid_id}:correct"),
            InlineKeyboardButton(text="❌ Wrong", callback_data=f"verify_act:{vid_id}:wrong")
        ]
    ])
    await call.message.edit_reply_markup(reply_markup=verify_kb)
    await call.answer()

@router.callback_query(F.data.startswith("v_set_cat:"))
async def cb_verify_set_category(call: CallbackQuery, bot: Bot):
    user_id = call.from_user.id
    if not await is_organizer(bot, call.message.chat.id, user_id):
        return await call.answer("⛔ Access Denied!", show_alert=True)

    parts = call.data.split(":")
    vid_id = int(parts[1])
    corrected_cat = parts[2]

    await execute("UPDATE categories SET use_count = use_count + 1, last_used_at = NOW() WHERE name = $1", corrected_cat)
    await execute("UPDATE videos SET verification_status = 'wrong_corrected', caption = $1, verifier_id = $2 WHERE id = $3", corrected_cat, user_id, vid_id)

    verifier_nick = await fetchval("SELECT nickname FROM employees WHERE user_id = $1", user_id) or call.from_user.full_name or f"User {user_id}"

    video = await fetchrow("SELECT file_id FROM videos WHERE id = $1", vid_id)
    if video:
        first_layers = await fetch("SELECT target_chat FROM destinations WHERE layer_type = 'first_layer'")
        for fl in first_layers:
            try:
                target = int(fl["target_chat"]) if fl["target_chat"].lstrip('-').isdigit() else fl["target_chat"]
                await bot.send_video(chat_id=target, video=video["file_id"], caption=corrected_cat)
            except Exception as e:
                logging.error(f"Error forwarding corrected video to 1st Layer: {e}")

    await send_notification_log(
        bot,
        f"❌ <b>[Category Corrected by Organizer]</b>\n"
        f"• <b>Video ID:</b> <code>#{vid_id}</code>\n"
        f"• <b>Organizer / Verifier:</b> {verifier_nick} (<code>{user_id}</code>)\n"
        f"• <b>New Category:</b> <code>{corrected_cat}</code>\n"
        f"• Forwarded corrected video to 1st Layer"
    )

    current_caption = call.message.caption or ""
    updated_caption = f"{current_caption}\n\n━━━━━━━━━━━━━━━━━━━━\n❌ <b>Verdict:</b> WRONG ➔ Corrected to <b>{corrected_cat}</b> by {verifier_nick}"
    await call.message.edit_caption(caption=updated_caption, reply_markup=None, parse_mode="HTML")
    await call.answer(f"Corrected to {corrected_cat}!")

@router.message(F.text == "📊 My Sort Stats")
async def msg_sorter_stats(message: Message):
    user_id = message.from_user.id
    today_count = await fetchval("SELECT COUNT(*) FROM videos WHERE sorter_id = $1 AND sorted_at >= CURRENT_DATE", user_id) or 0
    total_count = await fetchval("SELECT COUNT(*) FROM videos WHERE sorter_id = $1 AND status = 'sorted'", user_id) or 0

    await message.answer(
        f"📊 <b>Your Sorting Performance:</b>\n\n"
        f"• <b>Sorted Today:</b> <code>{today_count} videos</code>\n"
        f"• <b>All-Time Sorted:</b> <code>{total_count} videos</code>",
        parse_mode="HTML"
    )

@router.message(F.text == "🔍 Check Verification Stats")
async def msg_verifier_stats(message: Message):
    user_id = message.from_user.id
    verified_today = await fetchval("SELECT COUNT(*) FROM videos WHERE verifier_id = $1 AND sorted_at >= CURRENT_DATE", user_id) or 0
    total_verified = await fetchval("SELECT COUNT(*) FROM videos WHERE verifier_id = $1", user_id) or 0

    await message.answer(
        f"🔍 <b>Verifier Activity:</b>\n\n"
        f"• <b>Verified Today:</b> <code>{verified_today} videos</code>\n"
        f"• <b>All-Time Verified:</b> <code>{total_verified} videos</code>",
        parse_mode="HTML"
    )

# ---------------------------------------------------------------------------
# NOTIFICATION LOG SETTINGS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_notif_channel_menu")
async def cb_notif_channel_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    current_channel = await get_setting("notification_log_channel", "")
    display = f"<code>{current_channel}</code>" if current_channel else "<i>Not Configured</i>"

    buttons = [
        [InlineKeyboardButton(text="✏️ Set Log Channel", callback_data="notif_set")],
    ]
    if current_channel:
        buttons.append([InlineKeyboardButton(text="🗑️ Revoke Log Channel", callback_data="notif_revoke")])
    buttons.append([InlineKeyboardButton(text="🔙 Back to Menu", callback_data="admin_menu")])

    await call.message.edit_text(f"🔔 <b>Notification Log Channel</b>\n\n• Active: {display}", reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons), parse_mode="HTML")

@router.callback_query(F.data == "notif_set")
async def cb_notif_set(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "🔔 <b>Set Notification Log Channel:</b>\n\nSend Channel ID or @username:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_notif_channel_menu")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_notif_channel)

@router.message(AdminStates.waiting_for_notif_channel)
async def process_notif_channel_input(message: Message, state: FSMContext, bot: Bot):
    channel = message.text.strip()
    await set_setting("notification_log_channel", channel)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔔 Log Channel Menu", callback_data="admin_notif_channel_menu")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Notification Channel Saved!</b>\nTarget: <code>{channel}</code>", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "notif_revoke")
async def cb_notif_revoke(call: CallbackQuery):
    await set_setting("notification_log_channel", "")
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✏️ Set New Log Channel", callback_data="notif_set")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await call.message.edit_text("🗑️ <b>Notification Channel Revoked!</b>", reply_markup=kb, parse_mode="HTML")

# ---------------------------------------------------------------------------
# UNIVERSAL DELAY SETTINGS
# ---------------------------------------------------------------------------
@router.callback_query(F.data == "admin_universal_delay")
async def cb_universal_delay_menu(call: CallbackQuery, state: FSMContext):
    await state.clear()
    mode = await get_setting("delay_mode", "fixed")
    fixed_val = await get_setting("fixed_delay", "60")
    rnd_min = await get_setting("random_min", "30")
    rnd_max = await get_setting("random_max", "120")

    mode_display = "🎲 <b>Random Delay Active</b>" if mode == "random" else "⏱️ <b>Fixed Delay Active</b>"

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔀 Toggle Mode", callback_data="delay_toggle_mode")],
        [InlineKeyboardButton(text="✏️ Set Fixed Delay Seconds", callback_data="delay_set_fixed")],
        [InlineKeyboardButton(text="🎲 Set Random Range (Min Max)", callback_data="delay_set_random")],
        [InlineKeyboardButton(text="🔙 Back to Menu", callback_data="admin_menu")]
    ])

    await call.message.edit_text(
        f"⏱️ <b>Universal Delay Settings</b>\n\n"
        f"• Mode: {mode_display}\n"
        f"• Fixed: <code>{fixed_val}s</code> | Random: <code>{rnd_min}s - {rnd_max}s</code>",
        reply_markup=kb,
        parse_mode="HTML"
    )

@router.callback_query(F.data == "delay_toggle_mode")
async def cb_delay_toggle_mode(call: CallbackQuery, state: FSMContext):
    curr_mode = await get_setting("delay_mode", "fixed")
    new_mode = "random" if curr_mode == "fixed" else "fixed"
    await set_setting("delay_mode", new_mode)
    await call.answer(f"Switched to {new_mode.upper()} mode!", show_alert=True)
    await cb_universal_delay_menu(call, state)

@router.callback_query(F.data == "delay_set_fixed")
async def cb_delay_set_fixed(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "⏱️ <b>Set Fixed Universal Delay:</b>\n\nSend seconds between posts:",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_universal_delay")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_fixed_delay)

@router.message(AdminStates.waiting_for_fixed_delay)
async def process_fixed_delay_input(message: Message, state: FSMContext):
    text = message.text.strip()
    if not text.isdigit() or int(text) < 1:
        return await message.answer("⚠️ Please provide a valid positive integer.")

    await set_setting("fixed_delay", text)
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏱️ Delay Menu", callback_data="admin_universal_delay")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Fixed Delay Updated!</b>\nNew Delay: <code>{text}s</code>", reply_markup=kb, parse_mode="HTML")

@router.callback_query(F.data == "delay_set_random")
async def cb_delay_set_random(call: CallbackQuery, state: FSMContext):
    await call.message.edit_text(
        "🎲 <b>Set Universal Random Delay Range:</b>\n\nSend min and max seconds (e.g. <code>30 90</code>):",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="❌ Cancel", callback_data="admin_universal_delay")]
        ]),
        parse_mode="HTML"
    )
    await state.set_state(AdminStates.waiting_for_random_delay)

@router.message(AdminStates.waiting_for_random_delay)
async def process_random_delay_input(message: Message, state: FSMContext):
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return await message.answer("⚠️ Send two numbers separated by a space (e.g. <code>30 90</code>).", parse_mode="HTML")

    min_val, max_val = int(parts[0]), int(parts[1])
    if min_val >= max_val or min_val < 1:
        return await message.answer("⚠️ Minimum must be less than maximum.")

    await set_setting("random_min", str(min_val))
    await set_setting("random_max", str(max_val))
    await set_setting("delay_mode", "random")
    await state.clear()

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏱️ Delay Menu", callback_data="admin_universal_delay")],
        [InlineKeyboardButton(text="👑 Admin Menu", callback_data="admin_menu")]
    ])
    await message.answer(f"✅ <b>Random Delay Configured!</b>\nRange: <code>{min_val}s - {max_val}s</code>", reply_markup=kb, parse_mode="HTML")
