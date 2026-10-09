"""Telegram bot: заказ еды + заявки. Версия для Vercel (вебхук + Upstash Redis)."""
import asyncio
import contextvars
import hashlib
import html
import json
import logging
import os
import re
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

import aiohttp
from aiogram import Bot, Dispatcher, F, Router
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey
from aiogram.types import CallbackQuery, InlineKeyboardButton, Message, Update
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ---------- config (всё через env) ----------
TOKEN = os.environ.get("BOT_TOKEN", "").strip()
def _int_env(name, default):
    m = re.search(r"\d+", os.environ.get(name, ""))
    return int(m.group()) if m else default


# берём из значения только цифры, чтобы лишние буквы/пробелы не ломали запуск
ADMIN_IDS = {int(x) for x in re.findall(r"\d+", os.environ.get("ADMIN_IDS", ""))}
CARD_INFO = os.environ.get("CARD_INFO", "Карта: 0000 0000 0000 0000\nПолучатель: Имя Ф.")
OPEN_H = _int_env("OPEN_HOUR", 16)    # меню открывается
CLOSE_H = _int_env("CLOSE_HOUR", 21)  # приём заказов закрывается
CRON_SECRET = os.environ.get("CRON_SECRET", "").strip()
SECRET = hashlib.sha256(TOKEN.encode()).hexdigest()[:32]  # защита вебхука
KV_URL = (os.environ.get("KV_REST_API_URL") or os.environ.get("UPSTASH_REDIS_REST_URL") or "").strip()
KV_TOKEN = (os.environ.get("KV_REST_API_TOKEN") or os.environ.get("UPSTASH_REDIS_REST_TOKEN") or "").strip()
TZ = timezone(timedelta(hours=5))  # Ташкент, UTC+5, без перехода на летнее время

logging.basicConfig(level=logging.INFO)

# ---------- time ----------
def now():
    return datetime.now(TZ)


def phase():
    h = now().hour
    if h < OPEN_H:
        return "before"
    if h >= CLOSE_H:
        return "after"
    return "open"


def target_day():
    """День, на который сейчас собираются заказы (завтра)."""
    return (now().date() + timedelta(days=1)).isoformat()


# ---------- Redis (Upstash REST) ----------
_sess = contextvars.ContextVar("sess")
_cache = contextvars.ContextVar("cache")


async def kv(*cmd):
    s = _sess.get()
    async with s.post(
        KV_URL,
        json=[str(c) for c in cmd],
        headers={"Authorization": f"Bearer {KV_TOKEN}"},
    ) as resp:
        data = await resp.json()
    if isinstance(data, dict) and data.get("error"):
        raise RuntimeError(f"redis: {data['error']}")
    return data.get("result")


class KVStorage(BaseStorage):
    """Хранит шаги диалога (корзина, время, кабинет...) в Redis."""

    @staticmethod
    def _k(key: StorageKey):
        return f"fsm:{key.chat_id}:{key.user_id}"

    async def _load(self, key):
        k = self._k(key)
        cache = _cache.get()
        if k not in cache:
            raw = await kv("GET", k)
            cache[k] = json.loads(raw) if raw else {}
        return cache[k]

    async def _save(self, key):
        k = self._k(key)
        d = _cache.get()[k]
        if d.get("state") is None and not d.get("data"):
            await kv("DEL", k)
        else:
            await kv("SET", k, json.dumps(d, ensure_ascii=False), "EX", 172800)

    async def set_state(self, key, state=None):
        d = await self._load(key)
        d["state"] = state.state if isinstance(state, State) else state
        await self._save(key)

    async def get_state(self, key):
        return (await self._load(key)).get("state")

    async def set_data(self, key, data):
        d = await self._load(key)
        d["data"] = dict(data)
        await self._save(key)

    async def get_data(self, key):
        return dict((await self._load(key)).get("data") or {})

    async def close(self):
        pass


# ---------- данные ----------
async def get_setting(k, default=None):
    v = await kv("GET", f"setting:{k}")
    return v if v is not None else default


async def set_setting(k, v):
    await kv("SET", f"setting:{k}", str(v))


async def get_lang(uid):
    return (await kv("GET", f"lang:{uid}")) or "ru"


async def get_menu(day):
    raw = await kv("GET", f"menu:{day}")
    return json.loads(raw) if raw else None


async def save_order(o):
    await kv("SET", f"order:{o['id']}", json.dumps(o, ensure_ascii=False), "EX", 2592000)


async def new_order(o):
    oid = int(await kv("INCR", "seq:order"))
    o["id"] = oid
    await save_order(o)
    await kv("SADD", f"orders:{o['day']}", oid)
    await kv("EXPIRE", f"orders:{o['day']}", 2592000)
    return oid


async def get_order(oid):
    raw = await kv("GET", f"order:{oid}")
    return json.loads(raw) if raw else None


async def day_orders(day):
    ids = await kv("SMEMBERS", f"orders:{day}") or []
    ids = sorted(int(i) for i in ids)
    if not ids:
        return []
    raws = await kv("MGET", *[f"order:{i}" for i in ids])
    return [json.loads(x) for x in raws if x]


# ---------- texts ----------
TX = {
    "ru": {
        "hello": "Здравствуйте! Что хотите заказать?",
        "food": "🍽 Еда", "work": "📚 Отработка", "pres": "📊 Презентация", "art": "📝 Статья", "sup": "🛟 Поддержка",
        "before": "Меню на завтра откроется в {h}:00. Загляните позже 🙂",
        "after": "Извините, вы не успели 🙏 Приём заказов на завтра закончился в {h}:00. "
                 "Такие правила, будем рады видеть вас завтра!",
        "no_menu": "Меню на завтра пока не загружено. Загляните чуть позже.",
        "menu_head": "🛒 Ваш заказ на {day}\nНажимайте «Добавить» под блюдом выше.",
        "add": "➕ Добавить",
        "cart_empty": "Корзина пуста", "total": "Итого",
        "clear": "🗑 Очистить", "checkout": "✅ Оформить",
        "pay": "К оплате: {total}\n\nРеквизиты:\n{card}\n\n"
               "Переведите сумму и пришлите сюда чек из Payme или Click (скрин или файл).",
        "need_receipt": "Пришлите чек (скрин или файл) 🙏",
        "ask_time": "Во сколько принести завтра? (например 13:00)",
        "ask_room": "Номер кабинета?",
        "ask_name": "Ваши имя и фамилия?",
        "done": "Заказ принят ✅ Ждём подтверждение оплаты. Завтра в {time} принесём.",
        "paid": "Оплата подтверждена ✅ Заказ #{id} на завтра в силе.",
        "rejected": "Оплату по заказу #{id} не удалось подтвердить ❌ Напишите в поддержку.",
        "ask_req": "Опишите, что нужно (тема, срок, пожелания). Можно приложить файлы или фото:",
        "ask_sup": "В чём проблема? Опишите своими словами, можно приложить скрин:",
        "req_sent": "Отправили, скоро свяжемся ✅",
        "restart": "Нажмите «Еда» заново 🙂",
    },
    "uz": {
        "hello": "Assalomu alaykum! Nima buyurtma qilmoqchisiz?",
        "food": "🍽 Ovqat", "work": "📚 Otrabotka", "pres": "📊 Taqdimot", "art": "📝 Maqola", "sup": "🛟 Yordam",
        "before": "Ertangi menyu soat {h}:00 da ochiladi. Keyinroq kiring 🙂",
        "after": "Kechirasiz, ulgurmadingiz 🙏 Ertangi kunga buyurtma qabul qilish soat {h}:00 da tugadi. "
                 "Qoidalar shunday, ertaga kutamiz!",
        "no_menu": "Ertangi menyu hali yuklanmagan. Birozdan keyin kiring.",
        "menu_head": "🛒 {day} uchun buyurtmangiz\nYuqoridagi taom tagidagi «Qo'shish» tugmasini bosing.",
        "add": "➕ Qo'shish",
        "cart_empty": "Savat bo'sh", "total": "Jami",
        "clear": "🗑 Tozalash", "checkout": "✅ Rasmiylashtirish",
        "pay": "To'lov: {total}\n\nRekvizitlar:\n{card}\n\n"
               "Summani o'tkazing va Payme yoki Click chekini yuboring (skrin yoki fayl).",
        "need_receipt": "Chekni yuboring (skrin yoki fayl) 🙏",
        "ask_time": "Ertaga soat nechada olib kelaylik? (masalan 13:00)",
        "ask_room": "Xona raqami?",
        "ask_name": "Ism va familiyangiz?",
        "done": "Buyurtma qabul qilindi ✅ To'lov tasdig'ini kutamiz. Ertaga soat {time} da olib kelamiz.",
        "paid": "To'lov tasdiqlandi ✅ #{id} buyurtma ertaga uchun kuchda.",
        "rejected": "#{id} buyurtma to'lovini tasdiqlab bo'lmadi ❌ Yordamga yozing.",
        "ask_req": "Nima kerakligini yozing (mavzu, muddat, istaklar). Fayl yoki rasm ham yuborishingiz mumkin:",
        "ask_sup": "Muammo nimada? O'zingiz yozib bering, skrin ham yuborishingiz mumkin:",
        "req_sent": "Yuborildi, tez orada bog'lanamiz ✅",
        "restart": "«Ovqat» tugmasini qayta bosing 🙂",
    },
}


def t(lang, key, **kw):
    return TX[lang][key].format(**kw)


def fmt(n):
    return f"{n:,}".replace(",", " ") + " сум"



# ---------- menu parsing ----------
LINE = re.compile(
    r"^\W*?(\S.*?)\s*[-—–:|]*\s*(\d[\d\s.,]*)\s*(?:сум|сўм|som|so'm|uzs)?\s*$", re.I
)


def parse_menu(text):
    items = []
    for line in text.splitlines():
        m = LINE.match(line.strip())
        if not m:
            continue
        name = re.sub(r"^\d+[.)]\s*", "", m.group(1).strip(" -—–:|•*·."))
        price = int(re.sub(r"\D", "", m.group(2)) or 0)
        if name and price >= 1000:
            items.append({"n": name, "p": price})
    return items



# ---------- keyboards ----------
def main_kb(lang):
    kb = InlineKeyboardBuilder()
    for k in ("food", "work", "pres", "art", "sup"):
        kb.button(text=t(lang, k), callback_data=f"m:{k}")
    kb.adjust(1)
    return kb.as_markup()


def render(menu, cart, lang, day):
    lines, total = [], 0
    for i, qty in cart.items():
        it = menu[int(i)]
        s = it["p"] * qty
        total += s
        lines.append(f"• {it['n']} × {qty} = {fmt(s)}")
    body = "\n".join(lines) if lines else t(lang, "cart_empty")
    text = f"{t(lang, 'menu_head', day=day)}\n\n{body}\n\n{t(lang, 'total')}: {fmt(total)}"
    kb = InlineKeyboardBuilder()
    kb.row(
        InlineKeyboardButton(text=t(lang, "clear"), callback_data="clr"),
        InlineKeyboardButton(text=t(lang, "checkout"), callback_data="co"),
    )
    return text, kb.as_markup()


def cart_total(menu, cart):
    return sum(menu[int(i)]["p"] * q_ for i, q_ in cart.items())




# ---------- states ----------
class Order(StatesGroup):
    cart = State()
    receipt = State()
    time = State()
    room = State()
    name = State()


class Req(StatesGroup):
    text = State()


class Adm(StatesGroup):
    menu = State()
    markup = State()


r = Router()
is_admin = F.from_user.id.in_(ADMIN_IDS)


# ---------- summary ----------
async def build_summary(day):
    rows = [o for o in await day_orders(day) if o["status"] != "rejected"]
    if not rows:
        return f"📦 Заказов на {day} нет."
    agg = {}
    for o in rows:
        for it in o["items"]:
            agg[it["n"]] = agg.get(it["n"], 0) + it["q"]
    out = [f"📦 Заказы на {day}: {len(rows)} шт.\n", "🍳 Для ресторана:"]
    out += [f"• {n} × {k}" for n, k in agg.items()]
    out.append("\n👥 По людям:")
    for o in rows:
        its = ", ".join(f"{i['n']}×{i['q']}" for i in o["items"])
        icon = "✅" if o["status"] == "paid" else "⏳"
        out.append(f"{icon} #{o['id']} {o['fullname']} — каб. {o['room']} — {o['time']}\n   {its} — {fmt(o['total'])}")
    out.append("\n✅ оплата подтверждена, ⏳ ждёт подтверждения")
    return "\n".join(out)


async def send_long(bot, chat_id, text):
    chunk = ""
    for line in text.split("\n"):
        if len(chunk) + len(line) + 1 > 3900:
            await bot.send_message(chat_id, chunk)
            chunk = ""
        chunk += line + "\n"
    if chunk.strip():
        await bot.send_message(chat_id, chunk)


# ---------- start / language ----------
def admin_kb():
    kb = InlineKeyboardBuilder()
    kb.button(text="📋 Загрузить меню на завтра", callback_data="a:menu")
    kb.button(text="🍽 Какое меню сохранено", callback_data="a:showmenu")
    kb.button(text="💰 Изменить наценку", callback_data="a:markup")
    kb.button(text="📦 Заказы на завтра", callback_data="a:orders")
    kb.button(text="📖 Как пользоваться", callback_data="a:help")
    kb.button(text="👤 Посмотреть как клиент", callback_data="a:client")
    kb.adjust(1)
    return kb.as_markup()


async def admin_text():
    day = target_day()
    menu = await get_menu(day)
    menu_line = f"{len(menu)} блюд" if menu else "не загружено"
    return (
        f"🛠 Панель админа\n\n"
        f"Меню на {day}: {menu_line}\n"
        f"Наценка: {await get_setting('markup', '10')}%\n"
        f"Приём заказов: {OPEN_H}:00–{CLOSE_H}:00 (Ташкент)"
    )


def help_text():
    return (
        "📖 Как пользоваться\n\n"
        f"1. Каждый день до {OPEN_H}:00 нажми «Загрузить меню на завтра»\n"
        "2. Пересылай боту посты из канала: фото + подпись «Блюдо — цена». Каждое блюдо отдельным постом\n"
        "3. Когда всё скинул, отправь /done. Бот добавит наценку и сохранит меню\n"
        f"4. С {OPEN_H}:00 клиенты видят меню, заказы принимаются до {CLOSE_H}:00\n"
        "5. Приходит заказ с чеком: проверь оплату и нажми «✅ Оплата ок» или «❌ Отклонить». Клиент получит уведомление\n"
        f"6. После {CLOSE_H}:00 нажми «Заказы на завтра»: список для ресторана и по людям. Он также приходит сам раз в день\n\n"
        "Заявки на отработку, презентацию, статью и поддержку приходят сюда со ссылкой на клиента.\n"
        "Вернуться в панель: /start. Отмена любого действия: /start."
    )


async def ask_language(msg: Message):
    kb = InlineKeyboardBuilder()
    kb.button(text="🇷🇺 Русский", callback_data="lang:ru")
    kb.button(text="🇺🇿 O'zbekcha", callback_data="lang:uz")
    await msg.answer("Выберите язык / Tilni tanlang", reply_markup=kb.as_markup())


async def start_menu_upload(msg: Message, state: FSMContext):
    await state.set_state(Adm.menu)
    await state.update_data(items=[])
    await msg.answer(
        "Пересылай посты из канала (фото + подпись «Блюдо — цена», каждое блюдо отдельным постом).\n"
        f"Наценка сейчас {await get_setting('markup', '10')}%. Меню будет на {target_day()}.\n"
        "Когда всё скинул — /done. Отмена — /start."
    )


@r.message(CommandStart())
async def start(m: Message, state: FSMContext):
    await state.clear()
    if m.from_user.id in ADMIN_IDS:
        return await m.answer(await admin_text(), reply_markup=admin_kb())
    await ask_language(m)


@r.message(Command("admin"), is_admin)
async def admin_panel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer(await admin_text(), reply_markup=admin_kb())


@r.callback_query(F.data.startswith("a:"), is_admin)
async def admin_action(c: CallbackQuery, state: FSMContext, bot: Bot):
    act = c.data.split(":")[1]
    await c.answer()
    if act == "menu":
        await start_menu_upload(c.message, state)
    elif act == "showmenu":
        menu = await get_menu(target_day())
        if not menu:
            await c.message.answer("Меню на завтра ещё не загружено.", reply_markup=admin_kb())
        else:
            lines = "\n".join(f"• {i['n']} — {fmt(i['p'])}" for i in menu)
            await c.message.answer(f"Меню на {target_day()} (с наценкой):\n{lines}", reply_markup=admin_kb())
    elif act == "markup":
        await state.set_state(Adm.markup)
        await c.message.answer(
            f"Наценка сейчас {await get_setting('markup', '10')}%.\n"
            "Напиши новое число в процентах, например 15. Отмена — /start."
        )
    elif act == "orders":
        await send_long(bot, c.from_user.id, await build_summary(target_day()))
    elif act == "help":
        await c.message.answer(help_text(), reply_markup=admin_kb())
    elif act == "client":
        await ask_language(c.message)


@r.message(Adm.markup, F.text, is_admin)
async def adm_markup_value(m: Message, state: FSMContext):
    try:
        pct = float(m.text.strip().replace(",", ".").replace("%", ""))
        assert 0 <= pct <= 300
    except (ValueError, AssertionError):
        return await m.answer("Нужно число от 0 до 300, например 15. Или /start для отмены.")
    await set_setting("markup", pct)
    await state.clear()
    await m.answer(
        f"✅ Наценка {pct}%. Она применится к меню, которое загрузишь дальше.",
        reply_markup=admin_kb(),
    )


@r.callback_query(F.data.startswith("lang:"))
async def set_lang(c: CallbackQuery):
    lang = c.data.split(":")[1]
    await kv("SET", f"lang:{c.from_user.id}", lang)
    await c.message.answer(t(lang, "hello"), reply_markup=main_kb(lang))
    await c.answer()


# ---------- admin ----------
@r.message(Command("menu"), is_admin)
async def adm_menu(m: Message, state: FSMContext):
    await start_menu_upload(m, state)


@r.message(Command("markup"), is_admin)
async def adm_markup(m: Message):
    parts = (m.text or "").split()
    try:
        pct = float(parts[1].replace(",", "."))
    except (IndexError, ValueError):
        return await m.answer(f"Наценка сейчас {await get_setting('markup', '10')}%. Поменять: /markup 15")
    await set_setting("markup", pct)
    await m.answer(f"Наценка: {pct}% (действует на меню, загруженные после этого)")


@r.message(Command("orders"), is_admin)
async def adm_orders(m: Message, bot: Bot):
    await send_long(bot, m.from_user.id, await build_summary(target_day()))


@r.message(Command("done"), Adm.menu, is_admin)
async def adm_menu_done(m: Message, state: FSMContext):
    d = await state.get_data()
    items = d.get("items", [])
    if not items:
        return await m.answer("Пока ни одного блюда. Пересылай посты или /start для отмены.")
    pct = float(await get_setting("markup", "10"))
    final = [
        {
            "n": it["n"],
            "p": int(round(it["p"] * (1 + pct / 100) / 100.0)) * 100,
            "ph": it.get("ph"),
        }
        for it in items
    ]
    day = target_day()
    await kv("SET", f"menu:{day}", json.dumps(final, ensure_ascii=False), "EX", 604800)
    await state.clear()
    prev = "\n".join(f"• {i['n']} — {fmt(i['p'])}" for i in final)
    await m.answer(
        f"✅ Меню на {day} сохранено (наценка {pct}%):\n{prev}\n\n"
        f"Клиенты увидят его с {OPEN_H}:00, заказы до {CLOSE_H}:00.",
        reply_markup=admin_kb(),
    )


@r.message(Adm.menu, F.photo | F.text, is_admin)
async def adm_menu_item(m: Message, state: FSMContext):
    found = parse_menu(m.caption or m.text or "")
    if not found:
        return await m.answer("Не вижу название и цену в подписи, этот пост пропустил. Формат: «Плов — 25000».")
    if m.photo:
        for it in found:
            it["ph"] = m.photo[-1].file_id
    d = await state.get_data()
    items = d.get("items", []) + found
    await state.update_data(items=items)
    got = ", ".join(f"{i['n']} ({fmt(i['p'])})" for i in found)
    await m.answer(f"✔ {got}\nВсего блюд: {len(items)}. Ещё или /done")


@r.callback_query(F.data.regexp(r"^(ok|no):\d+$"))
async def decide(c: CallbackQuery, bot: Bot):
    if c.from_user.id not in ADMIN_IDS:
        return await c.answer("Нет доступа", show_alert=True)
    act, oid = c.data.split(":")
    o = await get_order(int(oid))
    if not o:
        return await c.answer("Заказ не найден", show_alert=True)
    o["status"] = "paid" if act == "ok" else "rejected"
    await save_order(o)
    lang = await get_lang(o["user_id"])
    key = "paid" if act == "ok" else "rejected"
    try:
        await bot.send_message(o["user_id"], t(lang, key, id=oid))
    except Exception:
        logging.exception("notify user")
    try:
        await c.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    await c.answer("✅ Подтверждено" if act == "ok" else "❌ Отклонено")


# ---------- main menu: отработка / презентация / статья / поддержка ----------
@r.callback_query(F.data.in_({"m:work", "m:pres", "m:art", "m:sup"}))
async def req_start(c: CallbackQuery, state: FSMContext):
    lang = await get_lang(c.from_user.id)
    kind = c.data.split(":")[1]
    await state.set_state(Req.text)
    await state.update_data(kind=t(lang, kind))
    await c.message.answer(t(lang, "ask_sup" if kind == "sup" else "ask_req"))
    await c.answer()


@r.message(Req.text)
async def req_text(m: Message, state: FSMContext, bot: Bot):
    lang = await get_lang(m.from_user.id)
    d = await state.get_data()
    u = m.from_user
    uname = f" @{u.username}" if u.username else ""
    head = (
        f"📨 <b>{html.escape(d['kind'])}</b>\n"
        f"От: <a href=\"tg://user?id={u.id}\">{html.escape(u.full_name)}</a>{uname} (id {u.id})"
    )
    for a in ADMIN_IDS:
        try:
            await bot.send_message(a, head, parse_mode="HTML")
            await bot.copy_message(a, m.chat.id, m.message_id)
        except Exception:
            logging.exception("send req")
    await state.clear()
    await m.answer(t(lang, "req_sent"), reply_markup=main_kb(lang))


# ---------- food flow ----------
@r.callback_query(F.data == "m:food")
async def food(c: CallbackQuery, state: FSMContext):
    lang = await get_lang(c.from_user.id)
    ph = phase()
    if ph == "before":
        await c.message.answer(t(lang, "before", h=OPEN_H))
        return await c.answer()
    if ph == "after":
        await c.message.answer(t(lang, "after", h=CLOSE_H))
        return await c.answer()
    day = target_day()
    menu = await get_menu(day)
    if not menu:
        await c.message.answer(t(lang, "no_menu"))
        return await c.answer()
    await c.answer()
    await state.set_state(Order.cart)
    await state.update_data(cart={}, day=day)
    for i, it in enumerate(menu):
        add_kb = InlineKeyboardBuilder()
        add_kb.button(text=t(lang, "add"), callback_data=f"add:{i}")
        cap = f"{it['n']} — {fmt(it['p'])}"
        if it.get("ph"):
            await c.message.answer_photo(it["ph"], caption=cap, reply_markup=add_kb.as_markup())
        else:
            await c.message.answer(cap, reply_markup=add_kb.as_markup())
    text, kb = render(menu, {}, lang, day)
    msg = await c.message.answer(text, reply_markup=kb)
    await state.update_data(cart_mid=msg.message_id)


@r.callback_query(F.data.startswith("add:"), Order.cart)
async def add_item(c: CallbackQuery, state: FSMContext, bot: Bot):
    lang = await get_lang(c.from_user.id)
    if phase() != "open":
        await state.clear()
        await c.message.answer(t(lang, "after", h=CLOSE_H))
        return await c.answer()
    d = await state.get_data()
    menu = await get_menu(d["day"])
    cart = d["cart"]
    i = c.data.split(":")[1]
    cart[i] = cart.get(i, 0) + 1
    await state.update_data(cart=cart)
    text, kb = render(menu, cart, lang, d["day"])
    try:
        await bot.edit_message_text(text, chat_id=c.message.chat.id, message_id=d["cart_mid"], reply_markup=kb)
    except Exception:
        pass
    await c.answer("✔")


@r.callback_query(F.data == "clr", Order.cart)
async def clear_cart(c: CallbackQuery, state: FSMContext, bot: Bot):
    lang = await get_lang(c.from_user.id)
    d = await state.get_data()
    await state.update_data(cart={})
    text, kb = render(await get_menu(d["day"]), {}, lang, d["day"])
    try:
        await bot.edit_message_text(text, chat_id=c.message.chat.id, message_id=d["cart_mid"], reply_markup=kb)
    except Exception:
        pass
    await c.answer()


@r.callback_query(F.data == "co", Order.cart)
async def checkout(c: CallbackQuery, state: FSMContext):
    lang = await get_lang(c.from_user.id)
    d = await state.get_data()
    if not d.get("cart"):
        return await c.answer(t(lang, "cart_empty"), show_alert=True)
    if phase() != "open":
        await state.clear()
        await c.message.answer(t(lang, "after", h=CLOSE_H))
        return await c.answer()
    total = cart_total(await get_menu(d["day"]), d["cart"])
    await state.update_data(total=total)
    await state.set_state(Order.receipt)
    await c.message.answer(t(lang, "pay", total=fmt(total), card=CARD_INFO))
    await c.answer()


# если состояние потерялось
@r.callback_query(F.data.regexp(r"^(add:\d+|clr|co)$"))
async def stale(c: CallbackQuery):
    lang = await get_lang(c.from_user.id)
    await c.message.answer(t(lang, "restart"), reply_markup=main_kb(lang))
    await c.answer()


@r.message(Order.receipt, F.photo | F.document)
async def got_receipt(m: Message, state: FSMContext):
    lang = await get_lang(m.from_user.id)
    if m.photo:
        await state.update_data(receipt=m.photo[-1].file_id, rkind="photo")
    else:
        await state.update_data(receipt=m.document.file_id, rkind="doc")
    await state.set_state(Order.time)
    await m.answer(t(lang, "ask_time"))


@r.message(Order.receipt)
async def need_receipt(m: Message):
    await m.answer(t(await get_lang(m.from_user.id), "need_receipt"))


@r.message(Order.time, F.text)
async def got_time(m: Message, state: FSMContext):
    await state.update_data(time=m.text.strip())
    await state.set_state(Order.room)
    await m.answer(t(await get_lang(m.from_user.id), "ask_room"))


@r.message(Order.room, F.text)
async def got_room(m: Message, state: FSMContext):
    await state.update_data(room=m.text.strip())
    await state.set_state(Order.name)
    await m.answer(t(await get_lang(m.from_user.id), "ask_name"))


@r.message(Order.name, F.text)
async def got_name(m: Message, state: FSMContext, bot: Bot):
    lang = await get_lang(m.from_user.id)
    d = await state.get_data()
    menu = await get_menu(d["day"])
    items = [{"n": menu[int(i)]["n"], "p": menu[int(i)]["p"], "q": qty} for i, qty in d["cart"].items()]
    o = {
        "user_id": m.from_user.id, "day": d["day"], "items": items, "total": d["total"],
        "receipt": d["receipt"], "rkind": d["rkind"], "time": d["time"], "room": d["room"],
        "fullname": m.text.strip(), "status": "pending", "created": now().isoformat(),
    }
    oid = await new_order(o)
    await state.clear()
    await m.answer(t(lang, "done", time=d["time"]), reply_markup=main_kb(lang))

    lines = "\n".join(f"• {i['n']} × {i['q']}" for i in items)
    caption = (
        f"🧾 Заказ #{oid} на {o['day']}\n👤 {o['fullname']}\n🏢 Каб: {o['room']}\n"
        f"⏰ {o['time']}\n{lines}\n💰 {fmt(o['total'])}"
    )[:1000]
    kb = InlineKeyboardBuilder()
    kb.button(text="✅ Оплата ок", callback_data=f"ok:{oid}")
    kb.button(text="❌ Отклонить", callback_data=f"no:{oid}")
    for a in ADMIN_IDS:
        try:
            if o["rkind"] == "photo":
                await bot.send_photo(a, o["receipt"], caption=caption, reply_markup=kb.as_markup())
            else:
                await bot.send_document(a, o["receipt"], caption=caption, reply_markup=kb.as_markup())
        except Exception:
            logging.exception("send order to admin")


dp = Dispatcher(storage=KVStorage())
dp.include_router(r)


# ---------- вебхук, расписание, настройка ----------
async def handle_update(data):
    async with aiohttp.ClientSession() as s:
        _sess.set(s)
        _cache.set({})
        # защита от повторной доставки одного и того же апдейта
        if await kv("SET", f"upd:{data.get('update_id')}", "1", "NX", "EX", 3600) is None:
            return
        bot = Bot(TOKEN)
        try:
            update = Update.model_validate(data, context={"bot": bot})
            await dp.feed_update(bot, update)
        finally:
            await bot.session.close()


async def cron_job():
    """Раз в день после закрытия приёма шлёт админам сводку на завтра."""
    async with aiohttp.ClientSession() as s:
        _sess.set(s)
        _cache.set({})
        day = target_day()
        if await kv("SET", f"sum:{day}", "1", "NX", "EX", 259200) is None:
            return "already sent"
        bot = Bot(TOKEN)
        try:
            text = await build_summary(day)
            for a in ADMIN_IDS:
                await send_long(bot, a, text)
        finally:
            await bot.session.close()
        return "sent"


async def setup_webhook(host):
    prod = os.environ.get("VERCEL_PROJECT_PRODUCTION_URL", "").strip()
    host = prod or host
    bot = Bot(TOKEN)
    try:
        url = f"https://{host}/api/index"
        await bot.set_webhook(url, secret_token=SECRET, allowed_updates=["message", "callback_query"])
        return f"OK. Вебхук установлен: {url}"
    finally:
        await bot.session.close()


async def notify_error(text):
    """Если что-то сломалось, бот пишет об этом админу."""
    if not TOKEN or not ADMIN_IDS:
        return
    bot = Bot(TOKEN)
    try:
        for a in ADMIN_IDS:
            await bot.send_message(a, ("⚠️ Ошибка бота:\n" + text)[:900])
    except Exception:
        logging.exception("notify_error failed")
    finally:
        await bot.session.close()


async def info():
    """Диагностика: что настроено и что видит Telegram."""
    out = [
        f"BOT_TOKEN: {'есть' if TOKEN else 'НЕТ'}",
        f"ADMIN_IDS: {len(ADMIN_IDS)} шт." if ADMIN_IDS else "ADMIN_IDS: НЕТ",
        f"Redis URL: {'есть' if KV_URL else 'НЕТ'}",
        f"Redis token: {'есть' if KV_TOKEN else 'НЕТ'}",
        f"CARD_INFO: {'задана' if 'CARD_INFO' in os.environ else 'НЕТ (стоит заглушка)'}",
        f"Часы приёма: {OPEN_H}:00-{CLOSE_H}:00, сейчас в Ташкенте {now().strftime('%H:%M')} ({phase()})",
        f"Домен проекта: {os.environ.get('VERCEL_PROJECT_PRODUCTION_URL', 'не известен')}",
    ]
    async with aiohttp.ClientSession() as s:
        _sess.set(s)
        _cache.set({})
        try:
            out.append(f"Redis PING: {await kv('PING')}")
        except Exception as e:
            out.append(f"Redis ОШИБКА: {type(e).__name__}: {e}")
    if TOKEN:
        bot = Bot(TOKEN)
        try:
            me = await bot.get_me()
            out.append(f"Бот: @{me.username}")
            wi = await bot.get_webhook_info()
            out.append(f"Вебхук: {wi.url or 'НЕ УСТАНОВЛЕН'}")
            out.append(f"Ждут доставки: {wi.pending_update_count}")
            if wi.last_error_message:
                out.append(f"Последняя ошибка Telegram: {wi.last_error_message}")
        except Exception as e:
            out.append(f"Telegram ОШИБКА: {type(e).__name__}: {e}")
        finally:
            await bot.session.close()
    return "\n".join(out)


class handler(BaseHTTPRequestHandler):
    def _reply(self, code, text):
        body = text.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self):
        n = int(self.headers.get("content-length", 0))
        body = self.rfile.read(n)
        if self.headers.get("x-telegram-bot-api-secret-token") != SECRET:
            return self._reply(403, "forbidden")
        try:
            asyncio.run(handle_update(json.loads(body)))
        except Exception as e:
            logging.exception("update failed")
            try:
                asyncio.run(notify_error(f"{type(e).__name__}: {e}"))
            except Exception:
                pass
        self._reply(200, "ok")  # всегда 200, чтобы Telegram не слал повторы

    def do_GET(self):
        qs = parse_qs(urlparse(self.path).query)
        task = qs.get("task", [""])[0]
        ua = self.headers.get("user-agent", "")
        try:
            if task == "info":
                return self._reply(200, asyncio.run(info()))
            if task == "setup":
                return self._reply(200, asyncio.run(setup_webhook(self.headers.get("host"))))
            if task == "cron" or ua.startswith("vercel-cron"):
                if CRON_SECRET and self.headers.get("authorization") != f"Bearer {CRON_SECRET}":
                    return self._reply(401, "unauthorized")
                return self._reply(200, asyncio.run(cron_job()))
        except Exception as e:
            logging.exception("get failed")
            return self._reply(500, f"error: {e}")
        self._reply(200, "bot is alive")
