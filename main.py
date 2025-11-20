import os
from datetime import datetime
import re
import asyncio
import logging
from collections import deque, defaultdict
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor

# Render environment variables orqali o‘qiydi:
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMINS = os.getenv("ADMINS", "").split(",")

from parser import extract_invoice_data, write_to_report

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# -----------------------------
# Foydalanuvchi konteksti
# -----------------------------
BURST_ITEMS = defaultdict(list)        # uid -> [types.Message, ...]
BURST_DEBOUNCE = 1.0                   # oqim tugashini kutish (sek)
BURST_LAST_TS = {}                     # uid -> last message ts (float)

PENDING_FILES = defaultdict(deque)     # uid -> deque([message_doc, ...])
PENDING_NAMES = defaultdict(deque)     # uid -> deque(["NODIRAKA", ...])

DEFAULT_PLACES = {}                    # uid -> "Toshkent"
AWAITING_PLACE = defaultdict(list)     # uid -> list[{"data":dict, "customer":str}]

# 🔒 Har foydalanuvchi uchun flush lock + task-versiyalash
USER_FLUSH_LOCKS = defaultdict(lambda: asyncio.Lock())
FLUSH_TASKS = {}                       # uid -> asyncio.Task (faqat oxirgisi amal qiladi)

# ✅ /done uchun flag: joy so'raganimizdan keyin otchotni yuborishni eslab turamiz
DONE_WAITING = set()                   # uid lar

TMP_DIR = Path("invoices/tmp")
TMP_DIR.mkdir(parents=True, exist_ok=True)
Path("reports").mkdir(exist_ok=True)

# ---------------------------------
# Yordamchilar
# ---------------------------------
def clean_name(text: str) -> str:
    if not text:
        return ""
    t = re.sub(r"[\/|\\]+", " ", text)   # // -> bo'shliq
    t = re.sub(r"\s+", " ", t)
    return t.strip(" -:–—")

def is_admin(message: types.Message) -> bool:
    return message.from_user and message.from_user.id in ADMINS

def is_excel(doc: types.Document) -> bool:
    name = (doc.file_name or "").lower()
    return name.endswith(".xlsx") or name.endswith(".xls")

def looks_like_logistics(text: str) -> bool:
    """DAP/FCA/CPT/CIP/CIF/DDP, 'г.', vergul va h.k. bo'lsa — logistika."""
    low = text.lower()
    bad_markers = ("dap", "fca", "cpt", "cip", "cif", "ddp", "г.", "город", ",", " - ")
    return any(b in low for b in bad_markers)

def extract_customer_from_text(text: str) -> str | None:
    """
    1) Prefiksli: 'Mijoz:', 'M:', 'Client:', 'Customer:' -> mijoz.
    2) Prefikssiz: matn mijoz ismiga o'xshasa va logistika belgilarini o'zida tutmasa -> mijoz.
    """
    if not text:
        return None

    raw = text.strip()
    low = raw.lower()

    # 1) Prefiksli holatlar
    for p in ("mijoz:", "m:", "client:", "customer:"):
        if low.startswith(p):
            name = clean_name(raw[len(p):])
            return name if 2 <= len(name) <= 80 else None

    # 2) Prefikssiz — logistika bo'lsa, mijoz emas
    if looks_like_logistics(raw):
        return None

    # Raqamlar bo'lsa — ehtiyot uchun mijoz emas
    if re.search(r"\d", raw):
        return None

    # Faqat ruxsat etilgan belgilar
    cand = re.sub(r"[^A-Za-zА-Яа-яЁёЎўҚқҒғҲҳÄÖÜäöüİıŞşÇçĞğʼ'\-\s]", "", raw)
    cand = clean_name(cand)

    # So'zlar soni va uzunligi mantiqiy bo'lsin
    if not (2 <= len(cand) <= 80):
        return None
    if not (1 <= len(cand.split()) <= 4):
        return None

    return cand or None

def current_report_path() -> str:
    """Joriy oylik otchot fayli yo'li."""
    ym = datetime.now().strftime("%Y_%m")
    return f"reports/otschot_{ym}.xlsx"

# ---------------------------------
# /start va /setplace
# ---------------------------------
@dp.message_handler(commands=['start'])
async def send_welcome(message: types.Message):
    await message.reply(
        "📦 Assalomu alaykum!\n"
        "Forward: 1) Excel fayl ➜ 2) Alohida mijoz ismi (keyingi xabar)\n"
        "— bir urinishda ko‘p juftlikni forward qilsangiz bo‘ladi.\n\n"
        "Standart yuk joyi: `/setplace Toshkent`\n"
        "Yakuniy otchotni olish: `/done` (forward tugagach)"
    )

@dp.message_handler(commands=['setplace'])
async def set_default_place(message: types.Message):
    if not is_admin(message):
        return
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply("ℹ️ Foydalanish: /setplace Toshkent, Chilonzor")
        return
    DEFAULT_PLACES[message.from_user.id] = parts[1].strip()
    await message.reply(f"✅ Standart yuk tushirish joyi o‘rnatildi: {parts[1].strip()}")

# ---------------------------------
# /done — yakuniy otchotni yuborish
# ---------------------------------
@dp.message_handler(commands=['done'])
async def cmd_done(message: types.Message):
    if not is_admin(message):
        return
    uid = message.from_user.id

    # 1) Hali flush bo'lmagan burst bo'lsa, majburan yakunlaymiz
    await flush_burst(uid)

    # 2) Agar default joy yo'q va juftliklar kutilyapti — joy so'raymiz, so'ng faylni yuboramiz
    if AWAITING_PLACE[uid] and (uid not in DEFAULT_PLACES):
        DONE_WAITING.add(uid)
        await message.reply("📍 Yuk tushirish joyini kiriting (masalan: SIRDARYO). Joydan so‘ng yakuniy otchotni yuboraman.")
        return

    # 3) Agar default joy bor bo'lsa — AWAITING_PLACE dagilarni ham yozamiz
    if AWAITING_PLACE[uid] and (uid in DEFAULT_PLACES):
        place = DEFAULT_PLACES[uid]
        pairs = AWAITING_PLACE[uid][:]
        AWAITING_PLACE[uid].clear()
        for item in pairs:
            data = item["data"]
            customer = item["customer"]
            write_to_report(data, place, customer)

    # 4) Joriy oy otchotini yuboramiz (bo'lsa)
    rp = current_report_path()
    if os.path.exists(rp):
        await message.answer_document(open(rp, "rb"), caption="📊 Yakuniy otchot (joriy oy)")
    else:
        await message.reply("ℹ️ Joriy oy uchun otchot fayli topilmadi.")

# ---------------------------------
# BURST yig‘ish va ishlov berish
# ---------------------------------
async def flush_burst(uid: int):
    """
    Atomar flush: lock bilan himoyalangan.
    Boshlashda BURST_ITEMS ni nusxalab olib, DARHOL tozalaymiz —
    parallel flush'lar bo‘lsa ham ikkilamchi ishlov bo‘lmaydi.
    """
    async with USER_FLUSH_LOCKS[uid]:
        items = BURST_ITEMS.get(uid, [])
        if not items:
            return
        # ⚠️ Muhim: copy + clear boshida
        items_local = items[:]
        BURST_ITEMS[uid].clear()

    # ❗ Fayldan oldin kelgan eski nomlar ta'sir qilmasligi uchun navbatni tozalaymiz
    PENDING_NAMES[uid].clear()

    # Lock tashqarisida og‘ir ishlarni qilamiz
    items_local.sort(key=lambda m: (m.date.timestamp() if m.date else 0.0))

    for m in items_local:
        # 1) Excel fayl navbatiga
        if m.content_type == types.ContentType.DOCUMENT and is_excel(m.document):
            PENDING_FILES[uid].append(m)

        # 2) Mijoz matni — FAQAT fayl kutayotgan bo‘lsa qabul qilamiz
        elif m.content_type == types.ContentType.TEXT:
            nm = extract_customer_from_text(m.text or "")
            if nm and PENDING_FILES[uid]:
                # faqat "fayldan keyin" kelgan ismni olamiz
                PENDING_NAMES[uid].append(nm)
            else:
                # logistika yoki faylsiz ism — e'tiborsiz
                pass

        # Har qo‘shimchadan so‘ng juftliklarni ishlatamiz
        await try_pair_and_process(uid)

async def try_pair_and_process(uid: int):
    """
    FIFO: 1-fayl <-> 1-mijoz.
    Joy bo'lsa — darhol hisobotga Yoziladi (lekin hech narsa yuborilmaydi).
    Joy bo'lmasa — juftlik vaqtincha AWAITING_PLACE ga yig'iladi.
    """
    while PENDING_FILES[uid] and PENDING_NAMES[uid]:
        file_msg = PENDING_FILES[uid].popleft()
        customer = PENDING_NAMES[uid].popleft()

        # Faylni tmp'ga aniq nom bilan yuklab olamiz
        uniq = file_msg.document.file_unique_id
        orig = file_msg.document.file_name or "invoice.xlsx"
        safe_name = f"{uniq}__{orig}"
        dest_path = TMP_DIR / safe_name
        await file_msg.document.download(destination=dest_path)

        # Excel'dan ma'lumot
        try:
            data = extract_invoice_data(str(dest_path))
        except Exception as e:
            await file_msg.answer(f"❌ Parse xatosi: {e}")
            continue

        if uid in DEFAULT_PLACES:
            place = DEFAULT_PLACES[uid]
            # ✅ Faqat yozamiz, hech nima yubormaymiz — yakunda /done da yuboramiz
            write_to_report(data, place, customer)
        else:
            # Joy yo'q — keyin bitta xabar bilan kiritasiz
            AWAITING_PLACE[uid].append({"data": data, "customer": customer})

    # Joy so'rash (agar default yo'q va juftliklar yig'ilgan bo'lsa)
    if AWAITING_PLACE[uid] and (uid not in DEFAULT_PLACES):
        await bot.send_message(uid, "📍 Yuk tushirish joyini kiriting (masalan: SIRDARYO):")

def add_to_burst(uid: int, message: types.Message):
    """
    Xabarni burst buferiga qo‘shadi va debounce flush bajaradi.
    Faqat OXIRGI task amal qiladi (versiyalash).
    """
    now = asyncio.get_event_loop().time()
    BURST_ITEMS[uid].append(message)
    BURST_LAST_TS[uid] = now

    async def _debounced_flush(u: int, started_at: float):
        await asyncio.sleep(BURST_DEBOUNCE)
        # Orada yangi xabar kelgan bo‘lsa — bu task eskirgan
        if BURST_LAST_TS.get(u, 0) > started_at:
            return
        await flush_burst(u)

    # to'g'ri: uid bilan saqlaymiz
    FLUSH_TASKS[uid] = asyncio.create_task(_debounced_flush(uid, now))


# ---------------------------------
# Handlerlar
# ---------------------------------
@dp.message_handler(content_types=types.ContentType.DOCUMENT)
async def on_document(message: types.Message):
    if not is_admin(message):
        await message.reply("⛔ Sizda ruxsat yo‘q.")
        return
    if not is_excel(message.document):
        await message.reply("ℹ️ Faqat .xlsx/.xls fayllarni yuboring.")
        return
    add_to_burst(message.from_user.id, message)

@dp.message_handler(content_types=types.ContentType.TEXT)
async def on_text(message: types.Message):
    if not is_admin(message):
        return

    uid = message.from_user.id
    text = (message.text or "").strip()

    # Agar joy kutayotgan bo‘lsak va bu text mijoz emas — joy sifatida qabul qilamiz
    if AWAITING_PLACE[uid] and not extract_customer_from_text(text):
        place = text
        pairs = AWAITING_PLACE[uid][:]
        AWAITING_PLACE[uid].clear()
        for item in pairs:
            data = item["data"]
            customer = item["customer"]
            write_to_report(data, place, customer)

        # Agar /done kutilayotgan bo'lsa — endi yakuniy otchotni ham yuboramiz
        if uid in DONE_WAITING:
            DONE_WAITING.discard(uid)
            rp = current_report_path()
            if os.path.exists(rp):
                await message.answer_document(open(rp, "rb"), caption="📊 Yakuniy otchot (joriy oy)")
            else:
                await message.reply("ℹ️ Joriy oy uchun otchot fayli topilmadi.")
        return

    # Aks holda — bu burst oqimining mijoz qismi bo‘lishi mumkin
    if extract_customer_from_text(text):
        add_to_burst(uid, message)
        return

    # Boshqa oddiy text — e'tiborsiz qoldiramiz
    # xohlasangiz: await message.reply("ℹ️ Mijoz nomi uchun `Mijoz: ISM` yoki ism yuboring.")


# ---------------------------------
# /reset
# ---------------------------------
@dp.message_handler(commands=["reset"])
async def reset_report(message: types.Message):
    if not is_admin(message):
        await message.reply("⛔ Sizda ruxsat yo‘q.")
        return
    today = datetime.today()
    report_filename = f"reports/otschot_{today.year}_{today.month:02}.xlsx"
    if os.path.exists(report_filename):
        try:
            os.remove(report_filename)
            await message.answer("✅ Bu oygi hisobot fayli o‘chirildi.")
        except Exception as e:
            await message.answer(f"❌ O‘chirishda xatolik: {e}")
    else:
        await message.answer("ℹ️ Bu oy uchun hisobot fayli topilmadi.")


# ---------------------------------
# Run
# ---------------------------------
if __name__ == '__main__':
    executor.start_polling(dp, skip_updates=True)
