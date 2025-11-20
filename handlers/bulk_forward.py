# handlers/bulk_forward.py
import asyncio
from datetime import datetime, timedelta
from typing import List, Optional, Union

from aiogram import Router, F
from aiogram.types import Message

from parser import extract_invoice_data, write_to_report  # parser.py dagi funksiyalar

router = Router()

BURST_TTL = timedelta(seconds=12)  # bitta multi-forward oynasi
DEBOUNCE = 1.0                     # oqim tugashini kutish
ACCEPT_EXT = (".xlsx", ".xls")     # kerak bo'lsa kengaytiring

BURSTS = {}  # user_id -> {"expires": dt, "items": list[Message]}

def _is_excel(name: Optional[str]) -> bool:
    return bool(name) and name.lower().endswith(ACCEPT_EXT)

def _extract_customer(text: Optional[str]) -> Optional[str]:
    if not text: return None
    t = text.strip(); low = t.lower()
    for p in ("mijoz:", "m:", "client:", "customer:"):
        if low.startswith(p):
            return t[len(p):].strip(" -:–—")
    return None  # prefikssiz matnlar mijoz emas

def _burst(u: int):
    now = datetime.utcnow()
    b = BURSTS.get(u, {"expires": now, "items": []})
    if b["expires"] < now: b = {"expires": now, "items": []}
    BURSTS[u] = b
    return b

def _add(u: int, m: Message):
    b = _burst(u)
    b["items"].append(m)
    b["expires"] = datetime.utcnow() + BURST_TTL

@router.message(F.document | F.text)
async def collect_and_process(msg: Message):
    _add(msg.from_user.id, msg)
    await _flush(msg.from_user.id)

@router.message(F.text == "/burst_end")
async def burst_end(msg: Message):
    BURSTS.pop(msg.from_user.id, None)
    await msg.answer("🧹 Burst yakunlandi. Endi yangi forward bilan yangi burst boshlang.")

async def _flush(u: int):
    b = _burst(u)
    await asyncio.sleep(DEBOUNCE)

    items: List[Message] = b["items"][:]
    b["items"].clear()
    if not items:
        return

    # vaqt bo'yicha tartib
    items.sort(key=lambda m: (m.date or datetime.utcnow()))

    # FIFO navbatlar: 1) fayl, 2) undan keyingi mijoz
    pending_files: List[Message] = []
    pending_names: List[str] = []

    async def try_pair_and_process():
        while pending_files and pending_names:
            file_msg = pending_files.pop(0)
            customer = pending_names.pop(0)

            # faylni yuklab-parsing → hisobot
            tg_file = await file_msg.bot.get_file(file_msg.document.file_id)
            fobj = await file_msg.bot.download_file(tg_file.file_path)

            data = extract_invoice_data(fobj)
            # Agar sizda joyni ham matndan olish bo'lsa, hozircha bo'sh qoldiramiz:
            report_path = write_to_report(data, delivery_place="", customer_name=customer)

            text = f"✅ Otchotga qo‘shildi: {file_msg.document.file_name} | 👤 {customer}"
            if report_path:
                text += f"\n🗂 {report_path}"
            await file_msg.answer(text)

    for m in items:
        if m.document and _is_excel(m.document.file_name):
            pending_files.append(m)
            await try_pair_and_process()
        elif m.text:
            nm = _extract_customer(m.text)
            if nm:
                pending_names.append(nm)
                await try_pair_and_process()

    # Juftlanmay qolganlar bo'lsa ogohlantirish
    for file_msg in pending_files:
        await file_msg.answer("⚠️ Bu fayl uchun mijoz matni topilmadi. Fayldan keyin `Mijoz: ...` yuboring.")
    if pending_names:
        await items[-1].answer("⚠️ Ba'zi `Mijoz:` matnlar uchun mos fayl topilmadi. Avval fayl, keyin `Mijoz:` yuboring.")

    BURSTS.pop(u, None)
