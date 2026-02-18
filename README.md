## KitobIjara – Telegram kitob ijarasi boti

Oddiy Telegram bot (Python + aiogram 3) talaba va yoshlar uchun kitoblarni ijaraga berish uchun.

### Tuzilishi

- `main.py` – asosiy bot kodi
- `books.json` – kitoblar ro'yxati (mahalliy saqlash, JSON)
- `rentals.json` – ijaralar ro'yxati (mahalliy saqlash, JSON)
- `requirements.txt` – kerakli Python kutubxonalari

### Talablar

- Python 3.10 yoki yuqori

### O'rnatish

1. Loyihaga kiring:

   ```bash
   cd c:\Users\user\Desktop\KitobIjara
   ```

2. Virtual muhit (ixtiyoriy, lekin tavsiya etiladi):

   ```bash
   python -m venv venv
   venv\Scripts\activate
   ```

3. Kutubxonalarni o'rnating:

   ```bash
   pip install -r requirements.txt
   ```

4. Telegramdan bot token oling (`@BotFather`).

5. Muhit o'zgaruvchilarini o'rnating:

   - `BOT_TOKEN` — bot token (majburiy)
   - `ADMIN_IDS` — adminlarning Telegram IDlari (vergul bilan ajratilgan). Ixtiyoriy fallback: `ADMIN_ID` (bitta ID yoki vergul bilan ajratilgan). Agar ikkalasi ham o'rnatilmasa yoki bo'sh qoldirilsa, admin funksiyalar o'chiriladi.

   PowerShell misol:

   ```powershell
   setx BOT_TOKEN "Sizning_Bot_Tokeningiz"
   setx ADMIN_IDS "8548504697,123456789"
   ```

   Yoki joriy sessiyada:

   ```powershell
   $env:BOT_TOKEN="Sizning_Bot_Tokeningiz"
   $env:ADMIN_IDS="8548504697,123456789"
   ```

6. `.env` fayli yordamida sozlash (tavsiya etiladi)

   - `.env` faylini loyihaning ildiziga joylashtiring yoki `.env.example` faylini nusxa ko'chirib to'ldiring.
   - Loyihada `python-dotenv` o'rnatilgan bo'lsa, `.env` avtomatik yuklanadi va `BOT_TOKEN` hamda `ADMIN_IDS` (yoki `ADMIN_ID`) o'qiladi.

   Misol `.env` tarkibi:

   ```ini
   BOT_TOKEN=123456:ABC-DEF1234ghIkl-zyx57W2v1u123ew11
   ADMIN_IDS=123456789,987654321
   ```

### Botni ishga tushirish

```bash
python main.py
```

Bot ishga tushgach, Telegramda botga `/start` yozing.

### Adminlar va bildirishnomalar

Bot yangi ijaralar yaratilganda avtomatik ravishda `ADMIN_IDS` dagi barcha administratorlarga xabar yuboradi. Adminlar ijarani tasdiqlash, bekor qilish, yetkazib berish tafsilotlarini jo'natish yoki lokatsiyani yuborish kabi tugmalarni ishlata oladi.

### Kitoblarni qo'shish (admin uchun)

Yangi kitob qo'shish uchun `main.py` ichidagi `INITIAL_BOOKS` ro'yxatini tahrirlang va botni qayta ishga tushiring. Misol:

```python
INITIAL_BOOKS: List[Book] = [
    Book(id=1, title="Java dasturlash asoslari", author="A. Karimov", category="Dasturlash", status="available", prices={"7":10000,"14":18000,"30":30000}),
    Book(id=5, title="Yangi kitob nomi", author="Muallif", category="Turkum", status="available", prices={"7":5000}),
]
```

ID lar takrorlanmasligiga e'tibor bering.

