## KitobIjara – Telegram kitob ijarasi boti

Oddiy Telegram bot (Python + aiogram 3) talaba va yoshlar uchun kitoblarni ijaraga berish uchun.

### Tuzilishi

- `main.py` – asosiy bot kodi
- `db.py` – SQLite DB (kitoblar, ijaralar, jarima, bildirishnomalar)
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
   - Lokal development uchun `.env` avtomatik yuklanadi. Production (Render) uchun `.env` shart emas — env vars'ni dashboarddan berasiz.

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

Admin menyudan **\"➕ Kitob qo'shish\"** orqali bot ichidan kitob qo'shadi (UI orqali).

---

## Deploy to Render (24/7)

Bu bot **polling** rejimida ishlaydi, shuning uchun Render’da **Background Worker** sifatida deploy qilish tavsiya qilinadi (Web service emas).

### 1) Render sozlamalari (tavsiya etilgan)

- **Service type**: Background Worker
- **Build command**: `pip install -r requirements.txt`
- **Start command**: `python main.py`

### 2) Environment variables (majburiy)

- **`BOT_TOKEN`**: `@BotFather` dan olingan token
- **`ADMIN_IDS`**: admin Telegram ID’lari (vergul bilan), masalan: `123456789,987654321`

Ixtiyoriy:
- `REMINDERS_ENABLED=1`
- `PENALTY_PER_DAY_DEFAULT=2000`

### 3) GitHub repo ulash

1. Render’da **New + → Background Worker** tanlang.
2. GitHub repo’ni ulang.
3. Yuqoridagi build/start commandlarni kiriting.
4. Env vars qo‘shing (**tokenni hech qachon repo’ga commit qilmang**).
5. Deploy qiling.

### 4) SQLite haqida muhim eslatma

Bot `bot.db` (SQLite) ni loyiha papkasida saqlaydi.

Render fayl tizimi **ephemeral** bo‘lishi mumkin, shuning uchun production’da:
- DB yo‘qolishi ehtimoli bor (redeploy/restart).

Hozircha bu oddiy usage uchun yetarli. Keyinchalik Postgres kabi persistent DB’ga o‘tish tavsiya etiladi (hozir implement qilinmagan).

