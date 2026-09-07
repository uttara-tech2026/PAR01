# Telegram Multi-Link & Video Uploader Bot (Neon DB Edition)

Production-ready bot built with **aiogram 3.x** and **Neon DB (Serverless PostgreSQL with `asyncpg`)** for deployment on **Railway**.

---

## 🔑 Environment Configuration

Update your `.env` file (or Railway's Variables tab):

```env
BOT_TOKEN=8422244235:AAF9MGhqyyzH2dzXPg74eMlWzs2ymy05YNU
ADMIN_ID=6427894095
DATABASE_URL=postgresql://neondb_owner:your_password@ep-xyz.us-east-2.aws.neon.tech/neondb?sslmode=require
```

---

## 🚀 Features & Commands

1. **Automatic Multi-Link Detection:**
   - Paste one or several Telegram links (`t.me/...` or `telegram.me/...`) in a single message.
   - The bot detects each link and prompts sequentially:
     - `📥 Downloadable`
     - `⏩ Forwardable`
     - `⏭️ Skip This Link`
     - `❌ Cancel Remaining`
   - Added links enter the upload queue automatically.

2. **Exclusive Task Locking:**
   - When an uploader clicks `▶️ Start Task`, a pending link is atomically locked via PostgreSQL `SKIP LOCKED`.
   - No other uploader can take or view that link until completed.

3. **Employee Management:**
   - Command: `/setemp (ROLE) (NUMERIC ID) (NICKNAME)`
   - Inline menu under `Manage Employees` -> `Uploader Role` with per-employee revoke and add actions.

4. **Live Counter & Deduplication:**
   - Previous counter messages are automatically deleted as new videos arrive.
   - Rejects duplicate videos via Telegram's `file_unique_id`.
   - Prompts for video category and displays today's completed link count.
