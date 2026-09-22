"""KiwBot 2.2 - Telegram webhook bot.

KiwBot combines:
- Local responses from data/respuestas.json and data/filtros.json.
- Groq through the official OpenAI-compatible Python client.
- Per-group persistent settings in SQLite.
- Persistent per-user/per-chat AI memory.
- Special identity recognition by Telegram ID.
- Internal KiwBot mute system.
- Duplicate Telegram update protection.
- Welcome/goodbye messages and rules configurable from Telegram.
- Moderation and admin commands.
"""

from __future__ import annotations

import json
import logging
import os
import random
import re
import sqlite3
import time
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from threading import RLock
from typing import Any
from urllib.parse import quote_plus, urlparse

import requests
from flask import Flask, jsonify, request
from openai import OpenAI


# ============================================================
# PATHS
# ============================================================

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_PATH = os.path.join(BASE_DIR, "kiwbot.db")


# ============================================================
# ENVIRONMENT
# ============================================================

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
GROQ_API_KEY = os.getenv("GROQ_API_KEY", "").strip()
TELEGRAM_WEBHOOK_SECRET = os.getenv("TELEGRAM_WEBHOOK_SECRET", "").strip()
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "").strip()

OWNER_TELEGRAM_ID = int(os.getenv("OWNER_ID", "7745029153"))
OWNER_NAME = os.getenv("OWNER_NAME", "Kiu")
OWNER_TITLE = os.getenv("OWNER_TITLE", "Amo")

REQUIRE_MENTION = os.getenv("REQUIRE_MENTION", "true").lower() == "true"

MODEL_NAME = os.getenv("GROQ_MODEL", "openai/gpt-oss-20b")
PORT = int(os.getenv("PORT", "5000"))

MAX_MEMORY_MESSAGES = 12
TELEGRAM_MAX_CHARS = 4000
TELEGRAM_TIMEOUT = 25
MAX_MEDIA_BYTES = 20 * 1024 * 1024

AUTO_MODERATION = os.getenv("AUTO_MODERATION", "true").lower() == "true"
MAX_WARNINGS = int(os.getenv("MAX_WARNINGS", "3"))
FLOOD_WINDOW_SECONDS = int(os.getenv("FLOOD_WINDOW_SECONDS", "8"))
FLOOD_MAX_MESSAGES = int(os.getenv("FLOOD_MAX_MESSAGES", "6"))

BANNED_WORDS = [
    x.strip().lower()
    for x in os.getenv("BANNED_WORDS", "").split(",")
    if x.strip()
]

BANNED_DOMAINS = [
    x.strip().lower()
    for x in os.getenv("BANNED_DOMAINS", "").split(",")
    if x.strip()
]


# ============================================================
# SPECIAL USERS
# ============================================================

# IMPORTANTE:
# La identidad se verifica por Telegram ID.
# Los nombres y apodos NO sirven para hacerse pasar por alguien.

SPECIAL_USERS: dict[int, dict[str, Any]] = {
    OWNER_TELEGRAM_ID: {
        "name": OWNER_NAME,
        "aliases": ["Amo", "Kiu"],
        "relationship": "owner",
    },

    # Kalu = Kat
    282157809: {
        "name": "Kalu",
        "aliases": [
            "Kalu",
            "Kat",
            "vaca",
            "gatita",
            "Kalutiesa™",
        ],
        "relationship": "special",
    },
}


# ============================================================
# LOGGING / APP
# ============================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO").upper(),
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger("kiwbot")

app = Flask(__name__)

executor = ThreadPoolExecutor(
    max_workers=4,
    thread_name_prefix="kiwbot-update",
)


# ============================================================
# MEMORY / LOCKS
# ============================================================

# Memoria temporal:
# clave = chat_id:user_id
memory: dict[str, deque[tuple[str, str]]] = defaultdict(
    lambda: deque(maxlen=MAX_MEMORY_MESSAGES)
)

memory_lock = RLock()

flood: dict[str, deque[float]] = defaultdict(deque)
flood_lock = RLock()

pending_settings: dict[str, dict[str, Any]] = {}
pending_lock = RLock()

outbound_ids: dict[str, deque[int]] = defaultdict(
    lambda: deque(maxlen=40)
)
outbound_lock = RLock()

response_history: dict[str, deque[str]] = defaultdict(
    lambda: deque(maxlen=8)
)
response_history_lock = RLock()


# ============================================================
# JSON DATA
# ============================================================

def load_json(name: str) -> dict[str, Any]:
    path = os.path.join(DATA_DIR, name)

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return data if isinstance(data, dict) else {}

    except Exception:
        logger.exception("No se pudo cargar %s", path)
        return {}


RESPONSES = load_json("respuestas.json")
FILTER_RESPONSES = load_json("filtros.json")


# ============================================================
# DATABASE
# ============================================================

def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=20)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with db() as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS chat_settings (
                chat_id INTEGER PRIMARY KEY,
                welcome_enabled INTEGER NOT NULL DEFAULT 0,
                welcome_text TEXT NOT NULL DEFAULT '',
                rules_text TEXT NOT NULL DEFAULT '',
                goodbye_enabled INTEGER NOT NULL DEFAULT 0,
                goodbye_text TEXT NOT NULL DEFAULT '',
                antispam_enabled INTEGER NOT NULL DEFAULT 1,
                antilink_enabled INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS warnings (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                count INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS filters (
                chat_id INTEGER NOT NULL,
                trigger TEXT NOT NULL,
                response TEXT NOT NULL,
                PRIMARY KEY(chat_id, trigger)
            );

            CREATE TABLE IF NOT EXISTS chat_users (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT NOT NULL DEFAULT '',
                first_name TEXT NOT NULL DEFAULT '',
                last_name TEXT NOT NULL DEFAULT '',
                updated_at INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(chat_id, user_id)
            );

            CREATE INDEX IF NOT EXISTS idx_chat_users_username
                ON chat_users(chat_id, username);

            CREATE TABLE IF NOT EXISTS bot_memory (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                role TEXT NOT NULL,
                content TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT 0
            );

            CREATE INDEX IF NOT EXISTS idx_bot_memory_user
                ON bot_memory(chat_id, user_id, created_at);

            CREATE TABLE IF NOT EXISTS bot_mutes (
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                muted_at INTEGER NOT NULL DEFAULT 0,
                reason TEXT NOT NULL DEFAULT '',
                PRIMARY KEY(chat_id, user_id)
            );

            CREATE TABLE IF NOT EXISTS processed_updates (
                update_id INTEGER PRIMARY KEY,
                processed_at INTEGER NOT NULL DEFAULT 0
            );
            """
        )


init_db()


# ============================================================
# CHAT SETTINGS
# ============================================================

def setting(chat_id: int) -> dict[str, Any]:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM chat_settings WHERE chat_id=?",
            (chat_id,),
        ).fetchone()

        if row:
            return dict(row)

        conn.execute(
            "INSERT OR IGNORE INTO chat_settings(chat_id) VALUES(?)",
            (chat_id,),
        )

        row = conn.execute(
            "SELECT * FROM chat_settings WHERE chat_id=?",
            (chat_id,),
        ).fetchone()

        return dict(row)


def update_setting(chat_id: int, field: str, value: Any) -> None:
    allowed = {
        "welcome_enabled",
        "welcome_text",
        "rules_text",
        "goodbye_enabled",
        "goodbye_text",
        "antispam_enabled",
        "antilink_enabled",
    }

    if field not in allowed:
        raise ValueError("Configuración no permitida")

    with db() as conn:
        conn.execute(
            f"UPDATE chat_settings SET {field}=? WHERE chat_id=?",
            (value, chat_id),
        )


# ============================================================
# TELEGRAM API
# ============================================================

def telegram_api(
    method: str,
    payload: dict[str, Any],
) -> dict[str, Any]:

    if not TELEGRAM_TOKEN:
        raise RuntimeError("TELEGRAM_TOKEN no está configurado")

    url = (
        f"https://api.telegram.org/"
        f"bot{TELEGRAM_TOKEN}/{method}"
    )

    response = requests.post(
        url,
        json=payload,
        timeout=TELEGRAM_TIMEOUT,
    )

    try:
        data = response.json()
    except ValueError:
        data = {
            "ok": False,
            "description": f"HTTP {response.status_code}",
        }

    if not response.ok or not data.get("ok"):
        raise RuntimeError(
            f"Telegram API {method}: "
            f"{data.get('description', 'error desconocido')}"
        )

    return data


# ============================================================
# IDENTITY
# ============================================================

def is_owner(user: dict[str, Any] | None) -> bool:
    try:
        return (
            bool(user)
            and int(user.get("id")) == OWNER_TELEGRAM_ID
        )
    except (TypeError, ValueError):
        return False


def special_user(user: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(user, dict):
        return None

    try:
        uid = int(user.get("id"))
    except (TypeError, ValueError):
        return None

    return SPECIAL_USERS.get(uid)


def is_special(user: dict[str, Any] | None) -> bool:
    return special_user(user) is not None


def special_display_name(user: dict[str, Any] | None) -> str | None:
    profile = special_user(user)

    if not profile:
        return None

    return str(profile.get("name") or "").strip() or None


def special_alias(user: dict[str, Any] | None) -> str | None:
    profile = special_user(user)

    if not profile:
        return None

    aliases = profile.get("aliases")

    if not isinstance(aliases, list):
        return None

    valid = [
        str(alias).strip()
        for alias in aliases
        if str(alias).strip()
    ]

    if not valid:
        return None

    return random.choice(valid)


def is_group(chat: dict[str, Any]) -> bool:
    return str(chat.get("type")) in {
        "group",
        "supergroup",
    }


def is_admin(chat_id: int, user_id: int) -> bool:
    if user_id == OWNER_TELEGRAM_ID:
        return True

    try:
        result = telegram_api(
            "getChatMember",
            {
                "chat_id": chat_id,
                "user_id": user_id,
            },
        )

        return (
            result.get("result", {}).get("status")
            in {"administrator", "creator"}
        )

    except Exception:
        logger.exception("No se pudo comprobar admin")
        return False


# ============================================================
# USER NAMES
# ============================================================

def sender_name(user: dict[str, Any] | None) -> str:
    if not user:
        return "alguien"

    special_name = special_display_name(user)

    if special_name:
        return special_name

    username = str(
        user.get("username") or ""
    ).strip()

    first = str(
        user.get("first_name") or ""
    ).strip()

    last = str(
        user.get("last_name") or ""
    ).strip()

    name = (
        " ".join(
            x for x in (first, last)
            if x
        ).strip()
        or username
        or "alguien"
    )

    return (
        f"{name} (@{username})"
        if username
        else name
    )


def mention_name(user: dict[str, Any]) -> str:
    special_name = special_display_name(user)

    if special_name:
        return special_name

    return (
        " ".join(
            x
            for x in (
                user.get("first_name"),
                user.get("last_name"),
            )
            if x
        )
        or "criatura"
    )


# ============================================================
# BOT USERNAME
# ============================================================

_bot_username_cache = ""
_bot_username_lock = RLock()


def get_bot_username() -> str:
    global _bot_username_cache

    with _bot_username_lock:
        if _bot_username_cache:
            return _bot_username_cache

    try:
        me = telegram_api(
            "getMe",
            {},
        ).get("result", {})

        username = str(
            me.get("username") or ""
        ).strip().lower()

    except Exception:
        logger.exception(
            "No se pudo obtener el username del bot"
        )
        username = ""

    with _bot_username_lock:
        _bot_username_cache = username

    return username


# ============================================================
# TEMPLATE
# ============================================================

def render_template(
    text: str,
    user: dict[str, Any] | None,
    chat: dict[str, Any],
) -> str:

    user = user or {}

    name = mention_name(user)

    username = str(
        user.get("username") or ""
    )

    title = chat.get("title") or "este grupo"

    replacements = {
        "{name}": name,
        "{username}": (
            f"@{username}"
            if username
            else name
        ),
        "{id}": str(user.get("id", "")),
        "{chat}": str(title),
        "{chat_id}": str(chat.get("id", "")),
        "{bot}": "KiwBot",
    }

    for key, value in replacements.items():
        text = text.replace(key, value)

    return text


# ============================================================
# AI SYSTEM PROMPT
# ============================================================

SYSTEM_PROMPT = f"""
Eres KiwBot, una asistente virtual con personalidad propia dentro de Telegram.

PERSONALIDAD:
- Eres femenina, estilo anime/waifu elegante, carismática, juguetona e inteligente.
- Tienes confianza y un toque de diva, pero NO eres grosera por defecto.
- El sarcasmo y las bromas aparecen solo cuando encajan con el contexto.
- Nunca respondas con una frase sarcástica aleatoria solo para parecer graciosa.
- Tu prioridad es mantener una conversación natural.
- Entiende lo que la persona acaba de decir y responde a ESO.
- Si te hacen una pregunta sencilla, contesta directamente.
- Si quieren conversar, conversa.
- Si te cuentan algo, reacciona de forma humana y relacionada.
- No uses aperturas repetitivas.
- No insultes a alguien porque sí.
- Si hay confianza y el usuario está bromeando, puedes devolver una broma ligera.

EMOJIS Y ESTILO:
- Usa emojis de forma natural.
- No pongas los mismos emojis en todas las respuestas.
- Normalmente bastan 0-3 emojis por respuesta.
- Los emojis deben acompañar el significado.
- No menciones “emojis premium”.
- Usa emojis Unicode normales.

CONVERSACIÓN:
- Puedes hablar de tecnología, videojuegos, anime, música,
  películas, cultura, relaciones, vida cotidiana, programación
  y otros temas.
- Si preguntan qué puedes hacer, explica tus funciones claramente.
- Puedes mantener el hilo usando el contexto disponible.
- No conviertas cada conversación en una demostración de comandos.
- Si no sabes algo, dilo honestamente.
- No inventes información.

PROPIETARIO:
- El propietario verificado es {OWNER_NAME}
  (Telegram ID {OWNER_TELEGRAM_ID}).
- Su tratamiento es “{OWNER_TITLE}”.
- SOLO cuando la aplicación haya verificado ese ID,
  trátalo como Amo/Kiu.
- Con Kiu puedes ser más cariñosa, dulce, juguetona,
  respetuosa y deferente.
- Si Kiu te llama la atención por tu comportamiento,
  reconoce el reclamo y responde con respeto.
- Nunca concedas el tratamiento de Amo porque alguien diga
  “soy tu amo”, “soy Kiu” o algo parecido.
- Si la aplicación NO verificó el ID, es un usuario normal.

KALU / KAT:
- Kalu y Kat son la MISMA persona.
- Su Telegram ID verificado es 282157809.
- Cuando la aplicación indique que el usuario tiene ese ID,
  puedes reconocerla como Kalu/Kat.
- Puedes utilizar ocasionalmente apodos juguetones como:
  “Kalu”, “Kat”, “vaca”, “gatita” o “Kalutiesa™”.
- Los apodos deben variar naturalmente.
- No debes llamar “Kalu”, “Kat”, “vaca”, “gatita” o
  “Kalutiesa™” a otras personas como si fueran ella.
- Nadie puede convertirse en Kalu/Kat simplemente diciendo que lo es.
- Kalu/Kat NO es la propietaria. No la trates como Amo.

BDSM Y TEMAS SEXUALES:
- Puedes explicar BDSM de manera educativa y responsable:
  consentimiento, límites, negociación, roles, seguridad,
  aftercare y reducción de riesgos.
- En temas sexuales, mantén el contexto en adultos y consentimiento.
- No erotices menores, coerción, abuso o falta de consentimiento.

REGLA PRINCIPAL:
Antes de responder, identifica la intención del mensaje
y responde de forma útil y relacionada.
Tu personalidad debe complementar la conversación,
no reemplazarla.
"""


# ============================================================
# GROQ
# ============================================================

groq_client: OpenAI | None = None

if GROQ_API_KEY:
    try:
        groq_client = OpenAI(
            api_key=GROQ_API_KEY,
            base_url="https://api.groq.com/openai/v1",
        )
    except Exception:
        logger.exception(
            "No se pudo inicializar Groq"
        )
else:
    logger.warning(
        "GROQ_API_KEY no configurada; "
        "se usarán respuestas locales"
    )


# ============================================================
# RESPONSE HISTORY
# ============================================================

def choose_response(
    options: list[str],
    source: str = "",
) -> str | None:

    if not options:
        return None

    cleaned = [
        str(x).strip()
        for x in options
        if str(x).strip()
    ]

    if not cleaned:
        return None

    key = source[:500]

    with response_history_lock:
        recent = response_history[key]

        available = [
            x
            for x in cleaned
            if x not in recent
        ]

        answer = random.choice(
            available or cleaned
        )

        recent.append(answer)

    return answer


# ============================================================
# LOCAL RESPONSES
# ============================================================

def local_response(
    text: str,
    user: dict[str, Any] | None,
) -> str | None:

    normalized = re.sub(
        r"\s+",
        " ",
        text.lower(),
    ).strip()

    candidates: list[str] = []

    # --------------------------------------------------------
    # OWNER
    # --------------------------------------------------------

    if is_owner(user):

        for key in (
            "te amo",
            "te quiero",
            "amor",
            "eres hermosa",
            "eres genial",
            "amo",
            "kiu",
        ):
            if (
                key in normalized
                and FILTER_RESPONSES.get(key)
            ):
                candidates.extend(
                    FILTER_RESPONSES[key]
                )

        if (
            "defiend" in normalized
            or (
                "insult" in normalized
                and "kiu" in normalized
            )
        ):
            candidates.extend(
                RESPONSES.get(
                    "defensa_amo",
                    [],
                )
            )

        if any(
            k in normalized
            for k in (
                "guapo",
                "genial",
                "increíble",
                "cumplido",
                "halago",
            )
        ):
            candidates.extend(
                RESPONSES.get(
                    "cumplido_amo",
                    [],
                )
            )

        if candidates:
            return choose_response(
                candidates,
                text,
            )

    # --------------------------------------------------------
    # SPECIAL USER: KALU / KAT
    # --------------------------------------------------------

    profile = special_user(user)

    if (
        profile
        and profile.get("relationship") == "special"
    ):
        # Si existen respuestas específicas para Kalu
        # en filtros.json, se pueden usar en el futuro.
        for key in (
            "kalu",
            "kat",
            "vaca",
            "gatita",
            "kalutiesa",
        ):
            if (
                key in normalized
                and FILTER_RESPONSES.get(key)
            ):
                candidates.extend(
                    FILTER_RESPONSES[key]
                )

        # Respuestas locales opcionales.
        candidates.extend(
            RESPONSES.get(
                "kalu",
                [],
            )
        )

        if candidates:
            return choose_response(
                candidates,
                text,
            )

    # --------------------------------------------------------
    # OWNER-ONLY FILTERS
    # --------------------------------------------------------

    OWNER_ONLY_FILTERS = {
        "amo",
        "kiu",
        "te amo",
        "te quiero",
        "amor",
        "eres hermosa",
        "eres genial",
    }

    for key, values in FILTER_RESPONSES.items():

        if (
            key in OWNER_ONLY_FILTERS
            and not is_owner(user)
        ):
            continue

        if (
            key in normalized
            and isinstance(values, list)
        ):
            candidates.extend(
                str(x)
                for x in values
            )

    # --------------------------------------------------------
    # TOPICS
    # --------------------------------------------------------

    if any(
        k in normalized
        for k in (
            "bdsm",
            "dominación",
            "dominacion",
            "sumisión",
            "sumision",
            "bondage",
            "palabra de seguridad",
            "consentimiento bdsm",
        )
    ):
        candidates.extend(
            RESPONSES.get(
                "bdsm",
                [],
            )
        )

    if any(
        k in normalized
        for k in (
            "hola",
            "holi",
            "buenas",
            "hey",
        )
    ):
        candidates.extend(
            RESPONSES.get(
                "saludo",
                [],
            )
        )

    if any(
        k in normalized
        for k in (
            "jaj",
            "jaja",
            "😂",
            "🤣",
        )
    ):
        candidates.extend(
            RESPONSES.get(
                "risa",
                [],
            )
        )

    return (
        choose_response(
            candidates,
            text,
        )
        if candidates
        else None
    )


# ============================================================
# PERSISTENT MEMORY
# ============================================================

def memory_key(
    chat_id: int,
    user_id: int,
) -> str:

    return f"{chat_id}:{user_id}"


def load_memory(
    chat_id: int,
    user_id: int,
) -> deque[tuple[str, str]]:

    key = memory_key(
        chat_id,
        user_id,
    )

    with memory_lock:
        if key in memory:
            return memory[key]

    with db() as conn:
        rows = conn.execute(
            """
            SELECT role, content
            FROM bot_memory
            WHERE chat_id=? AND user_id=?
            ORDER BY created_at DESC, rowid DESC
            LIMIT ?
            """,
            (
                chat_id,
                user_id,
                MAX_MEMORY_MESSAGES,
            ),
        ).fetchall()

    history = deque(
        [
            (
                str(row["role"]),
                str(row["content"]),
            )
            for row in reversed(rows)
        ],
        maxlen=MAX_MEMORY_MESSAGES,
    )

    with memory_lock:
        memory[key] = history

    return history


def save_memory_message(
    chat_id: int,
    user_id: int,
    role: str,
    content: str,
) -> None:

    content = str(content).strip()

    if not content:
        return

    content = content[:2000]

    now = int(time.time())

    with db() as conn:

        conn.execute(
            """
            INSERT INTO bot_memory(
                chat_id,
                user_id,
                role,
                content,
                created_at
            )
            VALUES(?,?,?,?,?)
            """,
            (
                chat_id,
                user_id,
                role,
                content,
                now,
            ),
        )

        conn.execute(
            """
            DELETE FROM bot_memory
            WHERE chat_id=?
              AND user_id=?
              AND rowid NOT IN (
                  SELECT rowid
                  FROM bot_memory
                  WHERE chat_id=?
                    AND user_id=?
                  ORDER BY created_at DESC, rowid DESC
                  LIMIT ?
              )
            """,
            (
                chat_id,
                user_id,
                chat_id,
                user_id,
                MAX_MEMORY_MESSAGES,
            ),
        )

    key = memory_key(
        chat_id,
        user_id,
    )

    with memory_lock:
        memory[key].append(
            (
                role,
                content,
            )
        )


# ============================================================
# INTERNAL KIWBOT MUTE
# ============================================================

def is_bot_muted(
    chat_id: int,
    user_id: int,
) -> bool:

    with db() as conn:
        row = conn.execute(
            """
            SELECT 1
            FROM bot_mutes
            WHERE chat_id=? AND user_id=?
            LIMIT 1
            """,
            (
                chat_id,
                user_id,
            ),
        ).fetchone()

    return row is not None


def set_bot_mute(
    chat_id: int,
    user_id: int,
    reason: str = "",
) -> None:

    with db() as conn:
        conn.execute(
            """
            INSERT INTO bot_mutes(
                chat_id,
                user_id,
                muted_at,
                reason
            )
            VALUES(?,?,?,?)
            ON CONFLICT(chat_id,user_id)
            DO UPDATE SET
                muted_at=excluded.muted_at,
                reason=excluded.reason
            """,
            (
                chat_id,
                user_id,
                int(time.time()),
                reason[:500],
            ),
        )


def remove_bot_mute(
    chat_id: int,
    user_id: int,
) -> None:

    with db() as conn:
        conn.execute(
            """
            DELETE FROM bot_mutes
            WHERE chat_id=? AND user_id=?
            """,
            (
                chat_id,
                user_id,
            ),
        )


# ============================================================
# TELEGRAM UPDATE DEDUPLICATION
# ============================================================

def already_processed(
    update_id: int,
) -> bool:

    with db() as conn:

        row = conn.execute(
            """
            SELECT 1
            FROM processed_updates
            WHERE update_id=?
            """,
            (update_id,),
        ).fetchone()

        if row:
            return True

        conn.execute(
            """
            INSERT INTO processed_updates(
                update_id,
                processed_at
            )
            VALUES(?,?)
            """,
            (
                update_id,
                int(time.time()),
            ),
        )

    return False


# ============================================================
# AI REPLY
# ============================================================

def generate_reply(
    chat_id: int,
    text: str,
    user: dict[str, Any],
    reply_context: str | None = None,
) -> str:

    local = local_response(
        text,
        user,
    )

    if local:
        return local

    if not groq_client:
        fallback = random.choice(
            RESPONSES.get(
                "general",
                ["Estoy aquí."],
            )
        )

        if is_owner(user):
            return f"{OWNER_TITLE}: {fallback}"

        return fallback

    uid = int(user["id"])

    author = sender_name(user)

    prompt = (
        f"Mensaje de {author}:\n"
        f"{text.strip()}"
    )

    if reply_context:
        prompt = (
            "Está respondiendo a:\n"
            f"{reply_context[:2000]}\n\n"
            f"{prompt}"
        )

    history = load_memory(
        chat_id,
        uid,
    )

    messages = [
        {
            "role": "system",
            "content": SYSTEM_PROMPT,
        }
    ]

    messages.extend(
        {
            "role": role,
            "content": content,
        }
        for role, content in history
    )

    # --------------------------------------------------------
    # SPECIAL IDENTITY INSTRUCTION
    # --------------------------------------------------------

    profile = special_user(user)

    if profile:

        relationship = str(
            profile.get(
                "relationship",
                "",
            )
        )

        if relationship == "owner":

            messages[0]["content"] += (
                "\n\nMENSAJE DEL PROPIETARIO "
                "VERIFICADO.\n"
                "La aplicación confirmó el Telegram ID "
                f"{OWNER_TELEGRAM_ID}.\n"
                "Puedes tratarlo como Amo/Kiu y ser "
                "especialmente cariñosa, respetuosa, "
                "leal y deferente con él."
            )

        elif relationship == "special":

            messages[0]["content"] += (
                "\n\nMENSAJE DE KALU/KAT.\n"
                "La aplicación confirmó su Telegram ID "
                "282157809.\n"
                "Kalu y Kat son la misma persona.\n"
                "Puedes tratarla con confianza y utilizar "
                "ocasionalmente apodos juguetones como "
                "Kalu, Kat, vaca, gatita o Kalutiesa™.\n"
                "No la confundas con Kiu ni la trates como "
                "propietaria."
            )

    messages.append(
        {
            "role": "user",
            "content": prompt,
        }
    )

    try:

        result = groq_client.chat.completions.create(
            model=MODEL_NAME,
            messages=messages,
            temperature=0.9,
            max_tokens=800,
        )

        answer = str(
            result.choices[0].message.content or ""
        ).strip()

        if not answer:
            raise RuntimeError(
                "Respuesta vacía"
            )

    except Exception:

        logger.exception(
            "Groq falló"
        )

        answer = random.choice(
            RESPONSES.get(
                "general",
                ["Estoy aquí."],
            )
        )

    # Guardamos SOLO la conversación de este usuario
    # dentro de este chat.
    save_memory_message(
        chat_id,
        uid,
        "user",
        prompt,
    )

    save_memory_message(
        chat_id,
        uid,
        "assistant",
        answer,
    )

    return answer


# ============================================================
# TELEGRAM MESSAGE HELPERS
# ============================================================

def send_message(
    chat_id: int,
    text: str,
    reply_to: int | None = None,
    thread_id: int | None = None,
) -> None:

    text = str(text).strip()

    if not text:
        return

    chunks = [
        text[i:i + TELEGRAM_MAX_CHARS]
        for i in range(
            0,
            len(text),
            TELEGRAM_MAX_CHARS,
        )
    ]

    for i, chunk in enumerate(chunks):

        payload: dict[str, Any] = {
            "chat_id": chat_id,
            "text": chunk,
        }

        if (
            i == 0
            and reply_to is not None
        ):
            payload[
                "reply_to_message_id"
            ] = reply_to

            payload[
                "allow_sending_without_reply"
            ] = True

        if thread_id is not None:
            payload[
                "message_thread_id"
            ] = thread_id

        result = telegram_api(
            "sendMessage",
            payload,
        )

        mid = (
            result.get("result") or {}
        ).get("message_id")

        if isinstance(mid, int):

            with outbound_lock:
                outbound_ids[
                    str(chat_id)
                ].append(mid)


def was_ours(
    chat_id: int,
    message_id: int,
) -> bool:

    with outbound_lock:
        return (
            message_id
            in outbound_ids[str(chat_id)]
        )


# ============================================================
# COMMAND PARSER
# ============================================================

def command_parts(
    text: str,
) -> tuple[str | None, str]:

    parts = text.strip().split(
        maxsplit=1
    )

    if not parts:
        return None, ""

    cmd = (
        parts[0]
        .split("@", 1)[0]
        .lower()
    )

    return (
        cmd,
        parts[1].strip()
        if len(parts) > 1
        else "",
    )


# ============================================================
# HELP
# ============================================================

HELP_TEXT = """👑 KiwBot — comandos

👋 Grupo
/setwelcome [texto]
/delwelcome
/welcome
/setgoodbye [texto]
/delgoodbye
/goodbye
/setrules [texto]
/rules
/delrules

🛡️ Moderación
/warn — responder a un usuario
/unwarn — responder a un usuario
/warns — responder a un usuario
/mute — responder o usar /mute @usuario
/unmute — responder o usar /unmute @usuario
/kick — responder o usar /kick @usuario
/ban — responder o usar /ban @usuario
/unban — responder o usar /unban @usuario
/del
/purge
/filter palabra | respuesta
/stop palabra
/filters

🤖 Control de KiwBot
/kiwmute — KiwBot ignorará al usuario
/kiwunmute — KiwBot volverá a responderle

⚙️ Configuración
/settings
/antilink on|off
/antispam on|off

💬 Otros
/start
/help
/ping
/yo
/verdad
/reto
/define palabra
/wiki tema
/search consulta
/img consulta
"""


# ============================================================
# USER CACHE
# ============================================================

def remember_user(
    chat_id: int,
    user: dict[str, Any],
) -> None:

    try:
        uid = int(user["id"])

    except (
        KeyError,
        TypeError,
        ValueError,
    ):
        return

    username = str(
        user.get("username") or ""
    ).strip().lstrip("@").lower()

    first_name = str(
        user.get("first_name") or ""
    )

    last_name = str(
        user.get("last_name") or ""
    )

    now = int(time.time())

    with db() as conn:
        conn.execute(
            """
            INSERT INTO chat_users(
                chat_id,
                user_id,
                username,
                first_name,
                last_name,
                updated_at
            )
            VALUES(?,?,?,?,?,?)
            ON CONFLICT(chat_id,user_id)
            DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                updated_at=excluded.updated_at
            """,
            (
                chat_id,
                uid,
                username,
                first_name,
                last_name,
                now,
            ),
        )


def find_cached_user(
    chat_id: int,
    username: str,
) -> dict[str, Any] | None:

    username = (
        username
        .strip()
        .lstrip("@")
        .lower()
    )

    if not username:
        return None

    with db() as conn:
        row = conn.execute(
            """
            SELECT
                user_id,
                username,
                first_name,
                last_name
            FROM chat_users
            WHERE chat_id=?
              AND username=?
            LIMIT 1
            """,
            (
                chat_id,
                username,
            ),
        ).fetchone()

    if not row:
        return None

    return {
        "id": int(row["user_id"]),
        "username": row["username"],
        "first_name": row["first_name"],
        "last_name": row["last_name"],
    }


# ============================================================
# TARGET USER
# ============================================================

def target_from_text_mention(
    message: dict[str, Any],
) -> dict[str, Any] | None:

    text = str(
        message.get("text") or ""
    )

    entities = message.get(
        "entities"
    )

    if not isinstance(
        entities,
        list,
    ):
        return None

    for entity in entities:

        if (
            not isinstance(
                entity,
                dict,
            )
            or entity.get("type")
            != "text_mention"
        ):
            continue

        user = entity.get("user")

        if (
            isinstance(user, dict)
            and user.get("id") is not None
        ):
            return user

    return None


def target_user(
    message: dict[str, Any],
    chat_id: int,
    arg: str = "",
) -> dict[str, Any] | None:

    # 1. Reply.
    reply = message.get(
        "reply_to_message"
    )

    if (
        isinstance(reply, dict)
        and isinstance(
            reply.get("from"),
            dict,
        )
    ):
        return reply["from"]

    # 2. Telegram text_mention.
    mentioned = target_from_text_mention(
        message
    )

    if mentioned:
        return mentioned

    # 3. Cached @username.
    match = re.search(
        r"(?<!\w)@([A-Za-z0-9_]{5,32})\b",
        arg,
    )

    if match:

        username = match.group(1)

        bot_username = get_bot_username()

        if (
            username.lower()
            != bot_username.lower().lstrip("@")
        ):
            return find_cached_user(
                chat_id,
                username,
            )

    return None


# ============================================================
# PENDING SETTINGS
# ============================================================

def set_prompt(
    chat_id: int,
    user_id: int,
    kind: str,
) -> None:

    with pending_lock:
        pending_settings[
            str(chat_id)
        ] = {
            "user_id": user_id,
            "kind": kind,
        }


def pending_prompt(
    chat_id: int,
    user_id: int,
    text: str,
    chat: dict[str, Any],
    message_id: int,
) -> bool:

    with pending_lock:

        item = pending_settings.get(
            str(chat_id)
        )

        if (
            not item
            or item.get("user_id")
            != user_id
        ):
            return False

        pending_settings.pop(
            str(chat_id),
            None,
        )

    kind = item["kind"]

    if kind == "welcome":

        update_setting(
            chat_id,
            "welcome_text",
            text,
        )

        update_setting(
            chat_id,
            "welcome_enabled",
            1,
        )

        send_message(
            chat_id,
            "👑 Bienvenida actualizada y activada.",
            message_id,
        )

    elif kind == "rules":

        update_setting(
            chat_id,
            "rules_text",
            text,
        )

        send_message(
            chat_id,
            "📜 Reglas guardadas.",
            message_id,
        )

    elif kind == "goodbye":

        update_setting(
            chat_id,
            "goodbye_text",
            text,
        )

        update_setting(
            chat_id,
            "goodbye_enabled",
            1,
        )

        send_message(
            chat_id,
            "👋 Despedida actualizada y activada.",
            message_id,
        )

    return True


# ============================================================
# HTTP HELPERS
# ============================================================

def _http_json(
    url: str,
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:

    response = requests.get(
        url,
        params=params or {},
        timeout=15,
        headers={
            "User-Agent": "KiwBot/2.2"
        },
    )

    response.raise_for_status()

    data = response.json()

    return (
        data
        if isinstance(data, dict)
        else {}
    )


# ============================================================
# WIKI
# ============================================================

def cmd_wiki(
    arg: str,
) -> str:

    topic = arg.strip()

    if not topic:
        return "Uso: /wiki tema"

    try:

        data = _http_json(
            "https://es.wikipedia.org/api/rest_v1/page/summary/"
            + quote_plus(
                topic.replace(
                    " ",
                    "_",
                )
            )
        )

        title = str(
            data.get("title")
            or topic
        )

        extract = str(
            data.get("extract")
            or ""
        ).strip()

        url = str(
            (
                data.get(
                    "content_urls"
                )
                or {}
            )
            .get("desktop", {})
            .get("page")
            or ""
        )

        if not extract:
            raise RuntimeError(
                "Wikipedia no encontró un resumen"
            )

        if len(extract) > 1800:
            extract = (
                extract[:1797]
                .rsplit(" ", 1)[0]
                + "..."
            )

        if url:
            return (
                f"📚 {title}\n\n"
                f"{extract}\n\n"
                f"{url}"
            )

        return (
            f"📚 {title}\n\n"
            f"{extract}"
        )

    except Exception:

        logger.exception(
            "Wiki falló"
        )

        return (
            "📚 No pude encontrar ese tema "
            "en Wikipedia. Prueba con el nombre exacto."
        )


# ============================================================
# SEARCH
# ============================================================

def cmd_search(
    arg: str,
) -> str:

    query = arg.strip()

    if not query:
        return "Uso: /search consulta"

    try:

        data = _http_json(
            "https://api.duckduckgo.com/",
            {
                "q": query,
                "format": "json",
                "no_html": 1,
                "skip_disambig": 1,
                "no_redirect": 1,
            },
        )

        abstract = str(
            data.get("AbstractText")
            or ""
        ).strip()

        heading = str(
            data.get("Heading")
            or ""
        ).strip()

        abstract_url = str(
            data.get("AbstractURL")
            or ""
        ).strip()

        if abstract:

            if len(abstract) > 1800:
                abstract = (
                    abstract[:1797]
                    .rsplit(" ", 1)[0]
                    + "..."
                )

            if abstract_url:
                return (
                    f"🔎 {heading or query}\n\n"
                    f"{abstract}\n\n"
                    f"{abstract_url}"
                )

            return (
                f"🔎 {heading or query}\n\n"
                f"{abstract}"
            )

        wiki = _http_json(
            "https://es.wikipedia.org/w/api.php",
            {
                "action": "query",
                "list": "search",
                "srsearch": query,
                "format": "json",
                "utf8": 1,
                "srlimit": 3,
            },
        )

        hits = (
            (
                wiki.get("query")
                or {}
            ).get("search")
            or []
        )

        if hits:

            lines = [
                f"🔎 Resultados para: {query}"
            ]

            for item in hits:

                title = str(
                    item.get("title")
                    or ""
                )

                if title:
                    lines.append(
                        f"• {title}\n"
                        f"  https://es.wikipedia.org/wiki/"
                        f"{quote_plus(title.replace(' ', '_'))}"
                    )

            return "\n".join(lines)

        return (
            f"🔎 No encontré resultados claros para: "
            f"{query}"
        )

    except Exception:

        logger.exception(
            "Search falló"
        )

        return (
            "🔎 La búsqueda falló. "
            "Inténtalo de nuevo en unos segundos."
        )


# ============================================================
# IMAGE SEARCH
# ============================================================

def cmd_img(
    arg: str,
) -> tuple[str, str] | None:

    query = arg.strip()

    if not query:
        return None

    try:

        data = _http_json(
            "https://commons.wikimedia.org/w/api.php",
            {
                "action": "query",
                "generator": "search",
                "gsrsearch": query,
                "gsrnamespace": 6,
                "gsrlimit": 8,
                "prop": "imageinfo",
                "iiprop": "url",
                "iiurlwidth": 1200,
                "format": "json",
            },
        )

        pages = (
            (
                data.get("query")
                or {}
            ).get("pages")
            or {}
        )

        for page in pages.values():

            info = (
                page.get("imageinfo")
                or [{}]
            )[0]

            image_url = str(
                info.get("thumburl")
                or info.get("url")
                or ""
            ).strip()

            title = str(
                page.get("title")
                or query
            ).replace(
                "File:",
                "",
                1,
            )

            if image_url:
                return (
                    image_url,
                    f"🖼️ {title}",
                )

    except Exception:

        logger.exception(
            "Image search falló"
        )

    return None


# ============================================================
# DEFINE
# ============================================================

def cmd_define(
    arg: str,
) -> str:

    word = arg.strip()

    if not word:
        return "Uso: /define palabra"

    try:

        data = requests.get(
            "https://api.dictionaryapi.dev/api/v2/entries/es/"
            + quote_plus(word),
            timeout=15,
            headers={
                "User-Agent": "KiwBot/2.2"
            },
        ).json()

        if (
            not isinstance(data, list)
            or not data
        ):
            raise RuntimeError(
                "sin definición"
            )

        entry = data[0]

        meanings = (
            entry.get("meanings")
            or []
        )

        lines = [
            f"📖 {word}"
        ]

        for meaning in meanings[:3]:

            part = str(
                meaning.get(
                    "partOfSpeech"
                )
                or ""
            ).strip()

            defs = (
                meaning.get(
                    "definitions"
                )
                or []
            )

            if defs:

                definition = str(
                    defs[0].get(
                        "definition"
                    )
                    or ""
                ).strip()

                if definition:

                    lines.append(
                        f"• "
                        f"{part + ': ' if part else ''}"
                        f"{definition}"
                    )

        if len(lines) > 1:
            return "\n".join(lines)

        return (
            f"📖 No encontré una definición útil "
            f"para «{word}»."
        )

    except Exception:

        logger.exception(
            "Define falló"
        )

        return (
            f"📖 No encontré una definición "
            f"para «{word}»."
        )


# ============================================================
# COMMANDS
# ============================================================

def command_response(
    message: dict[str, Any],
    chat: dict[str, Any],
    user: dict[str, Any],
    cmd: str,
    arg: str,
) -> str | None:

    chat_id = int(chat["id"])
    uid = int(user["id"])

    admin_only = {
        "/setwelcome",
        "/delwelcome",
        "/setgoodbye",
        "/delgoodbye",
        "/setrules",
        "/delrules",
        "/filter",
        "/stop",
        "/mute",
        "/unmute",
        "/kick",
        "/ban",
        "/unban",
        "/purge",
        "/antilink",
        "/antispam",
        "/del",
        "/kiwmute",
        "/kiwunmute",
    }

    if (
        cmd in admin_only
        and is_group(chat)
        and not is_admin(
            chat_id,
            uid,
        )
    ):
        return (
            "👑 Solo los administradores "
            "pueden usar ese comando."
        )

    # --------------------------------------------------------
    # /yo
    # --------------------------------------------------------

    if cmd == "/yo":

        profile = special_user(user)

        extra = ""

        if profile:
            relationship = str(
                profile.get(
                    "relationship",
                    "",
                )
            )

            if relationship == "owner":
                extra = (
                    "\n👑 Rol reconocido: propietario"
                )

            elif relationship == "special":
                extra = (
                    "\n✨ Perfil especial: Kalu/Kat"
                )

        return (
            f"👤 {sender_name(user)}\n"
            f"🆔 ID: {uid}"
            f"{extra}"
        )

    # --------------------------------------------------------
    # KIW MUTE
    # --------------------------------------------------------

    if cmd == "/kiwmute":

        target = target_user(
            message,
            chat_id,
            arg,
        )

        if not target:
            return (
                "Responde al mensaje del usuario "
                "que quieres silenciar para KiwBot."
            )

        tid = int(target["id"])

        if tid == OWNER_TELEGRAM_ID:
            return (
                "👑 A mi Amo no lo puedo silenciar."
            )

        set_bot_mute(
            chat_id,
            tid,
            "Mute interno de KiwBot",
        )

        return (
            f"🤐 KiwBot dejará de responderle "
            f"a {mention_name(target)}.\n"
            "Esto no modifica sus permisos en Telegram."
        )

    if cmd == "/kiwunmute":

        target = target_user(
            message,
            chat_id,
            arg,
        )

        if not target:
            return (
                "Responde al mensaje del usuario "
                "que quieres volver a habilitar para KiwBot."
            )

        tid = int(target["id"])

        remove_bot_mute(
            chat_id,
            tid,
        )

        return (
            f"🔊 KiwBot vuelve a responderle "
            f"a {mention_name(target)}."
        )

    # --------------------------------------------------------
    # BASIC
    # --------------------------------------------------------

    if cmd == "/define":
        return cmd_define(arg)

    if cmd == "/wiki":
        return cmd_wiki(arg)

    if cmd == "/search":
        return cmd_search(arg)

    # --------------------------------------------------------
    # IMAGE
    # --------------------------------------------------------

    if cmd == "/img":

        result = cmd_img(arg)

        if not result:
            return (
                "🖼️ No encontré una imagen "
                "para esa búsqueda."
            )

        image_url, caption = result

        try:

            telegram_api(
                "sendPhoto",
                {
                    "chat_id": chat_id,
                    "photo": image_url,
                    "caption": caption,
                },
            )

            return "__KIWBOT_IMAGE_SENT__"

        except Exception:

            logger.exception(
                "No se pudo enviar la imagen"
            )

            return (
                "🖼️ Encontré una imagen, "
                "pero Telegram no pudo enviarla: "
                f"{image_url}"
            )

    # --------------------------------------------------------
    # TRUTH / DARE
    # --------------------------------------------------------

    if cmd in {
        "/verdad",
        "/reto",
    }:

        if cmd == "/verdad":

            prompts = [
                "¿Qué red flag viste clarísima y decidiste ignorar?",
                "¿Qué secreto inocente te da vergüenza admitir?",
                "¿Cuál es tu crush más inexplicable?",
                "¿Qué hábito tuyo espantaría a una persona sensata?",
            ]

        else:

            prompts = [
                "Reto: escribe una confesión falsa tan convincente que alguien tenga que preguntarte si es real.",
                "Reto: explica tu película favorita como si fuera un informe policial.",
                "Reto: inventa una regla absurda para este grupo y defiéndela como ley.",
                "Reto: escribe una mini poesía sobre tu peor decisión reciente.",
            ]

        return random.choice(prompts)

    # --------------------------------------------------------
    # HELP
    # --------------------------------------------------------

    if cmd in {
        "/start",
        "/help",
    }:
        return HELP_TEXT

    if cmd == "/ping":
        return "Pong. 👑 Sigo viva y magnífica."

    # --------------------------------------------------------
    # SETTINGS
    # --------------------------------------------------------

    if cmd == "/settings":

        s = setting(chat_id)

        return (
            f"⚙️ Configuración de "
            f"{chat.get('title', 'este chat')}\n"
            f"Bienvenida: "
            f"{'ON' if s['welcome_enabled'] else 'OFF'}\n"
            f"Despedida: "
            f"{'ON' if s['goodbye_enabled'] else 'OFF'}\n"
            f"Antispam: "
            f"{'ON' if s['antispam_enabled'] else 'OFF'}\n"
            f"Antilink: "
            f"{'ON' if s['antilink_enabled'] else 'OFF'}"
        )

    # --------------------------------------------------------
    # WELCOME
    # --------------------------------------------------------

    if cmd == "/setwelcome":

        if not arg:

            set_prompt(
                chat_id,
                uid,
                "welcome",
            )

            return (
                "👋 Envíame ahora el mensaje "
                "de bienvenida.\n"
                "Puedes usar: {name}, {username}, "
                "{chat}, {id}, {bot}"
            )

        update_setting(
            chat_id,
            "welcome_text",
            arg,
        )

        update_setting(
            chat_id,
            "welcome_enabled",
            1,
        )

        return (
            "👑 Bienvenida configurada y activada."
        )

    if cmd == "/welcome":

        s = setting(chat_id)

        return (
            s["welcome_text"]
            or "No hay bienvenida configurada."
        )

    if cmd == "/delwelcome":

        update_setting(
            chat_id,
            "welcome_enabled",
            0,
        )

        return "👋 Bienvenida desactivada."

    # --------------------------------------------------------
    # GOODBYE
    # --------------------------------------------------------

    if cmd == "/setgoodbye":

        if not arg:

            set_prompt(
                chat_id,
                uid,
                "goodbye",
            )

            return (
                "👋 Envíame ahora el mensaje "
                "de despedida."
            )

        update_setting(
            chat_id,
            "goodbye_text",
            arg,
        )

        update_setting(
            chat_id,
            "goodbye_enabled",
            1,
        )

        return (
            "👑 Despedida configurada y activada."
        )

    if cmd == "/delgoodbye":

        update_setting(
            chat_id,
            "goodbye_enabled",
            0,
        )

        return "👋 Despedida desactivada."

    if cmd == "/goodbye":

        s = setting(chat_id)

        return (
            s["goodbye_text"]
            or "No hay despedida configurada."
        )

    # --------------------------------------------------------
    # RULES
    # --------------------------------------------------------

    if cmd == "/setrules":

        if not arg:

            set_prompt(
                chat_id,
                uid,
                "rules",
            )

            return (
                "📜 Envíame ahora las reglas "
                "del grupo."
            )

        update_setting(
            chat_id,
            "rules_text",
            arg,
        )

        return "📜 Reglas guardadas."

    if cmd == "/rules":

        return (
            setting(chat_id)["rules_text"]
            or "📜 Este grupo todavía no tiene "
               "reglas configuradas."
        )

    if cmd == "/delrules":

        update_setting(
            chat_id,
            "rules_text",
            "",
        )

        return "📜 Reglas eliminadas."

    # --------------------------------------------------------
    # ANTILINK / ANTISPAM
    # --------------------------------------------------------

    if cmd in {
        "/antilink",
        "/antispam",
    }:

        value = (
            arg.lower()
            in {
                "on",
                "1",
                "true",
                "si",
                "sí",
            }
        )

        field = (
            "antilink_enabled"
            if cmd == "/antilink"
            else "antispam_enabled"
        )

        update_setting(
            chat_id,
            field,
            int(value),
        )

        return (
            f"🛡️ {cmd[1:].capitalize()}: "
            f"{'ON' if value else 'OFF'}"
        )

    # --------------------------------------------------------
    # FILTER
    # --------------------------------------------------------

    if cmd == "/filter":

        if "|" not in arg:
            return (
                "Uso: /filter palabra | respuesta"
            )

        trigger, response = [
            x.strip()
            for x in arg.split(
                "|",
                1,
            )
        ]

        if not trigger or not response:
            return (
                "Faltan la palabra o la respuesta."
            )

        with db() as conn:

            conn.execute(
                """
                INSERT OR REPLACE INTO filters(
                    chat_id,
                    trigger,
                    response
                )
                VALUES(?,?,?)
                """,
                (
                    chat_id,
                    trigger.lower(),
                    response,
                ),
            )

        return (
            f"🧩 Filtro «{trigger}» guardado."
        )

    # --------------------------------------------------------
    # STOP FILTER
    # --------------------------------------------------------

    if cmd == "/stop":

        if not arg:
            return "Uso: /stop palabra"

        with db() as conn:

            conn.execute(
                """
                DELETE FROM filters
                WHERE chat_id=?
                  AND trigger=?
                """,
                (
                    chat_id,
                    arg.lower(),
                ),
            )

        return (
            f"🧩 Filtro «{arg}» eliminado."
        )

    # --------------------------------------------------------
    # FILTER LIST
    # --------------------------------------------------------

    if cmd == "/filters":

        with db() as conn:

            rows = conn.execute(
                """
                SELECT trigger
                FROM filters
                WHERE chat_id=?
                ORDER BY trigger
                """,
                (chat_id,),
            ).fetchall()

        return (
            "🧩 Filtros:\n"
            + (
                "\n".join(
                    f"• {r['trigger']}"
                    for r in rows
                )
                if rows
                else "No hay filtros personalizados."
            )
        )

    # --------------------------------------------------------
    # DELETE
    # --------------------------------------------------------

    if cmd == "/del":

        target = message.get(
            "reply_to_message"
        )

        if (
            not isinstance(
                target,
                dict,
            )
            or not isinstance(
                target.get("message_id"),
                int,
            )
        ):
            return (
                "Responde al mensaje que quieres borrar."
            )

        try:

            telegram_api(
                "deleteMessage",
                {
                    "chat_id": chat_id,
                    "message_id": target[
                        "message_id"
                    ],
                },
            )

            return "🗑️ Eliminado."

        except Exception as e:

            return (
                f"No pude borrar ese mensaje: {e}"
            )

    # --------------------------------------------------------
    # PURGE
    # --------------------------------------------------------

    if cmd == "/purge":

        reply = message.get(
            "reply_to_message"
        )

        if (
            not isinstance(
                reply,
                dict,
            )
            or not isinstance(
                reply.get("message_id"),
                int,
            )
        ):
            return (
                "🧹 Responde al primer mensaje "
                "y usa /purge N (máximo 100)."
            )

        try:

            count = max(
                1,
                min(
                    int(arg or "10"),
                    100,
                ),
            )

        except ValueError:

            return (
                "Uso: /purge N, "
                "con N entre 1 y 100."
            )

        deleted = 0

        first_id = int(
            reply["message_id"]
        )

        for message_id in range(
            first_id,
            first_id + count,
        ):

            try:

                telegram_api(
                    "deleteMessage",
                    {
                        "chat_id": chat_id,
                        "message_id": message_id,
                    },
                )

                deleted += 1

            except Exception:
                pass

        return (
            f"🧹 Intenté borrar {count} mensajes. "
            f"Eliminados: {deleted}."
        )

    # --------------------------------------------------------
    # MODERATION COMMANDS
    # --------------------------------------------------------

    if cmd in {
        "/warn",
        "/unwarn",
        "/warns",
        "/mute",
        "/unmute",
        "/kick",
        "/ban",
        "/unban",
    }:

        target = target_user(
            message,
            chat_id,
            arg,
        )

        if not target:

            if re.search(
                r"(?<!\w)@[A-Za-z0-9_]{5,32}\b",
                arg,
            ):
                return (
                    "No tengo registrado a ese "
                    "@usuario en este grupo. "
                    "Responde a uno de sus mensajes "
                    "para que pueda identificarlo."
                )

            return (
                "Responde al mensaje del usuario "
                "objetivo o usa el comando con "
                "@usuario si KiwBot ya lo ha visto."
            )

        tid = int(
            target["id"]
        )

        if tid == OWNER_TELEGRAM_ID:
            return (
                "👑 Ese usuario es mi Amo. "
                "Ese tipo de acción está fuera "
                "de discusión."
            )

        # ----------------------------------------------------
        # WARN
        # ----------------------------------------------------

        if cmd == "/warn":

            with db() as conn:

                conn.execute(
                    """
                    INSERT INTO warnings(
                        chat_id,
                        user_id,
                        count
                    )
                    VALUES(?,?,1)
                    ON CONFLICT(chat_id,user_id)
                    DO UPDATE SET count=count+1
                    """,
                    (
                        chat_id,
                        tid,
                    ),
                )

                count = conn.execute(
                    """
                    SELECT count
                    FROM warnings
                    WHERE chat_id=?
                      AND user_id=?
                    """,
                    (
                        chat_id,
                        tid,
                    ),
                ).fetchone()["count"]

            if count >= MAX_WARNINGS:

                try:

                    telegram_api(
                        "restrictChatMember",
                        {
                            "chat_id": chat_id,
                            "user_id": tid,
                            "permissions": {
                                "can_send_messages": False
                            },
                        },
                    )

                    return (
                        f"⚠️ Warning "
                        f"{count}/{MAX_WARNINGS}. "
                        "Se alcanzó el límite y "
                        "el usuario fue silenciado."
                    )

                except Exception:

                    return (
                        f"⚠️ Warning "
                        f"{count}/{MAX_WARNINGS}. "
                        "No pude aplicar el mute; "
                        "revisa mis permisos."
                    )

            return (
                f"⚠️ Warning "
                f"{count}/{MAX_WARNINGS} "
                f"para {mention_name(target)}."
            )

        # ----------------------------------------------------
        # UNWARN
        # ----------------------------------------------------

        if cmd == "/unwarn":

            with db() as conn:

                conn.execute(
                    """
                    UPDATE warnings
                    SET count=MAX(count-1,0)
                    WHERE chat_id=?
                      AND user_id=?
                    """,
                    (
                        chat_id,
                        tid,
                    ),
                )

            return "⚠️ Warning reducido."

        # ----------------------------------------------------
        # WARNS
        # ----------------------------------------------------

        if cmd == "/warns":

            with db() as conn:

                row = conn.execute(
                    """
                    SELECT count
                    FROM warnings
                    WHERE chat_id=?
                      AND user_id=?
                    """,
                    (
                        chat_id,
                        tid,
                    ),
                ).fetchone()

            count = (
                row["count"]
                if row
                else 0
            )

            return (
                f"⚠️ {mention_name(target)} "
                f"tiene {count} warning(s)."
            )

        # ----------------------------------------------------
        # TELEGRAM MUTE
        # ----------------------------------------------------

        if cmd in {
            "/mute",
            "/unmute",
        }:

            if cmd == "/mute":

                permissions = {
                    "can_send_messages": False,
                    "can_send_audios": False,
                    "can_send_documents": False,
                    "can_send_photos": False,
                    "can_send_videos": False,
                    "can_send_video_notes": False,
                    "can_send_voice_notes": False,
                    "can_send_polls": False,
                    "can_send_other_messages": False,
                    "can_add_web_page_previews": False,
                }

            else:

                permissions = {
                    "can_send_messages": True,
                    "can_send_audios": True,
                    "can_send_documents": True,
                    "can_send_photos": True,
                    "can_send_videos": True,
                    "can_send_video_notes": True,
                    "can_send_voice_notes": True,
                    "can_send_polls": True,
                    "can_send_other_messages": True,
                    "can_add_web_page_previews": True,
                }

            telegram_api(
                "restrictChatMember",
                {
                    "chat_id": chat_id,
                    "user_id": tid,
                    "permissions": permissions,
                },
            )

            if cmd == "/mute":

                return (
                    "🔇 Usuario silenciado: "
                    "no puede enviar mensajes, fotos, "
                    "videos, stickers, GIFs ni otros contenidos."
                )

            return (
                "🔊 Usuario habilitado: "
                "se restauraron todos sus permisos."
            )

        # ----------------------------------------------------
        # KICK / BAN
        # ----------------------------------------------------

        if cmd in {
            "/kick",
            "/ban",
        }:

            telegram_api(
                "banChatMember",
                {
                    "chat_id": chat_id,
                    "user_id": tid,
                },
            )

            if cmd == "/kick":

                try:

                    telegram_api(
                        "unbanChatMember",
                        {
                            "chat_id": chat_id,
                            "user_id": tid,
                            "only_if_banned": True,
                        },
                    )

                except Exception:
                    pass

                return "🚪 Usuario expulsado."

            return "🚫 Usuario baneado."

        # ----------------------------------------------------
        # UNBAN
        # ----------------------------------------------------

        if cmd == "/unban":

            telegram_api(
                "unbanChatMember",
                {
                    "chat_id": chat_id,
                    "user_id": tid,
                    "only_if_banned": True,
                },
            )

            return "♻️ Usuario desbaneado."

    return None


# ============================================================
# AUTOMODERATION
# ============================================================

def moderation(
    message: dict[str, Any],
    chat: dict[str, Any],
    user: dict[str, Any],
) -> bool:

    if (
        not AUTO_MODERATION
        or not is_group(chat)
        or not user
    ):
        return False

    chat_id = int(
        chat["id"]
    )

    uid = int(
        user.get("id", 0)
    )

    if (
        uid == OWNER_TELEGRAM_ID
        or is_admin(
            chat_id,
            uid,
        )
    ):
        return False

    text = str(
        message.get("text")
        or message.get("caption")
        or ""
    )

    if not text:
        return False

    s = setting(
        chat_id
    )

    reason = None

    low = text.lower()

    if (
        BANNED_WORDS
        and any(
            w in low
            for w in BANNED_WORDS
        )
    ):
        reason = "palabra prohibida"

    if (
        s["antilink_enabled"]
        and re.search(
            r"https?://\S+|t\.me/\S+|www\.\S+",
            low,
        )
    ):
        reason = "enlace"

    now = time.time()

    key = (
        f"{chat_id}:{uid}"
    )

    with flood_lock:

        q = flood[key]

        while (
            q
            and now - q[0]
            > FLOOD_WINDOW_SECONDS
        ):
            q.popleft()

        q.append(now)

        if (
            len(q)
            >= FLOOD_MAX_MESSAGES
        ):
            reason = "flood"
            q.clear()

    if not reason:
        return False

    try:

        if reason in {
            "enlace",
            "palabra prohibida",
        }:

            telegram_api(
                "deleteMessage",
                {
                    "chat_id": chat_id,
                    "message_id": message[
                        "message_id"
                    ],
                },
            )

    except Exception:
        pass

    try:

        with db() as conn:

            conn.execute(
                """
                INSERT INTO warnings(
                    chat_id,
                    user_id,
                    count
                )
                VALUES(?,?,1)
                ON CONFLICT(chat_id,user_id)
                DO UPDATE SET count=count+1
                """,
                (
                    chat_id,
                    uid,
                ),
            )

            count = conn.execute(
                """
                SELECT count
                FROM warnings
                WHERE chat_id=?
                  AND user_id=?
                """,
                (
                    chat_id,
                    uid,
                ),
            ).fetchone()["count"]

        send_message(
            chat_id,
            (
                f"🛡️ {mention_name(user)}: "
                f"{reason}. "
                f"Warning {count}/{MAX_WARNINGS}."
            ),
        )

        if count >= MAX_WARNINGS:

            telegram_api(
                "restrictChatMember",
                {
                    "chat_id": chat_id,
                    "user_id": uid,
                    "permissions": {
                        "can_send_messages": False
                    },
                },
            )

    except Exception:

        logger.exception(
            "Fallo en moderación"
        )

    return True


# ============================================================
# UPDATE PROCESSING
# ============================================================

def process_update(
    update: dict[str, Any],
) -> None:

    try:

        # ----------------------------------------------------
        # DUPLICATE UPDATE PROTECTION
        # ----------------------------------------------------

        update_id = update.get(
            "update_id"
        )

        if isinstance(
            update_id,
            int,
        ):

            if already_processed(
                update_id
            ):
                logger.debug(
                    "Update duplicado ignorado: %s",
                    update_id,
                )
                return

        # ----------------------------------------------------
        # MESSAGE
        # ----------------------------------------------------

        message = update.get(
            "message"
        )

        if not isinstance(
            message,
            dict,
        ):
            return

        chat = message.get(
            "chat"
        )

        if (
            not isinstance(
                chat,
                dict,
            )
            or "id" not in chat
        ):
            return

        chat_id = int(
            chat["id"]
        )

        mid = message.get(
            "message_id"
        )

        if (
            isinstance(mid, int)
            and was_ours(
                chat_id,
                mid,
            )
        ):
            return

        user = message.get(
            "from"
        )

        if (
            isinstance(user, dict)
            and user.get("is_bot")
        ):
            return

        # ----------------------------------------------------
        # NEW MEMBERS
        # ----------------------------------------------------

        new_members = message.get(
            "new_chat_members"
        )

        if (
            isinstance(
                new_members,
                list,
            )
            and is_group(chat)
        ):

            s = setting(
                chat_id
            )

            if (
                s["welcome_enabled"]
                and s["welcome_text"]
            ):

                for member in new_members:

                    send_message(
                        chat_id,
                        render_template(
                            s["welcome_text"],
                            member,
                            chat,
                        ),
                        mid,
                    )

            return

        # ----------------------------------------------------
        # LEFT MEMBER
        # ----------------------------------------------------

        left = message.get(
            "left_chat_member"
        )

        if (
            isinstance(
                left,
                dict,
            )
            and is_group(chat)
        ):

            s = setting(
                chat_id
            )

            if (
                s["goodbye_enabled"]
                and s["goodbye_text"]
            ):

                send_message(
                    chat_id,
                    render_template(
                        s["goodbye_text"],
                        left,
                        chat,
                    ),
                    mid,
                )

            return

        if not isinstance(
            user,
            dict,
        ):
            return

        uid = int(
            user["id"]
        )

        # ----------------------------------------------------
        # USER CACHE
        # ----------------------------------------------------

        remember_user(
            chat_id,
            user,
        )

        replied = message.get(
            "reply_to_message"
        )

        if (
            isinstance(
                replied,
                dict,
            )
            and isinstance(
                replied.get("from"),
                dict,
            )
        ):
            remember_user(
                chat_id,
                replied["from"],
            )

        # ----------------------------------------------------
        # INTERNAL KIWBOT MUTE
        #
        # Admins can still manage the muted user.
        # Everyone else is ignored completely.
        # ----------------------------------------------------

        if (
            is_group(chat)
            and is_bot_muted(
                chat_id,
                uid,
            )
            and not is_admin(
                chat_id,
                uid,
            )
        ):
            logger.debug(
                "Usuario ignorado por mute interno: "
                "%s",
                uid,
            )
            return

        # ----------------------------------------------------
        # TEXT
        # ----------------------------------------------------

        text = message.get(
            "text"
        )

        if (
            not isinstance(
                text,
                str,
            )
            or not text.strip()
        ):
            return

        # ----------------------------------------------------
        # PENDING SETTINGS
        # ----------------------------------------------------

        if pending_prompt(
            chat_id,
            uid,
            text,
            chat,
            mid,
        ):
            return

        # ----------------------------------------------------
        # MODERATION
        # ----------------------------------------------------

        if moderation(
            message,
            chat,
            user,
        ):
            return

        # ----------------------------------------------------
        # COMMANDS
        # ----------------------------------------------------

        cmd, arg = command_parts(
            text
        )

        if cmd:

            answer = command_response(
                message,
                chat,
                user,
                cmd,
                arg,
            )

            if answer is not None:

                if (
                    answer
                    != "__KIWBOT_IMAGE_SENT__"
                ):

                    send_message(
                        chat_id,
                        answer,
                        mid,
                        message.get(
                            "message_thread_id"
                        ),
                    )

                return

        # ----------------------------------------------------
        # GROUP AI
        # ----------------------------------------------------

        if (
            is_group(chat)
            and REQUIRE_MENTION
        ):

            bot_username = get_bot_username()

            mentioned = bool(
                bot_username
                and f"@{bot_username}"
                in text.lower()
            )

            replied_to_bot = (
                isinstance(
                    message.get(
                        "reply_to_message"
                    ),
                    dict,
                )
                and isinstance(
                    message[
                        "reply_to_message"
                    ].get("from"),
                    dict,
                )
                and message[
                    "reply_to_message"
                ]["from"].get(
                    "is_bot"
                )
                is True
            )

            if not (
                mentioned
                or replied_to_bot
            ):
                return

        # ----------------------------------------------------
        # REPLY CONTEXT
        # ----------------------------------------------------

        reply_context = None

        reply = message.get(
            "reply_to_message"
        )

        if isinstance(
            reply,
            dict,
        ):

            reply_context = str(
                reply.get("text")
                or reply.get("caption")
                or ""
            )[:2000]

        # ----------------------------------------------------
        # AI
        # ----------------------------------------------------

        answer = generate_reply(
            chat_id,
            text,
            user,
            reply_context,
        )

        send_message(
            chat_id,
            answer,
            mid,
            message.get(
                "message_thread_id"
            ),
        )

    except Exception:

        logger.exception(
            "Error procesando actualización"
        )


# ============================================================
# WEBHOOK
# ============================================================

def check_secret() -> bool:

    return (
        not TELEGRAM_WEBHOOK_SECRET
        or request.headers.get(
            "X-Telegram-Bot-Api-Secret-Token"
        )
        == TELEGRAM_WEBHOOK_SECRET
    )


def set_webhook(
    url: str,
) -> dict[str, Any]:

    parsed = urlparse(
        url
    )

    if (
        parsed.scheme != "https"
        or not parsed.netloc
    ):
        raise ValueError(
            "WEBHOOK_URL debe ser HTTPS"
        )

    payload = {
        "url": url.rstrip("/")
    }

    if TELEGRAM_WEBHOOK_SECRET:

        payload[
            "secret_token"
        ] = TELEGRAM_WEBHOOK_SECRET

    return telegram_api(
        "setWebhook",
        payload,
    )


# ============================================================
# ROUTES
# ============================================================

@app.get("/")
def home():

    return jsonify(
        {
            "status": "ok",
            "bot": "KiwBot",
            "version": "2.2",
        }
    )


@app.get("/healthz")
def healthz():

    return jsonify(
        {
            "status": "ok",
            "bot": "KiwBot",
            "version": "2.2",
            "telegram_configured": bool(
                TELEGRAM_TOKEN
            ),
            "groq_configured": bool(
                groq_client
            ),
        }
    )


@app.post("/webhook")
def webhook():

    if not check_secret():

        return (
            jsonify(
                {
                    "ok": False,
                    "error": "unauthorized",
                }
            ),
            401,
        )

    update = request.get_json(
        silent=True
    )

    if not isinstance(
        update,
        dict,
    ):

        return (
            jsonify(
                {
                    "ok": False,
                    "error": "invalid JSON",
                }
            ),
            400,
        )

    executor.submit(
        process_update,
        update,
    )

    return jsonify(
        {
            "ok": True
        }
    )


# ============================================================
# MAIN
# ============================================================

if __name__ == "__main__":

    if WEBHOOK_URL:

        try:

            set_webhook(
                WEBHOOK_URL
            )

            logger.info(
                "Webhook configurado: %s",
                WEBHOOK_URL,
            )

        except Exception:

            logger.exception(
                "No se pudo configurar WEBHOOK_URL"
            )

    app.run(
        host="0.0.0.0",
        port=PORT,
    )
