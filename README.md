# KiwBot 2.0 — Diva de Telegram

KiwBot 2.0 es un bot híbrido:

- Las respuestas normales salen de archivos locales y no necesitan IA.
- Groq es opcional y se usa como respaldo para conversación generativa.
- En grupos, KiwBot solo conversa cuando la mencionan o cuando responden a uno de sus mensajes.
- Los comandos funcionan de forma explícita.
- La moderación automática puede actuar aunque nadie mencione al bot.
- Puede enviar su imagen de diva cuando le preguntan cómo es.
- Incluye `/define`, `/wiki`, `/search` e `/img`.
- Guarda advertencias y filtros en SQLite.
- Tiene personalidad femenina, de diva/reina, sarcástica con los demás y respetuosa con Kiu/Amo.

## Archivos

```text
kiwbot2/
├── main.py
├── requirements.txt
├── .env.example
├── README.md
├── data/
│   ├── respuestas.json
│   └── filtros.json
└── assets/
    └── kiwbot.png
```

## 1. Crear el bot

En Telegram abre @BotFather y crea/usa tu bot.

Necesitas el token de Telegram.

## 2. Preparar las variables

Copia `.env.example` como `.env` y rellena:

- `TELEGRAM_TOKEN`
- `WEBHOOK_URL`
- `TELEGRAM_WEBHOOK_SECRET`
- `OWNER_ID`

`GROQ_API_KEY` es opcional. Si no existe, las funciones locales siguen funcionando.

## 3. Instalar

```bash
python -m venv .venv
```

Windows:

```bash
.venv\Scripts\activate
```

Linux/macOS:

```bash
source .venv/bin/activate
```

Después:

```bash
pip install -r requirements.txt
```

## 4. Ejecutar

```bash
python main.py
```

El programa configura el webhook automáticamente si `WEBHOOK_URL` está configurada.

También puedes comprobar:

```text
/healthz
```

en el navegador de tu servidor.

## 5. Permisos de Telegram

Para moderar grupos, añade KiwBot como administrador.

Para las funciones de moderación dale, como mínimo:

- eliminar mensajes;
- restringir miembros;
- expulsar/bloquear miembros, si quieres `/ban` y `/kick`.

Telegram exige los permisos administrativos correspondientes para esas operaciones.

## 6. Comportamiento de conversación

En un grupo:

```text
Luis: hola KiwBot
KiwBot: ¿Qué quieres, criatura? 👑

Luis: KiwBot, ¿cómo eres?
KiwBot: [envía su imagen de diva]
```

También responde si:

1. el mensaje contiene `@NombreDelBot`;
2. es una respuesta a un mensaje de KiwBot.

Un mensaje normal sin mención no genera conversación.

Los comandos, por ser invocaciones explícitas, sí funcionan sin mencionar al bot:

```text
/help
/verdad
/reto
/dado
/define nostalgia
/wiki México
/search Python
/img gatos
```

## 7. Moderación

La moderación automática NO necesita que mencionen a KiwBot.

Incluye:

```text
/warn
/warns
/unwarn
/mute
/unmute
/kick
/ban
/unban
/del
/purge
/filter
/filters
/stop
```

Los comandos administrativos se comprueban contra los administradores de Telegram y, además, Kiu tiene acceso de dueño.

## 8. Frases

Edita:

```text
data/respuestas.json
```

Puedes añadir cientos o miles de frases sin modificar `main.py`.

La estructura es:

```json
{
  "saludo": ["...", "..."],
  "insulto": ["...", "..."],
  "general": ["...", "..."]
}
```

También puedes editar `data/filtros.json` para respuestas por palabras.

## 9. Imagen de KiwBot

`assets/kiwbot.png` es la imagen de diva que KiwBot manda cuando detecta:

- "¿cómo eres?"
- "como eres"
- "quién eres?"
- "quien eres"
- "muéstrame cómo eres"
- `/yo`

Puedes reemplazar ese PNG por otra imagen manteniendo el mismo nombre.

## 10. Importante sobre "ilimitado"

El bot no impone un número artificial de respuestas locales. Las frases, dados, retos, filtros, comandos y moderación funcionan sin llamar a Groq.

Eso no significa que Telegram, el servidor o un proveedor externo sean literalmente ilimitados. Telegram y Groq tienen sus propios límites técnicos. La arquitectura está pensada para que la gran mayoría de interacciones no dependan de la IA.

## 11. Groq

Groq ofrece una API compatible con el cliente de OpenAI usando:

```text
https://api.groq.com/openai/v1
```

KiwBot usa esa compatibilidad. Si Groq falla o no está configurado, KiwBot intenta responder localmente en lugar de quedarse inutilizado.

## 12. Producción

Para un servidor con HTTPS, usa el webhook.

No publiques nunca:

- `TELEGRAM_TOKEN`
- `GROQ_API_KEY`
- `.env`

El archivo `kiwbot.db` se crea automáticamente.
