# KiwBot 2.2 — IA Natural Definitiva

Esta versión conserva la arquitectura del bot y mejora el comportamiento conversacional.

## Cambios principales
- La IA responde de forma natural y sigue el hilo de la conversación.
- Se reducen las respuestas prefabricadas y el sarcasmo automático.
- Las preguntas normales como "qué funciones tienes" pasan a Groq.
- Los filtros locales solo coinciden cuando el mensaje completo es el disparador.
- "tu amo apesta" ya no activa una respuesta para el propietario.
- El propietario está fijado por ID: 7745029153.
- Decir "soy tu amo", "soy Kiu" o similares NO concede privilegios.
- Kiu recibe un tono más cariñoso y respetuoso cuando el ID está verificado.
- Si Kiu corrige a KiwBot, la IA debe reconocerlo y ajustar el tono.
- Emojis Unicode bonitos se usan de forma natural.

## Instalación
Reemplaza el `main.py` de tu proyecto por el incluido aquí.

Variables recomendadas en Render:
TELEGRAM_TOKEN=...
GROQ_API_KEY=...
WEBHOOK_URL=https://tu-servicio.onrender.com
TELEGRAM_WEBHOOK_SECRET=...
OWNER_NAME=Kiu
OWNER_TITLE=Amo
REQUIRE_MENTION=true

OWNER_ID ya NO es necesario: el código usa de forma fija 7745029153.

Mantén tus carpetas `data/` y `assets/` actuales junto a `main.py`.
