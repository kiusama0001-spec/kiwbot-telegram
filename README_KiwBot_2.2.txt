# KiwBot 2.3 — Memoria y Control de IA

Esta versión conserva todo lo desarrollado en KiwBot 2.2 y añade memoria permanente, preferencias de Kiu y control independiente de la IA.

## Cambios principales
- KiwBot ahora cuenta con memoria permanente mediante SQLite.
- Puede recordar información incluso después de reiniciar el bot.
- La memoria de Kiu puede mantenerse entre diferentes grupos.
- Se añadieron los comandos `/recuerda`, `/memoria` y `/olvida`.
- También reconoce frases naturales como "recuerda que..." y "no olvides que...".
- KiwBot puede guardar preferencias de Kiu como "no me digas rey del Club América".
- Las preferencias guardadas se incorporan al contexto de la IA para que las respete posteriormente.
- Se añadieron `/iaon`, `/iaoff` y `/iastatus`.
- El estado de la IA se guarda independientemente para cada grupo.
- Solo Kiu, verificado mediante su ID de Telegram, puede encender o apagar la IA.
- Apagar la IA no desactiva los comandos normales del bot.
- Se mantiene el sistema de castigo con `/kiwmute` y `/kiwunmute`.
- Se mantiene el reconocimiento especial de Kalu/Kat mediante su ID.
- Se conserva la moderación, advertencias, filtros, flood control y protección contra updates duplicados.
- KiwBot continúa utilizando Groq para sus respuestas de IA.

## Comandos nuevos

`/iaon` — Enciende la IA en el chat actual.  
`/iaoff` — Apaga la IA en el chat actual.  
`/iastatus` — Muestra si la IA está encendida o apagada.

`/recuerda [texto]` — Guarda un recuerdo permanente.  
`/memoria` — Muestra los recuerdos disponibles.  
`/olvida [texto]` — Elimina recuerdos que coincidan con el texto.

## Comandos principales

`/ping` — Comprueba que KiwBot esté funcionando.  
`/yo` — Comprueba la identidad del usuario.  
`/help` — Muestra la ayuda.  
`/rules` — Muestra las reglas del grupo.

`/kiwmute` — Castiga y silencia a KiwBot.  
`/kiwunmute` — Termina el castigo de KiwBot.

`/warn` — Añade una advertencia.  
`/unwarn` — Elimina las advertencias.  
`/mute` — Silencia a un usuario.  
`/unmute` — Quita el silencio.  
`/kick` — Expulsa a un usuario.  
`/ban` — Banea a un usuario.  
`/unban` — Desbanea a un usuario.

`/truth` — Pregunta aleatoria de verdad.  
`/dare` — Reto aleatorio.

## Instalación

Reemplaza el `main.py` de tu proyecto por el incluido en esta versión.

Variables recomendadas en Render:

TELEGRAM_TOKEN=...  
GROQ_API_KEY=...  
WEBHOOK_URL=https://tu-servicio.onrender.com  
TELEGRAM_WEBHOOK_SECRET=...  
OWNER_ID=7745029153  
OWNER_NAME=Kiu  
OWNER_TITLE=Amo  
KALU_ID=282157809  
REQUIRE_MENTION=true  
AUTO_MODERATION=true  
MAX_WARNINGS=3  
FLOOD_WINDOW_SECONDS=8  
FLOOD_MAX_MESSAGES=6  
GROQ_MODEL=openai/gpt-oss-20b

Mantén tus carpetas `data/` y `assets/` actuales junto a `main.py`.

## Importante

La identidad de Kiu continúa verificándose mediante su ID de Telegram. Decir "soy Kiu", "soy tu Amo" o frases similares no concede privilegios.

Kalu y Kat son reconocidas como la misma persona mediante su ID de Telegram y nunca deben confundirse con Kiu.

La memoria permanente se guarda en SQLite y KiwBot utiliza esos recuerdos como contexto para sus futuras respuestas.
