# KiwBot 2.1

KiwBot es un bot de Telegram con personalidad de diva/reina, respuestas locales, Groq opcional, moderación y configuración persistente por grupo.

## Comandos de bienvenida y reglas

- `/setwelcome [texto]`
- `/welcome`
- `/delwelcome`
- `/setgoodbye [texto]`
- `/goodbye`
- `/delgoodbye`
- `/setrules [texto]`
- `/rules`
- `/delrules`

Si usas `/setwelcome` sin texto, KiwBot espera el siguiente mensaje del mismo administrador y lo guarda.

Variables disponibles en mensajes:
- `{name}`
- `{username}`
- `{id}`
- `{chat}`
- `{chat_id}`
- `{bot}`

## Moderación

`/warn`, `/unwarn`, `/warns`, `/mute`, `/unmute`, `/kick`, `/ban`, `/unban`, `/del`, `/filter`, `/stop`, `/filters`, `/antilink`, `/antispam`.

Los comandos administrativos se comprueban contra los permisos reales de Telegram.

## Importante

No subas `.env` ni `kiwbot.db` a GitHub. Usa las variables privadas de tu plataforma de alojamiento.
