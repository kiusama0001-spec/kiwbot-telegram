"bot_token_configured": bool(
            TELEGRAM_TOKEN
        ),

        "groq_configured": bool(
            GROQ_API_KEY
        ),

        "owner_id": OWNER_TELEGRAM_ID,

        "kalu_id": KALU_TELEGRAM_ID,

        "model": MODEL_NAME
    })


# =========================================================
# ROOT
# =========================================================

@app.route(
    "/",
    methods=["GET"]
)
def index():

    return jsonify({

        "bot": "KiwBot",

        "status": "online"
    })


# =========================================================
# CONFIGURAR WEBHOOK
# =========================================================

def configure_webhook():

    if not TELEGRAM_TOKEN:

        logger.warning(
            "No se configuró webhook porque falta TELEGRAM_TOKEN."
        )

        return


    if not WEBHOOK_URL:

        logger.warning(
            "WEBHOOK_URL no configurado."
        )

        return


    webhook_url = (
        WEBHOOK_URL.rstrip("/")
        + "/webhook"
    )


    data = {
        "url": webhook_url
    }


    if TELEGRAM_WEBHOOK_SECRET:

        data[
            "secret_token"
        ] = TELEGRAM_WEBHOOK_SECRET


    logger.info(
        "Configurando webhook: %s",
        webhook_url
    )


    result = telegram(
        "setWebhook",
        data
    )


    logger.info(
        "setWebhook: %s",
        result
    )


# =========================================================
# START
# =========================================================

if __name__ == "__main__":

    logger.info(
        "Iniciando KiwBot..."
    )

    logger.info(
        "Owner ID: %s",
        OWNER_TELEGRAM_ID
    )

    logger.info(
        "Kalu/Kat ID: %s",
        KALU_TELEGRAM_ID
    )

    configure_webhook()

    app.run(
        host="0.0.0.0",
        port=PORT
    )
