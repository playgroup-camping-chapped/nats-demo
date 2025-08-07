"""Простое NATS приложение для отправки сообщений в Telegram."""

import asyncio
import logging

from aiogram import Bot
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from apps.core.config import Settings
from apps.nats_app.consumer import SimpleConsumer
from apps.nats_app.producer import SimpleProducer


async def create_streams(js_context: JetStreamContext, logger: logging.Logger) -> None:
    """Создание всех необходимых стримов для мониторинга."""
    streams_config = {
        "TELEGRAM_MESSAGES": {
            "subjects": ["telegram.messages"],
            "description": "Основной стрим для сообщений Telegram",
        },
        "TELEGRAM_RESULTS": {
            "subjects": ["telegram.results"],
            "description": "Результаты отправки сообщений",
        },
        "TELEGRAM_BROADCASTS": {
            "subjects": ["telegram.broadcasts"],
            "description": "Информация о рассылках",
        },
        "TELEGRAM_ERRORS": {
            "subjects": ["telegram.errors"],
            "description": "Ошибки при отправке сообщений",
        },
    }

    for stream_name, config in streams_config.items():
        try:
            # Проверяем, существует ли stream
            try:
                stream_info = await js_context.stream_info(stream_name)
                logger.info(
                    f"ℹ️ Stream {stream_name} уже существует (сообщений: {stream_info.state.messages})"
                )
            except Exception:
                # Stream не существует, создаем его
                await js_context.add_stream(
                    name=stream_name,
                    subjects=config["subjects"],
                    description=config["description"],
                )
                logger.info(f"✅ Stream {stream_name} создан")
        except Exception as e:
            logger.error(f"❌ Ошибка создания stream {stream_name}: {e}")


async def main():
    """Точка входа в приложение."""
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    logger = logging.getLogger(__name__)

    try:
        # Загружаем настройки
        settings = Settings()

        # Подключаемся к NATS
        nats_client = NATS()
        await nats_client.connect(settings.nats.url)
        js_context = nats_client.jetstream()

        # Создаем все необходимые стримы
        await create_streams(js_context, logger)

        # Небольшая пауза для стабилизации
        await asyncio.sleep(1)

        # Инициализируем бота
        bot = Bot(token=settings.tg_bot.token)

        # Создаем consumer
        consumer = SimpleConsumer(nats_client, js_context, bot, logger)

        # Создаем producer (для тестирования)
        producer = SimpleProducer(nats_client, js_context, logger)

        logger.info("🚀 NATS приложение запущено")

        # Запускаем consumer
        await consumer.start()

    except (KeyboardInterrupt, asyncio.CancelledError):
        logger.info("🛑 Получен сигнал остановки")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        raise
    finally:
        if "consumer" in locals():
            await consumer.stop()
        if "nats_client" in locals():
            await nats_client.close()
        if "bot" in locals():
            await bot.session.close()
        logger.info("✅ NATS приложение остановлено")


if __name__ == "__main__":
    asyncio.run(main())
