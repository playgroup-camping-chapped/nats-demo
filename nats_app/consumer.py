"""Потребитель NATS для обработки сообщений."""

import asyncio
import json
import logging

from aiogram import Bot
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramNotFound,
    TelegramRetryAfter,
)
from aiogram.types import InlineKeyboardMarkup
from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from apps.common.database.models.user import UserORM
from apps.common.database.session import async_session_maker
from apps.common.schemas import AttachmentType
from apps.core.config import Settings
from apps.nats_app.models import MessageResult, SimpleMessage


class SimpleConsumer:
    """Потребитель сообщений из NATS."""

    def __init__(
        self,
        nats_client: NATS,
        js_context: JetStreamContext,
        bot: Bot,
        logger: logging.Logger,
    ):
        self.nats_client = nats_client
        self.js_context = js_context
        self.bot = bot
        self.logger = logger
        self._running = False
        self._consumer = None

        # Отслеживание завершения рассылок
        self.broadcast_stats: dict[str, dict[str, int]] = {}
        self.completed_broadcasts: set[str] = (
            set()
        )  # Защита от дублирования уведомлений

        # Очистка памяти каждые 10 минут
        self._cleanup_task = None

    async def start(self) -> None:
        """Запуск потребителя."""
        try:
            # Создаем consumer
            self._consumer = await self.js_context.pull_subscribe(
                subject="telegram.messages",
                durable="telegram_sender",
            )

            self._running = True
            self.logger.info("🚀 Потребитель NATS запущен")

            # Запускаем задачу очистки памяти
            self._cleanup_task = asyncio.create_task(self._memory_cleanup_loop())

            # Запускаем обработку
            await self._process_messages()

        except Exception as e:
            self.logger.error(f"❌ Ошибка запуска потребителя: {e}")
            # Попробуем еще раз через небольшую паузу
            await asyncio.sleep(2)
            try:
                self._consumer = await self.js_context.pull_subscribe(
                    subject="telegram.messages",
                    durable="telegram_sender",
                )
                self._running = True
                self.logger.info("🚀 Потребитель NATS запущен (повторная попытка)")
                self._cleanup_task = asyncio.create_task(self._memory_cleanup_loop())
                await self._process_messages()
            except Exception as retry_error:
                self.logger.error(
                    f"❌ Критическая ошибка после повторной попытки: {retry_error}"
                )
                raise

    async def stop(self) -> None:
        """Остановка потребителя."""
        self._running = False

        # Останавливаем задачу очистки памяти
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Отписываемся от consumer
        if self._consumer:
            try:
                await self._consumer.unsubscribe()
            except Exception as e:
                self.logger.debug(f"Ошибка при отписке от consumer: {e}")

        self.logger.info("🛑 Потребитель NATS остановлен")

    async def _memory_cleanup_loop(self) -> None:
        """Периодическая очистка памяти от старых данных."""
        while self._running:
            try:
                await asyncio.sleep(600)
                await self._cleanup_old_broadcasts()
                self.logger.info("🧹 Очищена память от старых рассылок")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"❌ Ошибка очистки памяти: {e}")

    async def _cleanup_old_broadcasts(self) -> None:
        """Очистка старых данных о рассылках."""
        try:
            # Очищаем completed_broadcasts если их больше 1000
            if len(self.completed_broadcasts) > 1000:
                self.completed_broadcasts.clear()
                self.logger.info("🧹 Очищена память от старых завершенных рассылок")

            # Очищаем broadcast_stats если их больше 100
            if len(self.broadcast_stats) > 100:
                # Оставляем только активные рассылки (не завершенные)
                active_broadcasts = {}
                for broadcast_id, stats in self.broadcast_stats.items():
                    if (
                        stats["total_expected"]
                        and stats["total_processed"] < stats["total_expected"]
                    ):
                        active_broadcasts[broadcast_id] = stats

                self.broadcast_stats = active_broadcasts
                self.logger.info(
                    f"🧹 Очищена память от старых рассылок. Осталось активных: {len(self.broadcast_stats)}"
                )

        except Exception as e:
            self.logger.error(f"❌ Ошибка очистки старых рассылок: {e}")

    async def _process_messages(self) -> None:
        """Обработка сообщений из очереди."""
        self.logger.info("🔄 Начинаем обработку сообщений из очереди...")

        while self._running:
            try:
                # Получаем батч сообщений
                messages = await self._consumer.fetch(batch=25, timeout=5.0)

                if not messages:
                    continue

                self.logger.info(f"📦 Получен батч из {len(messages)} сообщений")

                tasks = []
                for msg in messages:
                    task = asyncio.create_task(self._process_message(msg))
                    tasks.append(task)

                await asyncio.gather(*tasks, return_exceptions=True)

            except TimeoutError:
                continue
            except Exception as e:
                self.logger.error(f"❌ Ошибка обработки батча: {e}")

    async def _process_message(self, msg) -> MessageResult:
        """Обработка одного сообщения."""
        try:
            # Парсим сообщение
            data = json.loads(msg.data.decode())
            message = SimpleMessage(**data)

            self.logger.info(
                f"📤 Отправляем сообщение пользователю {message.user_id}: {message.text[:50]}..."
            )

            # Отправляем в Telegram
            result = await self._send_to_telegram(message)

            # Публикуем результат в отдельный стрим для мониторинга
            await self._publish_result(message, result)

            await msg.ack()
            self.logger.info("✅ Сообщение обработано и подтверждено (ack)")

            # Отслеживаем прогресс рассылки
            if message.broadcast_id:
                await self._track_broadcast_progress(
                    message.broadcast_id,
                    message.user_id,
                    result,
                    message.total_expected,
                )

            return result

        except TelegramRetryAfter as e:
            self.logger.warning(f"⏳ Rate limit, сообщение будет обработано позже: {e}")
            return MessageResult(
                success=False,
                error_type="RATE_LIMIT",
                error_message=f"через {e.retry_after} сек",
            )

        except Exception as e:
            self.logger.error(f"❌ Ошибка обработки сообщения: {e}")
            await msg.ack()  # Все равно отмечаем как обработанное
            error_result = MessageResult(
                success=False, error_type="PROCESSING_ERROR", error_message=str(e)
            )
            await self._publish_error(message, error_result)
            return error_result

    async def _send_to_telegram(self, message: SimpleMessage) -> MessageResult:
        """Отправка сообщения в Telegram."""
        try:
            # Подготавливаем клавиатуру
            keyboard = None
            if message.buttons:
                keyboard = InlineKeyboardMarkup(inline_keyboard=message.buttons)

            # Если есть вложение и текст больше 1000 символов, отправляем вложение отдельно
            if message.attachments and len(message.text) > 1000:
                await self._send_attachment(
                    message.user_id, message.attachments, message.attachment_type
                )

                telegram_msg = await self.bot.send_message(
                    chat_id=message.user_id,
                    text=message.text,
                    parse_mode=message.parse_mode,
                    reply_markup=keyboard,
                )
            elif message.attachments:
                telegram_msg = await self._send_attachment_with_text(
                    message.user_id,
                    message.attachments,
                    message.attachment_type,
                    message.text,
                    message.parse_mode,
                    keyboard,
                )
            else:
                telegram_msg = await self.bot.send_message(
                    chat_id=message.user_id,
                    text=message.text,
                    parse_mode=message.parse_mode,
                    reply_markup=keyboard,
                )

            self.logger.info(f"✅ Сообщение отправлено пользователю {message.user_id}")
            return MessageResult(success=True, message_id=str(telegram_msg.message_id))

        except TelegramRetryAfter as e:
            # Ошибка 429 - превышен лимит запросов
            self.logger.warning(
                f"⏳ Rate limit для пользователя {message.user_id}: повтор через {e.retry_after} сек"
            )
            # НЕ отмечаем сообщение как обработанное, чтобы повторить попытку
            raise  # Пробрасываем ошибку дальше

        except TelegramNotFound:
            # Пользователь не найден - отмечаем как отписанного
            self.logger.warning(f"❌ Пользователь {message.user_id} не найден")
            await self._mark_user_unsubscribed(message.user_id)
            return MessageResult(
                success=False, error_type="USER_NOT_FOUND", user_subscribed_to_bot=False
            )

        except TelegramForbiddenError:
            # Пользователь заблокировал бота - отмечаем как отписанного
            self.logger.warning(f"❌ Пользователь {message.user_id} заблокировал бота")
            await self._mark_user_unsubscribed(message.user_id)
            return MessageResult(
                success=False, error_type="USER_FORBIDDEN", user_subscribed_to_bot=False
            )

    async def _publish_result(
        self, message: SimpleMessage, result: MessageResult
    ) -> None:
        """Публикация результата обработки сообщения."""
        try:
            result_dict = result.model_dump()
            if result_dict.get("processed_at"):
                result_dict["processed_at"] = result_dict["processed_at"].isoformat()

            result_data = {
                "message_id": message.user_id,
                "text_preview": message.text[:100],
                "message_type": message.message_type,
                "broadcast_id": message.broadcast_id,
                "result": result_dict,
                "timestamp": result.processed_at.isoformat(),
            }

            await self.js_context.publish(
                subject="telegram.results",
                payload=json.dumps(result_data, ensure_ascii=False).encode(),
            )
        except Exception as e:
            self.logger.error(f"❌ Ошибка публикации результата: {e}")

    async def _publish_error(
        self, message: SimpleMessage, result: MessageResult
    ) -> None:
        """Публикация ошибки в отдельный стрим."""
        try:
            error_dict = result.model_dump()
            if error_dict.get("processed_at"):
                error_dict["processed_at"] = error_dict["processed_at"].isoformat()

            error_data = {
                "message_id": message.user_id,
                "text_preview": message.text[:100],
                "message_type": message.message_type,
                "broadcast_id": message.broadcast_id,
                "error": error_dict,
                "timestamp": result.processed_at.isoformat(),
            }

            await self.js_context.publish(
                subject="telegram.errors",
                payload=json.dumps(error_data, ensure_ascii=False).encode(),
            )
        except Exception as e:
            self.logger.error(f"❌ Ошибка публикации ошибки: {e}")

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки пользователю {message.user_id}: {e}")
            return MessageResult(
                success=False, error_type="UNEXPECTED_ERROR", error_message=str(e)
            )

    async def _send_attachment_with_text(
        self,
        user_id: int,
        attachment: str,
        attachment_type: AttachmentType,
        text: str,
        parse_mode: str,
        keyboard=None,
    ):
        """Отправка вложения с текстом одним сообщением."""
        try:
            if attachment_type == AttachmentType.PHOTO:
                return await self.bot.send_photo(
                    chat_id=user_id,
                    photo=attachment,
                    caption=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )
            if attachment_type == AttachmentType.VIDEO:
                return await self.bot.send_video(
                    chat_id=user_id,
                    video=attachment,
                    caption=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )
            if attachment_type == AttachmentType.DOCUMENT:
                return await self.bot.send_document(
                    chat_id=user_id,
                    document=attachment,
                    caption=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )
            if attachment_type == AttachmentType.VOICE:
                await self.bot.send_voice(chat_id=user_id, voice=attachment)
                return await self.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )
            if attachment_type == AttachmentType.VIDEO_NOTE:
                await self.bot.send_video_note(chat_id=user_id, video_note=attachment)
                return await self.bot.send_message(
                    chat_id=user_id,
                    text=text,
                    parse_mode=parse_mode,
                    reply_markup=keyboard,
                )
            return await self.bot.send_document(
                chat_id=user_id,
                document=attachment,
                caption=text,
                parse_mode=parse_mode,
                reply_markup=keyboard,
            )

        except Exception:
            raise

    async def _send_attachment(
        self, user_id: int, attachment: str, attachment_type: AttachmentType
    ) -> None:
        """Отправка вложения без текста."""
        try:
            if attachment_type == AttachmentType.PHOTO:
                await self.bot.send_photo(chat_id=user_id, photo=attachment)
            elif attachment_type == AttachmentType.VIDEO:
                await self.bot.send_video(chat_id=user_id, video=attachment)
            elif attachment_type == AttachmentType.DOCUMENT:
                await self.bot.send_document(chat_id=user_id, document=attachment)
            elif attachment_type == AttachmentType.VOICE:
                await self.bot.send_voice(chat_id=user_id, voice=attachment)
            elif attachment_type == AttachmentType.VIDEO_NOTE:
                await self.bot.send_video_note(chat_id=user_id, video_note=attachment)
            else:
                await self.bot.send_document(chat_id=user_id, document=attachment)

        except Exception:
            # Не логируем здесь - исключение будет обработано в _send_to_telegram
            raise

    async def _mark_user_unsubscribed(self, user_id: int) -> None:
        """Отметить пользователя как отписанного от бота."""
        try:
            async with async_session_maker() as session:
                user = await session.get(UserORM, user_id)
                if user:
                    user.subscribed_to_bot = False
                    await session.commit()
                    self.logger.info(
                        f"✅ Пользователь {user_id} отмечен как отписанный"
                    )

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка обновления статуса пользователя {user_id}: {e}"
            )

    async def _track_broadcast_progress(
        self,
        broadcast_id: str,
        user_id: int,
        result: MessageResult,
        total_expected: int,
    ) -> None:
        """Отслеживание прогресса рассылки."""
        if broadcast_id not in self.broadcast_stats:
            self.broadcast_stats[broadcast_id] = {
                "total_processed": 0,
                "successful": 0,
                "unsubscribed": 0,
                "errors": 0,
                "total_expected": total_expected,
            }

        stats = self.broadcast_stats[broadcast_id]
        stats["total_processed"] += 1

        if result.success:
            stats["successful"] += 1
        elif result.user_subscribed_to_bot is False:
            stats["unsubscribed"] += 1
        else:
            stats["errors"] += 1

        # Проверяем завершение рассылки
        await self._check_broadcast_completion(broadcast_id)

    async def _check_broadcast_completion(self, broadcast_id: str) -> None:
        """Проверка завершения рассылки."""
        try:
            stats = self.broadcast_stats[broadcast_id]
            self.logger.info(
                f"📊 Прогресс рассылки {broadcast_id}: "
                f"Обработано: {stats['total_processed']}, "
                f"Успешно: {stats['successful']}, "
                f"Отписаны: {stats['unsubscribed']}, "
                f"Ошибки: {stats['errors']}",
            )

            # Проверяем завершение если указано общее количество
            if (
                stats["total_expected"]
                and stats["total_processed"] >= stats["total_expected"]
                and broadcast_id not in self.completed_broadcasts
            ):
                self.logger.info(f"✅ Рассылка {broadcast_id} завершена!")
                self.completed_broadcasts.add(broadcast_id)  # Отмечаем как завершенную
                await self._notify_broadcast_completion(broadcast_id)
                # Очищаем статистику после уведомления
                del self.broadcast_stats[broadcast_id]

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка проверки завершения рассылки {broadcast_id}: {e}"
            )

    async def _notify_broadcast_completion(self, broadcast_id: str) -> None:
        """Уведомление админов о завершении рассылки."""
        try:
            settings = Settings()

            stats = self.broadcast_stats[broadcast_id]
            message = (
                f"✅ Рассылка {broadcast_id} завершена\n"
                f"📊 Статистика:\n"
                f"• Обработано: {stats['total_processed']}\n"
                f"• Успешно отправлено: {stats['successful']}\n"
                f"• Отписаны: {stats['unsubscribed']}\n"
                f"• Ошибки: {stats['errors']}"
            )

            for admin_id in settings.tg_bot.admin_ids:
                await self.bot.send_message(chat_id=admin_id, text=message)

            self.logger.info(
                f"✅ Админы уведомлены о завершении рассылки {broadcast_id}"
            )

        except Exception as e:
            self.logger.error(
                f"❌ Ошибка уведомления админов о рассылке {broadcast_id}: {e}"
            )
