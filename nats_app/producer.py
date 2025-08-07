"""Продюсер для отправки сообщений в NATS."""

import json
import logging
from typing import Any

from nats.aio.client import Client as NATS
from nats.js import JetStreamContext

from apps.common.schemas import AttachmentType
from apps.nats_app.models import SimpleMessage


class SimpleProducer:
    """Продюсер сообщений в NATS."""

    def __init__(
        self, nats_client: NATS, js_context: JetStreamContext, logger: logging.Logger
    ):
        self.nats_client = nats_client
        self.js_context = js_context
        self.logger = logger

    async def send_message(
        self,
        user_id: int,
        text: str,
        message_type: str,
        attachments: str | None = None,
        attachment_type: AttachmentType | None = None,
        buttons: list[dict[str, Any]] | None = None,
        broadcast_id: str | None = None,
        total_expected: int | None = None,
        parse_mode: str = "HTML",
    ) -> bool:
        """Отправка любого типа сообщения в NATS."""
        try:
            message = SimpleMessage(
                user_id=user_id,
                text=text,
                message_type=message_type,
                attachments=attachments,
                attachment_type=attachment_type,
                buttons=buttons,
                broadcast_id=broadcast_id,
                total_expected=total_expected,
                parse_mode=parse_mode,
            )

            # Публикуем сообщение
            ack = await self.js_context.publish(
                subject="telegram.messages",
                payload=message.model_dump_json().encode(),
            )

            # Если это рассылка, публикуем информацию о ней
            if broadcast_id:
                await self._publish_broadcast_info(message)

            self.logger.info(
                f"📤 Сообщение отправлено в NATS для пользователя {user_id} (seq: {ack.seq})"
            )
            return True

        except Exception as e:
            self.logger.error(f"❌ Ошибка отправки сообщения в NATS: {e}")
            return False

    async def _publish_broadcast_info(self, message: SimpleMessage) -> None:
        """Публикация информации о рассылке."""
        try:
            broadcast_data = {
                "broadcast_id": message.broadcast_id,
                "message_type": message.message_type,
                "text_preview": message.text[:100],
                "total_expected": message.total_expected,
                "user_id": message.user_id,
                "timestamp": message.created_at.isoformat(),
            }

            await self.js_context.publish(
                subject="telegram.broadcasts",
                payload=json.dumps(broadcast_data, ensure_ascii=False).encode(),
            )
        except Exception as e:
            self.logger.error(f"❌ Ошибка публикации информации о рассылке: {e}")
