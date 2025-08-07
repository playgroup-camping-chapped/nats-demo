"""Модели данных для NATS сообщений."""

from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field

from apps.common.schemas import AttachmentType


class SimpleMessage(BaseModel):
    """Универсальная модель сообщения для всех типов отправки."""

    # Основные поля
    user_id: int = Field(..., description="ID пользователя в Telegram")
    text: str = Field(..., description="Текст сообщения")
    parse_mode: str = Field(default="HTML", description="Режим парсинга (HTML/Markdown)")

    # Опциональные поля
    attachments: str | None = Field(
        default=None, description="file_id или путь к файлу"
    )
    attachment_type: AttachmentType | None = Field(
        default=None, description="Тип вложения - обязателен при наличии вложения"
    )
    buttons: list[dict[str, Any]] | None = Field(
        default=None, description="Кнопки в формате InlineKeyboardMarkup"
    )

    # Метаданные для отслеживания
    message_type: str = Field(
        ..., description="Тип сообщения (reminder, broadcast, follow_up)"
    )
    broadcast_id: str | None = Field(
        default=None, description="ID рассылки для отслеживания завершения"
    )
    total_expected: int | None = Field(
        default=None, description="Общее количество получателей в рассылке"
    )

    created_at: datetime = Field(default_factory=datetime.utcnow)


class MessageResult(BaseModel):
    """Результат обработки сообщения."""

    success: bool = Field(..., description="Успешность отправки")
    message_id: str | None = Field(None, description="ID сообщения в Telegram")
    error_type: str | None = Field(None, description="Тип ошибки")
    error_message: str | None = Field(None, description="Текст ошибки")
    user_subscribed_to_bot: bool | None = Field(
        None, description="Подписан ли пользователь на бота"
    )
    processed_at: datetime = Field(default_factory=datetime.utcnow)
