"""Точка входа для NATS приложения."""

import asyncio

from apps.nats_app.main import main

if __name__ == "__main__":
    asyncio.run(main())
