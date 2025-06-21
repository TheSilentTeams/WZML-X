import os
import asyncio
from secrets import token_hex

from .... import (
    LOGGER,
    task_dict,
    task_dict_lock,
)
from ...ext_utils.task_manager import (
    check_running_tasks,
    stop_duplicate_check,
    limit_checker,
)
from ...listeners.direct_listener import DirectListener
from ...mirror_leech_utils.status_utils.direct_status import DirectStatus
from ...mirror_leech_utils.status_utils.queue_status import QueueStatus
from ...telegram_helper.message_utils import send_status_message
from ...ext_utils.bypasser import get_real_download_links  # Import bypasser


async def add_direct_download(listener, path):
    url = listener.link if isinstance(listener.link, str) else listener.link.get("url", "")

    # Run bypasser only for hubcloud links
    if "hubcloud" in url:
        LOGGER.info("Running Selenium bypass for hubcloud link...")
        links = await asyncio.to_thread(get_real_download_links, url)
        if not links:
            await listener.on_download_error("❌ Could not bypass or extract download link.")
            return

        final_link = links[0]
        listener.link = {
            "contents": [final_link],
            "total_size": 0,  # Optional: use aiohttp HEAD to get real size if needed
            "title": os.path.basename(final_link),
        }
    else:
        LOGGER.info("Skipping bypass — not a hubcloud link.")

    details = listener.link

    if not (contents := details.get("contents")):
        await listener.on_download_error("❌ There is nothing to download!")
        return

    listener.size = details["total_size"]

    if not listener.name:
        listener.name = details["title"]
    path = f"{path}/{listener.name}"

    # Duplicate check
    msg, button = await stop_duplicate_check(listener)
    if msg:
        await listener.on_download_error(msg, button)
        return

    # Limit check
    if limit_exceeded := await limit_checker(listener):
        await listener.on_download_error(limit_exceeded, is_limit=True)
        return

    # Check if queued
    gid = token_hex(5)
    add_to_queue, event = await check_running_tasks(listener)
    if add_to_queue:
        LOGGER.info(f"🕒 Added to Queue: {listener.name}")
        async with task_dict_lock:
            task_dict[listener.mid] = QueueStatus(listener, gid, "dl")
        await listener.on_download_start()
        if listener.multi <= 1:
            await send_status_message(listener.message)
        await event.wait()
        if listener.is_cancelled:
            return

    # Download options
    a2c_opt = {"follow-torrent": "false", "follow-metalink": "false"}
    if header := details.get("header"):
        a2c_opt["header"] = header

    directListener = DirectListener(path, listener, a2c_opt)

    async with task_dict_lock:
        task_dict[listener.mid] = DirectStatus(listener, directListener, gid)

    if add_to_queue:
        LOGGER.info(f"▶️ Start Queued Download: {listener.name}")
    else:
        LOGGER.info(f"▶️ Start Download: {listener.name}")
        await listener.on_download_start()
        if listener.multi <= 1:
            await send_status_message(listener.message)

    await directListener.download(contents)
