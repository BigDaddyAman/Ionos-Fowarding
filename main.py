import asyncio
import random
from pyrogram import Client
import config
import os

# Add video extensions support
VIDEO_EXTENSIONS = [".mp4", ".webm", ".mkv", ".3gp", ".avi", ".mov", ".wmv", 
                   ".flv", ".m4v", ".mpg", ".mpeg", ".vob", ".ogv", ".rm", 
                   ".rmvb", ".asf", ".ts"]

app = Client(config.SESSION_NAME, config.API_ID, config.API_HASH)

def is_video(message):
    # Check if message is a video
    if message.video:
        return True
    # Check if message is a document with video extension
    if message.document:
        filename = message.document.file_name
        if filename:
            _, ext = os.path.splitext(filename.lower())
            return ext in VIDEO_EXTENSIONS
    return False

async def forward_oldest_first(app):  # Add app parameter
    messages = []
    total_messages = 0  
    offset_id = 0
    hours_passed = 0  

    while True:
        batch = []
        async for message in app.get_chat_history(config.SOURCE_CHANNEL, offset_id=offset_id, limit=100):
            # Only append if it's a video
            if is_video(message):
                batch.append(message)

        if not batch:
            break  # ✅ No more messages left to fetch

        messages.extend(batch)
        offset_id = batch[-1].id  # ✅ Use `.id`, not `.message_id`

        print(f"Fetched {len(messages)} video messages so far...")

    messages.reverse()  # ✅ Process from oldest to newest

    print(f"Total messages to forward: {len(messages)}")

    for i, message in enumerate(messages, start=1):
        try:
            # Forward with original sender information
            await message.forward(
                config.DEST_CHANNEL,
                disable_notification=True
            )
            total_messages += 1

            print(f"Forwarded message {i}/{len(messages)}")

            await asyncio.sleep(random.randint(10, 13))

            if total_messages % 360 == 0:
                hours_passed += 1
                short_break = random.randint(300, 900)
                print(f"Taking a {short_break//60} min break...")
                await asyncio.sleep(short_break)

            if hours_passed >= 16:
                print("Taking a 1-hour break...")
                await asyncio.sleep(3600)
                hours_passed = 0

        except Exception as e:
            print(f"Error forwarding message {i}: {e}")

async def start_bot():
    try:
        async with Client(
            config.SESSION_NAME,
            config.API_ID,
            config.API_HASH
        ) as app:
            await forward_oldest_first(app)  # Pass app to the function
    except Exception as e:
        print(f"Error in start_bot: {e}")

if __name__ == "__main__":
    # Create a new event loop
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(start_bot())
    except Exception as e:
        print(f"Main loop error: {e}")
    finally:
        loop.close()
