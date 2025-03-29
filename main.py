import asyncio
import random
from pyrogram import Client, filters
from pyrogram.types import Message
from config import API_ID, API_HASH, SESSION_NAME, SOURCE_CHANNEL, DEST_CHANNEL

# Initialize Pyrogram user bot
app = Client(SESSION_NAME, api_id=API_ID, api_hash=API_HASH)

async def forward_oldest_first():
    async with app:
        messages = []
        total_messages = 0  # Counter for tracking forwarded messages
        hours_passed = 0  # Track hours for the long break

        # Fetch messages in batches
        async for message in app.get_chat_history(SOURCE_CHANNEL, limit=10000):
            if message.video or (message.document and message.document.mime_type.startswith("video")):
                messages.append(message)

        # Reverse messages to process from oldest to newest
        messages.reverse()

        for message in messages:
            try:
                await message.copy(DEST_CHANNEL)  # Copy instead of forward
                total_messages += 1

                # Random delay (10-13 sec)
                await asyncio.sleep(random.randint(10, 13))

                # Every hour, take a 5-15 min break
                if total_messages % 360 == 0:  # 360 messages ≈ 1 hour (10 sec per msg avg)
                    hours_passed += 1
                    short_break = random.randint(300, 900)  # 5-15 minutes
                    print(f"Taking a {short_break//60} min break...")
                    await asyncio.sleep(short_break)

                # Every 16 hours, take a 1-hour break
                if hours_passed >= 16:
                    print("Taking a 1-hour break...")
                    app.disconnect()  # Simulate going offline
                    await asyncio.sleep(3600)  # 1-hour break
                    app.connect()  # Reconnect after the break
                    hours_passed = 0  # Reset counter

            except Exception as e:
                print(f"Error forwarding message: {e}")

@app.on_message(filters.chat(SOURCE_CHANNEL) & (filters.video | filters.document))
async def forward_videos(client: Client, message: Message):
    """Handles real-time forwarding of new messages"""
    if message.video or (message.document and message.document.mime_type.startswith("video")):
        try:
            await message.copy(DEST_CHANNEL)  # Copy instead of forward
            await asyncio.sleep(random.randint(10, 13))
        except Exception as e:
            print(f"Error forwarding message: {e}")

if __name__ == "__main__":
    print("Bot started...")

    loop = asyncio.get_event_loop()
    loop.run_until_complete(forward_oldest_first())  # Start forwarding old messages

    app.run()  # Keep running to forward new messages in real-time
