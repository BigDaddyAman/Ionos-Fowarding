import asyncio
import random
from pyrogram import Client
import config

app = Client(config.SESSION_NAME, config.API_ID, config.API_HASH)

# Add video extensions list at the top level
VIDEO_EXTENSIONS = [
    ".mp4", ".webm", ".mkv", ".3gp", ".avi", ".mov", ".wmv", ".flv", ".m4v", 
    ".mpg", ".mpeg", ".vob", ".ogv", ".rm", ".rmvb", ".asf", ".ts"
]

async def forward_oldest_first():
    total_messages = 0
    hours_passed = 0
    batch_size = 100
    offset_id = 0

    while True:
        messages = []
        async for message in app.get_chat_history(config.SOURCE_CHANNEL, offset_id=offset_id, limit=batch_size):
            # Only append video messages
            if (message.video or 
                (message.document and any(
                    message.document.file_name.lower().endswith(ext) 
                    for ext in VIDEO_EXTENSIONS
                ))):
                messages.append(message)
            
        if not messages:
            print("No more video messages to forward")
            break

        offset_id = messages[-1].id
        messages.reverse()  # Process from oldest to newest

        for i, message in enumerate(messages, start=1):
            try:
                await message.copy(config.DEST_CHANNEL)
                total_messages += 1

                # Print appropriate message based on type
                if message.video:
                    print(f"Forwarded video message {i}/{len(messages)}: {message.video.file_name}")
                elif message.document:
                    print(f"Forwarded document video {i}/{len(messages)}: {message.document.file_name}")
                
                await asyncio.sleep(random.randint(15, 20))

                if total_messages % 300 == 0:
                    hours_passed += 1
                    short_break = random.randint(900, 1800)
                    print(f"Taking a {short_break//60} min break after {total_messages} messages...")
                    await asyncio.sleep(short_break)

                if hours_passed >= 12:
                    print("Taking a 2-hour break...")
                    await asyncio.sleep(7200)
                    hours_passed = 0

            except Exception as e:
                print(f"Error forwarding message: {e}")
                await asyncio.sleep(60)

        # Small delay between batches
        await asyncio.sleep(30)

async def start_bot():
    async with app:
        await forward_oldest_first()

if __name__ == "__main__":
    asyncio.run(start_bot())
