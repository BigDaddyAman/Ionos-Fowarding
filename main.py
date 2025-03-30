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
                # Refresh the message before forwarding
                refreshed_message = await app.get_messages(
                    chat_id=config.SOURCE_CHANNEL,
                    message_ids=message.id
                )
                
                if refreshed_message:
                    await app.forward_messages(
                        chat_id=config.DEST_CHANNEL,
                        from_chat_id=config.SOURCE_CHANNEL,
                        message_ids=refreshed_message.id
                    )
                    total_messages += 1

                    if refreshed_message.video:
                        print(f"Forwarded video message {i}/{len(messages)}: {refreshed_message.video.file_name}")
                    elif refreshed_message.document:
                        print(f"Forwarded document video {i}/{len(messages)}: {refreshed_message.document.file_name}")
                
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
                print(f"Error forwarding message {message.id}: {e}")
                await asyncio.sleep(60)

        # Small delay between batches
        await asyncio.sleep(30)

async def start_bot():
    try:
        await forward_oldest_first()
    finally:
        await app.stop()

if __name__ == "__main__":
    app.run(start_bot())
