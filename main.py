import os
import logging
import re
import asyncio
import hashlib
import psycopg2
from datetime import datetime, time, timedelta
from dotenv import load_dotenv
from aiogram import Bot, Dispatcher, types, F
from aiogram.types import (
    InlineKeyboardMarkup, 
    InlineKeyboardButton,
    InlineQuery,
    InlineQueryResultArticle,
    InputTextMessageContent
)
from aiohttp import web
from redis_cache import RedisCache
from backup import BackupBot
from database import (
    init_db,
    save_video_to_db,
    get_videos_by_name,
    get_video_by_token,
    get_db_connection,
    save_token,
    add_or_update_user,
    get_all_users,
    get_cached_stats,
    init_stats_table,
    update_stats_periodically,
    reset_daily_stats,
    track_search
)
from constants import (
    BOT_USERNAME,
    SUPPORT_GROUP,
    REQUIRED_CHANNEL,
    CHANNEL_INVITE_LINK,
    AUTHORIZED_USER_IDS,
    VIDEO_EXTENSIONS,
    WELCOME_MESSAGE,
    UNAUTHORIZED_MESSAGE,
    ADMIN_COMMANDS,
    NOT_FOUND_MESSAGE,
    BROADCAST_USAGE,
    BROADCAST_ERROR,
    BROADCAST_STATUS,
    BROADCAST_PROGRESS,
    BROADCAST_COMPLETE,
    BOT_STATUS,
    PREMIUM_NOTIFICATION,
    VIDEO_CAPTION,
    USER_STATS,
    ADMIN_STATS,  
    ADVERTISEMENT_INFO,
    SOURCE_CHANNELS,
    MESSAGE_DELAY,
    BATCH_DELAY,
    BROADCAST_BATCH_SIZE,
    HELP_MESSAGE,  # Add this line
)
from premium import (
    add_premium_user,
    check_premium_status,
    is_premium,
    handle_premium_command,
    handle_renew_command,
    handle_video_for_premium
)
from broadcast import broadcast_command
from auto_tables import setup_database
from database import init_db
init_db()

# Create set from SOURCE_CHANNELS for faster lookup
SOURCE_CHANNELS_SET = set(SOURCE_CHANNELS)

load_dotenv()
API_TOKEN = os.getenv('BOT_TOKEN')
WEBSITE_URL = os.getenv('WEBSITE_URL')

# Set up simpler logging format
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S'
)

# Remove the complex formatter and handlers setup
main_logger = logging.getLogger('MainBot')
backup_logger = logging.getLogger('BackupBot')

# Initialize Redis with debug
async def init_redis():
    try:
        redis_cache = RedisCache()
        # Test connection
        test_key = "test:connection"
        test_value = {"test": "data"}
        
        main_logger.info("Testing Redis connection...")
        set_result = redis_cache.set_cache(test_key, test_value)
        main_logger.info(f"Set test key result: {set_result}")
        
        get_result = redis_cache.get_cache(test_key)
        main_logger.info(f"Get test key result: {get_result}")
        
        if get_result == test_value:
            main_logger.info("✅ Redis connection test successful")
        else:
            main_logger.error("❌ Redis test failed - data mismatch")
            
        return redis_cache
    except Exception as e:
        main_logger.error(f"❌ Redis initialization error: {str(e)}")
        raise

# Create bot and dispatcher
bot = Bot(token=API_TOKEN)
dp = Dispatcher()

# Initialize Redis cache
redis_cache = RedisCache()

# Add webhook settings
WEBHOOK_HOST = os.getenv('WEBHOOK_HOST')
WEBHOOK_PATH = f'/webhook/{os.getenv("BOT_TOKEN")}'
WEBHOOK_URL = f'https://{WEBHOOK_HOST}{WEBHOOK_PATH}'
PORT = int(os.getenv('PORT', 8080))

def clean_video_name(video_name: str) -> str:
    """Clean video name by removing credits and special characters"""
    # Remove credit section and unwanted text
    patterns_to_remove = [
        r'Credit.*$',  # Remove credit section
        r'@\w+',  # Remove @ mentions
        r'https?://\S+',  # Remove URLs
        r't\.me/\S+',  # Remove telegram links
        r'Join \w+',  # Remove "Join Channel/Group" text
        r'Channel \w+',  # Remove "Channel" text
        r'Group \w+',  # Remove "Group" text
        r'Telegram \w+',  # Remove "Telegram" text
        r'\[.*?\]',  # Remove text in square brackets
        r'\((?:TAG|LINK|SHARE)\)',  # Remove common tags
    ]
    # Apply all cleanup patterns
    for pattern in patterns_to_remove:
        video_name = re.sub(pattern, '', video_name, flags=re.IGNORECASE)
    # Replace special characters with dots
    video_name = re.sub(r'[^\w\s]', '.', video_name)
    # Replace multiple spaces with single dot
    video_name = re.sub(r'\s+', '.', video_name)
    # Clean up multiple dots and trim
    video_name = re.sub(r'\.+', '.', video_name)
    video_name = video_name.strip('.')
    return video_name

# Remove all @dp.message_handler decorators and use the new format
async def check_subscription(user_id: int) -> bool:
    """Check if user is subscribed to required channel"""
    try:
        member = await bot.get_chat_member(REQUIRED_CHANNEL, user_id)
        return member.status not in ["left", "kicked", "banned"]
    except Exception as e:
        logging.error(f"Error checking subscription: {e}")
        return False

async def check_auth(message: types.Message) -> bool:
    """Check if user is authorized and subscribed"""
    user_id = message.from_user.id
    # Always allow authorized users
    if user_id in AUTHORIZED_USER_IDS:
        return True
    # Check channel subscription
    if not await check_subscription(user_id):
        await message.reply(
            UNAUTHORIZED_MESSAGE.format(CHANNEL_INVITE_LINK=CHANNEL_INVITE_LINK),
            disable_web_page_preview=True
        )
        return False
    return True

async def handle_video(message: types.Message):
    """Handle video uploads from authorized users"""
    # Only allow authorized users to save videos
    if message.from_user.id not in AUTHORIZED_USER_IDS:
        await message.reply(
            UNAUTHORIZED_MESSAGE.format(CHANNEL_INVITE_LINK=CHANNEL_INVITE_LINK),
            disable_web_page_preview=True
        )
        return

    file_name = message.video.file_name
    if not any(file_name.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
        return

    raw_video_name = message.caption or "Untitled"
    video_name = clean_video_name(raw_video_name)
    file_id = message.video.file_id
    access_hash = message.video.file_unique_id
    upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # Save to database with clean video name
    save_video_to_db(video_name, message.caption, file_id, access_hash, upload_date)
    # Log instead of sending messages
    logging.info(f"Auto-storing file from channel: {file_name}")
    logging.info(f"Successfully stored metadata for {file_name}")

VIDEO_EXTENSIONS = ['.mp4', '.mkv', '.avi', '.mov', '.flv', '.wmv', '.m4v', '.ts']

async def handle_document(message: types.Message):
    """Handle document uploads from authorized users"""
    if message.from_user.id not in AUTHORIZED_USER_IDS:
        await message.reply(
            UNAUTHORIZED_MESSAGE.format(CHANNEL_INVITE_LINK=CHANNEL_INVITE_LINK),
            disable_web_page_preview=True
        )
        return

    if not message.document or not message.document.file_name:
        return

    file_name = message.document.file_name.lower()
    if not any(file_name.endswith(ext) for ext in VIDEO_EXTENSIONS):
        return

    raw_video_name = message.caption or file_name
    video_name = clean_video_name(raw_video_name)
    file_id = message.document.file_id
    access_hash = message.document.file_unique_id
    upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Save to database with clean video name
    save_video_to_db(video_name, message.caption, file_id, access_hash, upload_date, is_document=True)
    await message.reply(f"✅ Video document saved: {video_name}")
    logging.info(f"Stored document: {file_name}")

async def start_command(message: types.Message):
    """Handle /start command"""
    user = message.from_user
    add_or_update_user(
        user_id=user.id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name
    )
    
    if not await check_auth(message):
        return

    args = message.text.split()[1:] if len(message.text.split()) > 1 else []
    if args:
        # Check if it's a search share (s_query format)
        if args[0].startswith('s_'):
            search_query = args[0][2:].replace('_', ' ')  # Remove s_ and convert back to spaces
            logging.info(f"Handling shared search: {search_query}")
            await display_search_results(message, search_query, page=1)
            return
        # Handle regular video token
        try:
            video = get_video_by_token(args[0])
            if (video):
                await bot.send_video(
                    chat_id=message.chat.id,
                    video=video['file_id'],
                    caption=f"🎬 {video['video_name']}\n\n📤 @kakifilembot"
                )
            else:
                await message.reply("Invalid or expired token. Please try again.")
        except Exception as e:
            logging.error(f"Error sending video: {e}")
            await message.reply("Sorry, there was an error processing your request.")
    else:
        await message.reply(WELCOME_MESSAGE)

async def search_videos(message: types.Message):
    if not await check_auth(message):
        return

    query = message.text.split(" ", 1)
    if len(query) < 2:
        await message.reply("Please provide a video name to search.")
        return

    video_name = query[1]
    page = 1
    per_page = 10

    # Get total count and videos
    total_videos, videos = get_videos_by_name(video_name, page, per_page)
    if not videos:
        await message.reply("No videos found.")
        return

    # Create inline keyboard with video buttons
    buttons = []
    current_row = []

    for video in videos:
        token = hashlib.sha256(f"{video[0]}:{message.from_user.id}:{datetime.now()}".encode()).hexdigest()
        website_link = f"{WEBSITE_URL}/index.html?token={token}&videoName={video[1]}"
        current_row.append(InlineKeyboardButton(
            text=video[1][:30] + "..." if len(video[1]) > 30 else video[1],
            url=website_link
        ))
        # Create rows of videos
        if len(current_row) == 2:  # 2 videos per row
            buttons.append(current_row)
            current_row = []

    # Add remaining videos if any
    if current_row:
        buttons.append(current_row)
    # Calculate total pages
    total_pages = (total_videos + per_page - 1) // per_page
    # Add navigation row
    nav_row = []
    # First page button
    if page > 1:
        nav_row.append(InlineKeyboardButton("⏮️ First", callback_data="page_1"))
    # Previous page button
    if page > 1:
        nav_row.append(InlineKeyboardButton("◀️", callback_data=f"page_{page-1}"))
    # Page numbers
    for p in range(max(1, page-2), min(total_pages + 1, page+3)):
        nav_row.append(InlineKeyboardButton(
            f"[{p}]" if p == page else str(p),
            callback_data=f"page_{p}"
        ))
    # Next page button
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("▶️", callback_data=f"page_{page+1}"))
    # Last page button
    if page < total_pages:
        nav_row.append(InlineKeyboardButton("⏭️ Last", callback_data=f"page_{total_pages}"))
    buttons.append(nav_row)

    # Add total results info
    await message.reply(
        f"🔍 Found {total_videos} results for '{video_name}'\n"
        f"📄 Page {page} of {total_pages}",
        reply_markup=InlineKeyboardMarkup(inline_keyboard=buttons)
    )

async def handle_text_search(message: types.Message):
    """Handle text search and track user activity"""
    user = message.from_user
    search_query = message.text.strip()
    
    # Don't process commands
    if search_query.startswith('/'):
        return
        
    logging.info(f"Processing search: {search_query}")  # Add logging
    
    try:
        # Track user and search
        add_or_update_user(
            user_id=user.id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name
        )
        track_search(user.id, search_query)
        
        # Try to get from cache first
        logging.info("Checking Redis cache...")  # Add logging
        cached_results = redis_cache.get_cached_search(search_query)
        
        if cached_results:
            logging.info("Cache hit! Using cached results")  # Add logging
            total_count, videos = cached_results
        else:
            logging.info("Cache miss! Querying database...")  # Add logging
            # If not in cache, query database and cache results
            total_count, videos = get_videos_by_name(search_query, page=1)
            if videos:
                logging.info(f"Caching {total_count} results for '{search_query}'")  # Add logging
                redis_cache.cache_search_results(search_query, videos, total_count)
                logging.info("Results cached successfully")  # Add logging
            else:
                logging.info("No results found to cache")  # Add logging

        # Verify cache status after operation
        cache_status = redis_cache.debug_cache_status()
        logging.info(f"Cache status after search: {cache_status}")  # Add logging

        await display_search_results(message, search_query, page=1)
        
    except Exception as e:
        logging.error(f"Error in search handling: {e}")
        await message.reply("An error occurred while processing your search")

async def display_search_results(message: types.Message, search_query: str, page: int = 1):
    """Display search results with pagination"""
    per_page = 10
    total_count, videos = get_videos_by_name(search_query, page, per_page)
    user_id = message.from_user.id if isinstance(message, types.Message) else message.from_user.id
    is_premium_user = is_premium(user_id)
    if not videos:
        await message.reply(NOT_FOUND_MESSAGE)
        return

    buttons = []
    total_pages = (total_count + per_page - 1) // per_page
    # Create video buttons (1 per row)
    for video in videos:
        video_id, video_name, _, file_id, upload_date, is_document = video
        token = hashlib.sha256(f"{video_id}:{user_id}:{datetime.now()}".encode()).hexdigest()[:32]
        try:
            save_token(video_id, user_id, token)
            button_text = f"📄 {video_name[:40]}" if is_document else f"🎬 {video_name[:40]}"
            button_text += "..." if len(video_name) > 40 else ""
            
            if is_premium_user:
                buttons.append([
                    InlineKeyboardButton(
                        text=button_text,
                        callback_data=f"v_{token}"
                    )
                ])
            else:
                buttons.append([
                    InlineKeyboardButton(
                        text=button_text,
                        url=f"{WEBSITE_URL}/index.html?token={token}&videoName={video_name}"
                    )
                ])
        except Exception as e:
            logging.error(f"Error saving token: {e}")
            continue
    # Navigation row with page numbers
    nav_row = []
    # Previous page button
    if page > 1:
        nav_row.append(InlineKeyboardButton(
            text="◀️",
            callback_data=f"page_{search_query}_{page-1}"
        ))
    # Add page numbers (1 2 3 4 5)
    start_page = max(1, page - 2)
    end_page = min(total_pages, page + 2)
    for p in range(start_page, end_page + 1):
        nav_row.append(InlineKeyboardButton(
            text=f"[{p}]" if p == page else str(p),
            callback_data=f"page_{search_query}_{p}"
        ))
    # Next page button
    if page < total_pages:
        nav_row.append(InlineKeyboardButton(
            text="▶️",
            callback_data=f"page_{search_query}_{page+1}"
        ))
    buttons.append(nav_row)
    # First/Last page row
    first_last_row = [
        InlineKeyboardButton(text="First Page", callback_data=f"page_{search_query}_1"),
        InlineKeyboardButton(text="Last Page", callback_data=f"page_{search_query}_{total_pages}")
    ]
    buttons.append(first_last_row)
    # Add copy button for admins
    if message.from_user.id in AUTHORIZED_USER_IDS:
        admin_row = [
            InlineKeyboardButton(
                text="Get Share Link",
                callback_data=f"copy_{search_query}"
            )
        ]
        buttons.append(admin_row)
    
    # Update share button to use switch_inline_query
    share_row = [
        InlineKeyboardButton(
            text="Share Results",
            switch_inline_query=f"share_{search_query}_{total_count}"  # Pass total_count in query
        )
    ]
    buttons.append(share_row)
    text = f"🔍 Found {total_count} results for '{search_query}'\n📄 Page {page} of {total_pages}"
    markup = InlineKeyboardMarkup(inline_keyboard=buttons)
    try:
        if isinstance(message, types.CallbackQuery):
            await message.message.edit_text(text, reply_markup=markup)
        else:
            await message.answer(text, reply_markup=markup)
    except Exception as e:
        logging.error(f"Error sending message: {e}")

async def handle_video_selection(callback_query: types.CallbackQuery):
    """Handle video selection"""
    user_id = callback_query.from_user.id
    chat_id = callback_query.message.chat.id
    try:
        if is_premium(user_id):
            token = callback_query.data.split('_')[1]
            logging.info(f"Processing video request - User: {user_id}, Token: {token}")
            
            video = get_video_by_token(token)
            if not video:
                logging.error(f"Video not found for token: {token}")
                await callback_query.answer("Video not found or expired")
                return
                
            try:
                # Send video
                sent_video = await bot.send_video(
                    chat_id=chat_id,
                    video=video['file_id'],
                    caption=f"🎬 {video['video_name']}\n\n📤 @kakifilembot"
                )
                # Log success with video details
                logging.info(
                    f"File {video['video_name']} sent successfully to user {user_id}"
                )
                await callback_query.answer("Video sent!")
                
            except Exception as e:
                logging.error(f"Error delivering video '{video['video_name']}' to user {user_id}: {e}")
                await callback_query.answer("Error processing video selection")
    except Exception as e:
        logging.error(f"Error processing video selection: {e}")
        await callback_query.answer("Error processing video selection")

async def handle_pagination(callback_query: types.CallbackQuery):
    """Handle pagination callback"""
    try:
        # Format: page_search-query_pagenumber
        _, search_query, page = callback_query.data.split('_', 2)
        page = int(page)
        # Log success
        logging.info(f"Handling pagination: query='{search_query}', page={page}")
        
        # Display search results for the requested page
        await display_search_results(callback_query, search_query, page)
        await callback_query.answer()
    except Exception as e:
        logging.error(f"Pagination error: {e}", exc_info=True)
        await callback_query.answer("Error processing pagination")

async def renew_command(message: types.Message):
    """Handle /renew command for admins"""
    logging.info(f"Renew command received from user {message.from_user.id}")  # Add logging
    if message.from_user.id not in AUTHORIZED_USER_IDS:
        await message.reply("⚠️ This command is only for administrators.")
        return

    args = message.text.split()
    if len(args) != 3:
        await message.reply("Usage: /renew <user_id> <days>")
        return
    try:
        user_id = int(args[1])
        days = int(args[2])
        
        logging.info(f"Processing renewal: user_id={user_id}, days={days}")  # Add logging
        success, expiry_date = add_premium_user(user_id, days)
        if success and expiry_date:
            await message.reply(
                "✅ Premium access granted!\n\n"
                f"👤 User ID: {user_id}\n"
                f"⏳ Duration: {days} days\n"
                f"📅 Expires: {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}"
            )
            logging.info(f"Successfully renewed premium for user {user_id}")  # Add logging
        else:
            await message.reply("❌ Failed to grant premium access.")
            logging.error(f"Failed to renew premium for user {user_id}")  # Add logging
    except Exception as e:
        logging.error(f"Error in renew command: {e}")
        await message.reply(f"❌ Error: {str(e)}")

async def premium_command(message: types.Message):
    """Handle /premium command"""
    user_id = message.from_user.id
    is_premium_user, days_remaining, expiry_date = check_premium_status(user_id)
    if not is_premium_user:
        await message.reply(
            "🚀 Anda Bukan Pengguna Premium!\n"
            "📌 Nikmati carian pantas & akses tanpa iklan selama 3 bulan hanya RM10!\n"
            "💎 *Hanya RM10 untuk 3 bulan akses premium!*\n\n"
            "📩 *Dapatkan sekarang di @Kakifilem2019bot dan nikmati keistimewaan eksklusif!*" 
        )
        return

    await message.reply(
        "💎 Premium Status\n\n"
        f"✅ Active\n"
        f"⏳ Days remaining: {days_remaining}\n"
        f"📅 Expires: {expiry_date.strftime('%Y-%m-%d %H:%M:%S')}"
    )

async def status_command(message: types.Message):
    """Handle /status command"""
    try:
        stats = get_cached_stats()  # Changed back to get_cached_stats
        # Format top searches text
        top_searches_text = ""
        if stats['top_searches']:
            for term, count in stats['top_searches']:
                top_searches_text += f"\n• {term}: {count} searches"
        else:
            top_searches_text = "\n• No searches today"
            
        # Use different templates for admin and regular users
        if message.from_user.id in AUTHORIZED_USER_IDS:
            stats_text = ADMIN_STATS.format(
                total_users=stats['total_users'],
                active_today=stats['active_today'],
                active_month=stats['active_month'],
                premium_users=stats['premium_users'],
                searches_today=stats['searches_today'],
                total_videos=stats['total_videos'],
                top_searches=top_searches_text
            )
        else:
            stats_text = USER_STATS.format(
                total_users=stats['total_users'],
                searches_today=stats['searches_today'],
                total_videos=stats['total_videos'],
                top_searches=top_searches_text
            )
        await message.reply(stats_text)
        logging.info(f"Stats sent to user {message.from_user.id}")
    except Exception as e:
        logging.error(f"Error in status command: {e}")
        await message.reply("❌ Error fetching statistics")

async def iklan_command(message: types.Message):
    """Handle /iklan command"""
    # Get first admin's username for contact
    try:
        admin = await bot.get_chat_member(chat_id=message.chat.id, user_id=AUTHORIZED_USER_IDS[0])
        admin_username = admin.user.username
    except:
        admin_username = "admin"  # Fallback if can't get username
    await message.reply(
        ADVERTISEMENT_INFO.format(admin_username=admin_username),
        disable_web_page_preview=True
    )

# Update broadcast command to use BROADCAST_ERROR instead of BROADCAST_TIME_ERROR
async def send_broadcast_message(bot: Bot, user_id: int, message: types.Message, broadcast_text: str) -> bool:
    """Send a single broadcast message with error handling"""
    try:
        if message.reply_to_message and message.reply_to_message.content_type in ['photo', 'video', 'document']:
            caption = None if broadcast_text.lower() == 'none' else broadcast_text
            
            if message.reply_to_message.photo:
                await bot.send_photo(
                    chat_id=user_id,
                    photo=message.reply_to_message.photo[-1].file_id,
                    caption=caption
                )
            elif message.reply_to_message.video:
                await bot.send_video(
                    chat_id=user_id,
                    video=message.reply_to_message.video.file_id,
                    caption=caption
                )
            elif message.reply_to_message.document:
                await bot.send_document(
                    chat_id=user_id,
                    document=message.reply_to_message.document.file_id,
                    caption=caption
                )
        else:
            await bot.send_message(user_id, broadcast_text)
            
        return True
    except Exception as e:
        main_logger.error(f"Failed to send broadcast to user {user_id}: {e}")
        return False

async def broadcast_command(message: types.Message):
    """Handle /broadcast command for admins"""
    if message.from_user.id not in AUTHORIZED_USER_IDS:
        await message.reply("⚠️ This command is only for administrators.")
        return

    broadcast_text = message.text.replace('/broadcast', '').strip()
    if not broadcast_text:
        await message.reply(BROADCAST_USAGE)
        return

    try:
        # Get all users
        users = await get_all_users(exclude_admins=True)
        total_users = len(users)
        
        if total_users == 0:
            await message.reply("No users to broadcast to.")
            return

        # Calculate daily limits
        users_per_day = total_users
        current_hour = datetime.now().hour

        # Send initial status
        status_msg = await message.reply(
            f"📡 Starting broadcast...\n"
            f"👥 Total users: {total_users}\n"
            f"📊 Users per day: {users_per_day}\n"
            f"⏱ Estimated time: {(users_per_day * MESSAGE_DELAY) / 60:.1f} minutes"
        )

        success_count = 0
        fail_count = 0
        
        # Process users in batches
        for i in range(0, total_users, BROADCAST_BATCH_SIZE):
            batch = users[i:i + BROADCAST_BATCH_SIZE]
            batch_start_time = datetime.now()
            
            for user_id in batch:
                try:
                    # Send the message
                    if message.reply_to_message and message.reply_to_message.content_type in ['photo', 'video', 'document']:
                        caption = None if broadcast_text.lower() == 'none' else broadcast_text
                        if message.reply_to_message.photo:
                            await bot.send_photo(
                                chat_id=user_id,
                                photo=message.reply_to_message.photo[-1].file_id,
                                caption=caption
                            )
                        elif message.reply_to_message.video:
                            await bot.send_video(
                                chat_id=user_id,
                                video=message.reply_to_message.video.file_id,
                                caption=caption
                            )
                        elif message.reply_to_message.document:
                            await bot.send_document(
                                chat_id=user_id,
                                document=message.reply_to_message.document.file_id,
                                caption=caption
                            )
                    else:
                        await bot.send_message(user_id, broadcast_text)
                    
                    success_count += 1
                except Exception as e:
                    logging.error(f"Failed to send to user {user_id}: {e}")
                    fail_count += 1

                await asyncio.sleep(MESSAGE_DELAY)

            # Calculate progress
            progress = min(i + BROADCAST_BATCH_SIZE, total_users)
            success_rate = (success_count / (success_count + fail_count)) * 100 if (success_count + fail_count) > 0 else 0

            # Update status message
            await status_msg.edit_text(
                f"🔄 Broadcasting in progress...\n"
                f"✅ Sent: {success_count}\n"
                f"❌ Failed: {fail_count}\n"
                f"📊 Progress: {progress}/{total_users}\n"
                f"⚡️ Success Rate: {success_rate:.1f}%"
            )

            # Add delay between batches
            elapsed = (datetime.now() - batch_start_time).total_seconds()
            if elapsed < BATCH_DELAY:
                await asyncio.sleep(BATCH_DELAY - elapsed)

        # Final status update
        total_processed = success_count + fail_count
        final_success_rate = (success_count / total_processed * 100) if total_processed > 0 else 0
        
        await status_msg.edit_text(
            f"✅ Broadcast completed!\n"
            f"📨 Sent: {success_count}\n"
            f"❌ Failed: {fail_count}\n"
            f"📊 Total Processed: {total_processed}\n"
            f"⚡️ Success Rate: {final_success_rate:.1f}%\n"
            f"⏱ Completed at: {datetime.now().strftime('%H:%M:%S')}"
        )
        logging.info(f"Broadcast completed: {success_count} sent, {fail_count} failed")

    except Exception as e:
        logging.error(f"Broadcast error: {e}")
        await message.reply(f"❌ Error during broadcast: {str(e)}")

async def schedule_daily_reset():
    """Schedule daily stats reset at midnight"""
    while True:
        now = datetime.now()
        # Calculate time until next midnight
        midnight = datetime.combine(now.date(), time(0, 0)) + timedelta(days=1)
        seconds_until_midnight = (midnight - now).total_seconds()
        # Wait until midnight
        await asyncio.sleep(seconds_until_midnight)
        # Reset stats
        await reset_daily_stats()
        logging.info("Daily stats reset completed")

async def handle_channel_video(message: types.Message):
    """Handle videos from source channels"""
    try:
        channel_id = message.chat.id
        
        if str(channel_id) not in [str(x) for x in SOURCE_CHANNELS]:
            return
            
        if message.video:
            file = message.video
            file_name = message.video.file_name
            file_id = message.video.file_id
            access_hash = message.video.file_unique_id
            video_name = clean_video_name(message.caption or file_name)
            upload_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            
            # Simplified logging
            logging.info(f"Auto-storing file from channel: {file_name}")
            save_video_to_db(video_name, message.caption, file_id, access_hash, upload_date)
            logging.info(f"Successfully stored metadata for {file_name}")
            
        elif message.document and any(message.document.file_name.lower().endswith(ext) for ext in VIDEO_EXTENSIONS):
            # Similar simplified logging for documents
            file_name = message.document.file_name
            # ...rest of document handling code...
            
    except Exception as e:
        logging.error(f"Error: {str(e)}")

async def handle_inline_query(inline_query: InlineQuery):
    """Handle inline queries for group sharing"""
    query = inline_query.query.strip()
    
    if (query.startswith('share_')):
        try:
            # Parse search query and total count from inline query
            _, search_query, total_count = query.split('_', 2)
            total_count = int(total_count)
            
            share_text = (
                f"👀 Whoa! We just dug up {total_count} results for '{search_query}'!\n"
                f"🎥 Click below to browse them all:\n\n"
                f"https://t.me/{BOT_USERNAME}?start=s_{search_query.replace(' ', '_')}"
            )
            
            results = [
                InlineQueryResultArticle(
                    id="share_results",
                    title=f"Share {total_count} results for '{search_query}'",
                    description="Click to share these search results",
                    input_message_content=InputTextMessageContent(
                        message_text=share_text,
                        disable_web_page_preview=True
                    )
                )
            ]
            await inline_query.answer(results, cache_time=1)
        except Exception as e:
            logging.error(f"Error in inline sharing: {e}")
    else:
        await inline_query.answer([], cache_time=1)

async def handle_copy_link(callback_query: types.CallbackQuery):
    """Handle copy link button for admins"""
    if callback_query.from_user.id not in AUTHORIZED_USER_IDS:
        await callback_query.answer("This feature is only for admins")
        return
    try:
        # Get search query from callback data
        _, search_query = callback_query.data.split('_', 1)
        share_link = f"https://t.me/{BOT_USERNAME}?start=s_{search_query.replace(' ', '_')}"
        # Send link as a separate message that's easy to copy
        await callback_query.message.answer(share_link)
        await callback_query.answer("Link sent! Just tap to copy.")
    except Exception as e:
        logging.error(f"Error handling copy link: {e}")
        await callback_query.answer("Error generating link")

# Add this new function after the other command handlers
async def help_command(message: types.Message):
    """Handle /help command"""
    await message.reply(HELP_MESSAGE, parse_mode="HTML")

# Add this new function after the other command handlers
async def redis_debug_command(message: types.Message):
    """Handle /redis_debug command for admins"""
    if message.from_user.id not in AUTHORIZED_USER_IDS:
        await message.reply("⚠️ This command is only for administrators.")
        return
        
    try:
        cache_status = redis_cache.debug_cache_status()
        
        if cache_status['connected']:
            # Format sample searches
            samples = "\n".join([
                f"• '{s['term']}' ({s['total_results']} results, expires in {s['expires_in']}s)"
                for s in cache_status['sample_searches']
            ])
            
            status_text = (
                "📊 Redis Cache Status:\n\n"
                f"✅ Connection: Active\n"
                f"💾 Memory Used: {cache_status['memory_used']}\n"
                f"🔑 Total Keys: {cache_status['total_keys']}\n"
                f"🔍 Search Keys: {cache_status['search_keys']}\n"
                f"📈 Stats Keys: {cache_status['stats_keys']}\n\n"
                f"📝 Sample Searches:\n{samples}"
            )
        else:
            status_text = f"❌ Redis Error: {cache_status['error']}"
            
        await message.reply(status_text)
        
    except Exception as e:
        await message.reply(f"❌ Error checking Redis: {str(e)}")

# Update main() to register inline handler
async def on_startup(app):
    """Set webhook on startup"""
    webhook_info = await bot.get_webhook_info()
    if (webhook_info.url != WEBHOOK_URL):
        await bot.set_webhook(url=WEBHOOK_URL)
    logging.info(f"Webhook set to {WEBHOOK_URL}")

async def handle_webhook(request):
    """Handle webhook requests"""
    try:
        update = types.Update(**await request.json())
        await dp.feed_update(bot=bot, update=update)
        return web.Response()
    except Exception as e:
        logging.error(f"Webhook error: {e}")
        return web.Response(status=500)

async def handle_health_check(request):
    """Handle health check requests"""
    return web.Response(text="OK", status=200)

async def main():
    try:
        # Initialize database
        setup_database()
        
        # Initialize backup bot first
        backup_bot = BackupBot()
        
        # Set up webhook app
        app = web.Application()
        app.router.add_get('/', handle_health_check)
        app.router.add_post(WEBHOOK_PATH, handle_webhook)
        app.on_startup.append(on_startup)
        
        # Start background tasks
        asyncio.create_task(update_stats_periodically())
        asyncio.create_task(schedule_daily_reset())
        
        # Start backup bot in polling mode
        polling_task = asyncio.create_task(backup_bot.start())
        main_logger.info("Backup bot polling task created")
        
        # Wait a moment to ensure backup bot starts
        await asyncio.sleep(2)
        
        # Register handlers for main bot
        # Channel post handlers - put these first
        dp.channel_post.register(handle_channel_video, lambda msg: msg.video and msg.chat.id in SOURCE_CHANNELS)
        dp.channel_post.register(handle_channel_video, lambda msg: msg.document and msg.chat.id in SOURCE_CHANNELS)
        
        # Regular message handlers
        dp.message.register(lambda msg: handle_video_for_premium(bot, msg.chat.id, msg.video.file_id), 
                          lambda msg: msg.video and is_premium(msg.from_user.id))
        dp.message.register(start_command, F.text.startswith("/start"))
        dp.message.register(status_command, F.text.startswith("/status"))
        dp.message.register(premium_command, F.text.startswith("/premium"))
        dp.message.register(renew_command, F.text.startswith("/renew"))
        dp.message.register(broadcast_command, F.text.startswith("/broadcast"))
        dp.message.register(iklan_command, F.text.startswith("/iklan"))
        dp.message.register(help_command, F.text.startswith("/help"))  # Add this line
        dp.message.register(redis_debug_command, F.text.startswith("/redis_debug"))  # Add this line
        dp.message.register(handle_video, F.video)
        dp.message.register(handle_document, F.document)
        dp.message.register(handle_text_search, F.text)
        # Callback handlers
        dp.callback_query.register(handle_video_selection, F.data.startswith("v_"))
        dp.callback_query.register(handle_pagination, F.data.startswith("page_"))
        dp.callback_query.register(handle_copy_link, F.data.startswith("copy_"))
        
        # Add back the inline query handler for sharing
        dp.inline_query.register(handle_inline_query)
        
        logging.info(f"Main bot started as @{BOT_USERNAME}")
        return app
        
    except Exception as e:
        logging.error(f"Startup error: {e}", exc_info=True)

if __name__ == '__main__':
    app = asyncio.get_event_loop().run_until_complete(main())
    port = int(os.getenv('PORT', 8080))
    web.run_app(app, host='0.0.0.0', port=port)









