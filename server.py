from fastapi import FastAPI, APIRouter, Depends, HTTPException, Response, Request
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel
from typing import List, Optional
import uuid
from datetime import datetime, timezone, timedelta, date
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger
import asyncio
import bcrypt
import re
from exponent_server_sdk import PushClient, PushMessage, PushServerError, DeviceNotRegisteredError


ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# SendGrid Configuration (for future use)
SENDGRID_API_KEY = os.environ.get('SENDGRID_API_KEY')
SENDGRID_FROM_EMAIL = os.environ.get('SENDGRID_FROM_EMAIL')

# Admin emails (users with these emails have admin access)
ADMIN_EMAILS = [
    "abdelkaderbeldjoudi@gmail.com",
    "abdelkaderbeldjoudi8@gmail.com"
]

# Create the main app without a prefix
app = FastAPI()

# Create a router with the /api prefix
api_router = APIRouter(prefix="/api")

# Initialize scheduler for 24h reminders
scheduler = AsyncIOScheduler()

# Initialize Expo Push Client
push_client = PushClient()


# ============== Expo Push Notification Service ==============

async def send_push_notification(
    push_tokens: List[str],
    title: str,
    body: str,
    data: Optional[dict] = None,
    sound: str = "default",
    badge: int = 1
) -> dict:
    """
    Send push notification to devices via Expo Push Service
    
    Args:
        push_tokens: List of Expo push tokens
        title: Notification title (clear, attention-grabbing for 35-75 age group)
        body: Notification body
        data: Additional data for deep linking
        sound: Notification sound
        badge: Badge count on app icon
    """
    if not push_tokens:
        logging.warning("📲 [PUSH] No push tokens provided")
        return {"sent": 0, "failed": 0, "errors": []}
    
    # Filter valid Expo push tokens
    valid_tokens = [t for t in push_tokens if t and t.startswith("ExponentPushToken[")]
    
    if not valid_tokens:
        logging.warning("📲 [PUSH] No valid Expo push tokens found")
        return {"sent": 0, "failed": 0, "errors": []}
    
    messages = []
    for token in valid_tokens:
        message = PushMessage(
            to=token,
            title=title,
            body=body,
            sound=sound,
            badge=badge,
            data=data or {}
        )
        messages.append(message)
    
    sent_count = 0
    failed_count = 0
    errors = []
    
    try:
        # Send in chunks of 100 (Expo limit)
        chunk_size = 100
        for i in range(0, len(messages), chunk_size):
            chunk = messages[i:i + chunk_size]
            
            try:
                responses = push_client.publish_multiple(chunk)
                
                for response in responses:
                    if response.is_success():
                        sent_count += 1
                    else:
                        failed_count += 1
                        errors.append(str(response.message))
                        
            except PushServerError as exc:
                logging.error(f"📲 [PUSH] Server error: {exc}")
                failed_count += len(chunk)
                errors.append(str(exc))
                
    except Exception as exc:
        logging.error(f"📲 [PUSH] Unexpected error: {exc}")
        failed_count += len(valid_tokens)
        errors.append(str(exc))
    
    logging.info(f"📲 [PUSH] Sent: {sent_count}, Failed: {failed_count}")
    return {"sent": sent_count, "failed": failed_count, "errors": errors}


async def send_push_to_user(user_id: str, title: str, body: str, data: Optional[dict] = None):
    """Send push notification to a specific user by user_id"""
    # Get user's push tokens from database
    tokens = await db.push_tokens.find(
        {"user_id": user_id, "active": True}
    ).to_list(10)
    
    if not tokens:
        logging.info(f"📲 [PUSH] No active push tokens for user {user_id}")
        return {"sent": 0, "message": "No push tokens"}
    
    push_tokens = [t["push_token"] for t in tokens]
    return await send_push_notification(push_tokens, title, body, data)


async def send_push_to_users(user_ids: List[str], title: str, body: str, data: Optional[dict] = None):
    """Send push notification to multiple users"""
    tokens = await db.push_tokens.find(
        {"user_id": {"$in": user_ids}, "active": True}
    ).to_list(1000)
    
    if not tokens:
        logging.info(f"📲 [PUSH] No active push tokens for users {user_ids}")
        return {"sent": 0, "message": "No push tokens"}
    
    push_tokens = [t["push_token"] for t in tokens]
    return await send_push_notification(push_tokens, title, body, data)


# ============== Notification Helper Functions ==============

def normalize_sex(sex_value: str) -> str:
    """Normalize sex values to M/F/X format for consistent matching.
    Handles both old format (Male/Female/Prefer not to say) and new format (M/F/X).
    """
    if not sex_value:
        return None
    sex_upper = sex_value.upper().strip()
    if sex_upper in ['M', 'MALE']:
        return 'M'
    elif sex_upper in ['F', 'FEMALE']:
        return 'F'
    elif sex_upper in ['X', 'PREFER NOT TO SAY', 'N/A', 'OTHER']:
        return 'X'
    return sex_value  # Return original if no match

def normalize_sex_list(sex_list: list) -> list:
    """Normalize a list of sex values to M/F/X format."""
    if not sex_list:
        return []
    return [normalize_sex(s) for s in sex_list if normalize_sex(s)]

async def send_email_notification(email: str, subject: str, body: str):
    """Send email notification using SendGrid (mock if not configured)"""
    if not SENDGRID_API_KEY or not SENDGRID_FROM_EMAIL:
        logging.info(f"📧 [MOCK EMAIL] to {email}\nSubject: {subject}\nBody: {body[:100]}...")
        return False
    
    try:
        # SendGrid implementation will be added later
        # For now, log as mock
        logging.info(f"📧 [MOCK EMAIL] to {email}\nSubject: {subject}")
        return False
    except Exception as e:
        logging.error(f"❌ Email Error to {email}: {e}")
        return False

async def notify_previous_participants(walk: dict):
    """Notify users with matching city, sex, and age group about a new walk (Neighborhood is informational only)"""
    try:
        # Get walk location and conditions
        walk_city = walk.get("city", "")
        walk_conditions = walk.get("conditions", {}) or {}
        walk_sex_list = walk_conditions.get("sex", [])
        walk_age_groups = walk_conditions.get("age_groups", [])
        walk_organizer_id = walk.get("organizer_id", "")
        
        # Normalize walk sex requirements to handle both old (Male/Female) and new (M/F/X) formats
        walk_sex_normalized = normalize_sex_list(walk_sex_list)
        
        # Build list of all possible sex values that match (both old and new formats)
        sex_match_values = []
        for sex in walk_sex_normalized:
            if sex == 'M':
                sex_match_values.extend(['M', 'Male'])
            elif sex == 'F':
                sex_match_values.extend(['F', 'Female'])
            elif sex == 'X':
                sex_match_values.extend(['X', 'Prefer not to say', 'N/A', 'Other'])
        
        logging.info(f"📢 Sending notifications for new walk: {walk.get('title')}")
        logging.info(f"   Walk criteria: city={walk_city}, sex={walk_sex_list} (normalized: {walk_sex_normalized}), age_groups={walk_age_groups}")
        logging.info(f"   Sex match values for query: {sex_match_values}")
        logging.info("   Note: Neighborhood is informational only, not used for matching")
        logging.info(f"   Organizer ID (excluded): {walk_organizer_id}")
        
        # Build base query: filter by city ONLY (not neighborhood), with notifications enabled
        # EXCLUDE the organizer from notifications (they don't need to be notified about their own walk)
        base_query = {
            "notifications_enabled": True,
            "city": walk_city,
            "user_id": {"$ne": walk_organizer_id}  # Exclude organizer
        }
        
        # Add sex filter if walk has sex requirements (include all matching formats)
        if sex_match_values:
            base_query["sex"] = {"$in": sex_match_values}
        
        logging.info(f"   Query: {base_query}")
        
        # Get potential users matching location and sex
        potential_users = await db.users.find(base_query, {"_id": 0}).to_list(10000)
        logging.info(f"   Found {len(potential_users)} potential users matching location/sex")
        
        # Log potential users for debugging
        for u in potential_users:
            logging.info(f"   - Potential user: {u.get('name')}, age={u.get('age')}, sex={u.get('sex')}")
        
        # Further filter by age groups if walk has age requirements
        users = []
        if walk_age_groups:
            for user in potential_users:
                user_age = user.get("age")
                if user_age is None:
                    logging.info(f"   Skipping user {user.get('name')} - no age set")
                    continue  # Skip users without age set
                
                # Check if user's age falls within any of the required age groups
                # Non-overlapping ranges: 18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75+
                age_match = False
                for age_group in walk_age_groups:
                    if age_group == "18-24" and 18 <= user_age <= 24:
                        age_match = True
                    elif age_group == "25-34" and 25 <= user_age <= 34:
                        age_match = True
                    elif age_group == "35-44" and 35 <= user_age <= 44:
                        age_match = True
                    elif age_group == "45-54" and 45 <= user_age <= 54:
                        age_match = True
                    elif age_group == "55-64" and 55 <= user_age <= 64:
                        age_match = True
                    elif age_group == "65-74" and 65 <= user_age <= 74:
                        age_match = True
                    elif age_group == "75+" and user_age >= 75:
                        age_match = True
                
                if age_match:
                    users.append(user)
                    logging.info(f"   User {user.get('name')} age {user_age} matches age groups {walk_age_groups}")
                else:
                    logging.info(f"   User {user.get('name')} age {user_age} does NOT match age groups {walk_age_groups}")
        else:
            users = potential_users
        
        if not users:
            logging.info(f"❌ No users found matching city '{walk_city}', sex {walk_sex_list}, age groups {walk_age_groups} for notifications")
            return
        
        logging.info(f"   Final user count for notifications: {len(users)}")
        
        # Prepare notification message - Clear and attention-grabbing for 35-75 age group
        walk_date = walk.get("date", "TBD")
        walk_time = walk.get("time", "TBD")
        walk_city = walk.get("city", "TBD")
        walk_neighborhood = walk.get("neighborhood", "TBD")
        walk_starting_point = walk.get("starting_point", "TBD")
        organizer_name = walk.get("organizer_name", "")
        
        # Push notification - Clear title and body for 35-75 age group
        push_title = "🚶 New Walk Available!"
        push_body = f"{walk['title']} on {walk_date} at {walk_time}. Join {organizer_name}'s walk in {walk_city}!"
        push_data = {
            "type": "new_walk",
            "walk_id": walk.get("walk_id"),
            "screen": "/walk-details"
        }
        
        email_subject = f"New Walk: {walk['title']}"
        email_body = f"""
        <h2>New Walk Available!</h2>
        <p><strong>{walk['title']}</strong></p>
        <p><strong>Date:</strong> {walk_date}</p>
        <p><strong>Time:</strong> {walk_time}</p>
        <p><strong>City:</strong> {walk_city}</p>
        <p><strong>Neighborhood:</strong> {walk_neighborhood}</p>
        <p><strong>Starting Point:</strong> {walk_starting_point}</p>
        {f"<p><strong>Description:</strong> {walk.get('description', '')}</p>" if walk.get('description') else ''}
        {f"<p><strong>Conditions:</strong> {walk.get('conditions', '')}</p>" if walk.get('conditions') else ''}
        <p>Log in to book your spot!</p>
        """
        
        # Send Push Notifications to all matching users
        user_ids = [user["user_id"] for user in users]
        logging.info(f"   📲 Sending push notifications to {len(user_ids)} users")
        
        await send_push_to_users(user_ids, push_title, push_body, push_data)
        
        # Send email notifications (optional, as backup)
        for i, user in enumerate(users):
            try:
                logging.info(f"   Sending notification to {user.get('name')} ({user.get('email')})")
                
                if user.get("email_notifications", True):
                    await send_email_notification(user["email"], email_subject, email_body)
                
            except Exception as e:
                logging.error(f"Error sending notification to user {user['user_id']}: {e}")
        
        logging.info(f"✅ Sent notifications to {len(users)} users in {walk_city}, {walk_neighborhood} (sex: {walk_sex_list}, age: {walk_age_groups}) about new walk: {walk['title']}")
    except Exception as e:
        logging.error(f"❌ Error in notify_previous_participants: {e}")
        import traceback
        logging.error(traceback.format_exc())

async def send_walk_reminder(walk_id: str):
    """Send reminder to all participants of a walk (call this 1 day before walk)"""
    try:
        # Get walk details
        walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
        if not walk:
            return
        
        # Get all active bookings for this walk
        bookings = await db.bookings.find(
            {"walk_id": walk_id, "status": "active"},
            {"_id": 0}
        ).to_list(1000)
        
        if not bookings:
            return
        
        # Get unique user IDs
        user_ids = list(set(booking["user_id"] for booking in bookings))
        
        # Get user details with notifications enabled
        users = await db.users.find(
            {
                "user_id": {"$in": user_ids},
                "notifications_enabled": True
            },
            {"_id": 0}
        ).to_list(1000)
        
        # Prepare reminder message - Clear and attention-grabbing for 35-75 age group
        walk_date = walk.get("date", "TBD")
        walk_time = walk.get("time", "TBD")
        walk_city = walk.get("city", "TBD")
        walk_neighborhood = walk.get("neighborhood", "TBD")
        walk_starting_point = walk.get("starting_point", "TBD")
        
        # Push notification - Clear, direct, attention-grabbing for 35-75 age group
        push_title = "⏰ Walk Tomorrow - Don't Forget!"
        push_body = f"{walk['title']} at {walk_time}. Meet at {walk_starting_point}, {walk_neighborhood}. See you there!"
        push_data = {
            "type": "walk_reminder",
            "walk_id": walk_id,
            "screen": "/walk-details"
        }
        
        email_subject = f"Reminder: {walk['title']} Tomorrow"
        email_body = f"""
        <h2>Walk Reminder</h2>
        <p>This is a friendly reminder about your upcoming walk!</p>
        <p><strong>{walk['title']}</strong></p>
        <p><strong>Date:</strong> {walk_date}</p>
        <p><strong>Time:</strong> {walk_time}</p>
        <p><strong>City:</strong> {walk_city}</p>
        <p><strong>Neighborhood:</strong> {walk_neighborhood}</p>
        <p><strong>Starting Point:</strong> {walk_starting_point}</p>
        {f"<p><strong>Description:</strong> {walk.get('description', '')}</p>" if walk.get('description') else ''}
        {f"<p><strong>Conditions:</strong> {walk.get('conditions', '')}</p>" if walk.get('conditions') else ''}
        <p>We look forward to seeing you!</p>
        """
        
        # Send Push Notifications to all participants
        logging.info(f"   📲 Sending reminder push notifications to {len(user_ids)} users")
        await send_push_to_users(user_ids, push_title, push_body, push_data)
        
        # Send email reminders (as backup)
        for user in users:
            try:
                if user.get("email_notifications", True):
                    await send_email_notification(user["email"], email_subject, email_body)
            except Exception as e:
                logging.error(f"Error sending reminder to user {user['user_id']}: {e}")
        
        logging.info(f"✅ Sent reminders to {len(users)} participants for walk: {walk['title']}")
    except Exception as e:
        logging.error(f"Error in send_walk_reminder: {e}")


async def check_and_send_24h_reminders():
    """Check for walks happening in 24 hours and send reminders"""
    try:
        # Calculate the target date (tomorrow)
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)
        tomorrow_date = tomorrow.strftime("%Y-%m-%d")
        
        logging.info(f"🔔 Checking for walks on {tomorrow_date} to send 24h reminders...")
        
        # Find all walks happening tomorrow
        walks = await db.walks.find(
            {"date": tomorrow_date},
            {"_id": 0}
        ).to_list(1000)
        
        if not walks:
            logging.info(f"No walks scheduled for {tomorrow_date}")
            return
        
        logging.info(f"Found {len(walks)} walk(s) scheduled for tomorrow")
        
        for walk in walks:
            # Check if reminder was already sent (to avoid duplicate reminders)
            reminder_key = f"reminder_sent_{walk['walk_id']}_{tomorrow_date}"
            existing_reminder = await db.reminders_sent.find_one({"key": reminder_key})
            
            if existing_reminder:
                logging.info(f"Reminder already sent for walk: {walk['title']}")
                continue
            
            # Send reminders
            await send_walk_reminder(walk['walk_id'])
            
            # Mark reminder as sent
            await db.reminders_sent.insert_one({
                "key": reminder_key,
                "walk_id": walk['walk_id'],
                "sent_at": datetime.now(timezone.utc)
            })
            
            logging.info(f"✅ 24h reminder sent for walk: {walk['title']}")
        
    except Exception as e:
        logging.error(f"Error in check_and_send_24h_reminders: {e}")


# ============== Retention Notification System ==============

import random

# Motivational messages for retention notifications
RETENTION_MESSAGES = [
    {"title": "👋 We miss you!", "body": "Your walking community is waiting. Join a local walk today!"},
    {"title": "🚶 Time to walk!", "body": "New walks are available near you. Come explore!"},
    {"title": "💪 Keep moving!", "body": "Walking is better together. Find your group today!"},
    {"title": "🌟 Don't miss out!", "body": "Join local walking groups and stay connected."},
    {"title": "🤝 We grow together!", "body": "Your walking buddies miss you. Come back and join a walk!"},
]

async def get_weather_message_for_user(user: dict) -> Optional[str]:
    """Get weather-based message for a user based on their city"""
    try:
        # Try to get user's city from their profile or last booking
        city = user.get('city')
        
        if not city:
            # Try to get from their most recent booking or walk
            recent_booking = await db.bookings.find_one(
                {"user_id": user["user_id"]},
                sort=[("booked_at", -1)]
            )
            if recent_booking and recent_booking.get("walk_id"):
                walk = await db.walks.find_one({"walk_id": recent_booking["walk_id"]})
                if walk:
                    city = walk.get("city")
        
        if not city:
            # Try from walks they organized
            recent_walk = await db.walks.find_one(
                {"organizer_id": user["user_id"]},
                sort=[("created_at", -1)]
            )
            if recent_walk:
                city = recent_walk.get("city")
        
        if city:
            # Get weather for tomorrow
            tomorrow = (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d")
            coords = await get_coordinates(city)
            
            if coords:
                weather = await get_weather_for_date(coords["latitude"], coords["longitude"], tomorrow)
                
                if weather and not weather.get("unavailable"):
                    temp = weather.get("temp_max")
                    icon = weather.get("icon", "🌤️")
                    
                    # Good weather conditions (sunny, partly cloudy, clear)
                    good_weather_codes = [0, 1, 2, 3]  # Clear, mainly clear, partly cloudy
                    weather_code = weather.get("weather_code", 99)
                    
                    if weather_code in good_weather_codes and temp and temp > 10:
                        return f"{icon} Beautiful {temp}°C weather for a walk tomorrow in {city}!"
        
    except Exception as e:
        logging.error(f"Error getting weather message: {e}")
    
    return None


async def get_city_stats(city: str) -> Optional[int]:
    """Get number of people who walked in a city recently"""
    try:
        # Count bookings in the last 7 days for walks in this city
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        
        # Get walks in this city
        city_walks = await db.walks.find({"city": city}).to_list(length=100)
        walk_ids = [w["walk_id"] for w in city_walks]
        
        if walk_ids:
            # Count unique users who booked these walks
            bookings = await db.bookings.find({
                "walk_id": {"$in": walk_ids},
                "booked_at": {"$gte": week_ago}
            }).to_list(length=1000)
            
            unique_users = set(b["user_id"] for b in bookings)
            return len(unique_users)
    
    except Exception as e:
        logging.error(f"Error getting city stats: {e}")
    
    return None


async def send_retention_notification(user: dict):
    """Send a retention push notification to an inactive user"""
    try:
        user_id = user["user_id"]
        
        # Get user's push token
        push_token_doc = await db.push_tokens.find_one({"user_id": user_id})
        
        if not push_token_doc or not push_token_doc.get("push_token"):
            logging.info(f"No push token for user {user_id}, skipping retention notification")
            return False
        
        push_token = push_token_doc["push_token"]
        
        # Check if we already sent a retention notification recently (within 3 days)
        existing = await db.retention_notifications.find_one({
            "user_id": user_id,
            "sent_at": {"$gte": datetime.now(timezone.utc) - timedelta(days=3)}
        })
        
        if existing:
            logging.info(f"Retention notification already sent recently to {user_id}")
            return False
        
        # Build personalized message
        title = None
        body = None
        
        # Try weather-based message first (Phase 2)
        weather_message = await get_weather_message_for_user(user)
        if weather_message:
            title = "☀️ Perfect walking weather!"
            body = weather_message
        
        # Try city stats message
        if not title:
            city = None
            recent_walk = await db.walks.find_one(
                {"organizer_id": user_id},
                sort=[("created_at", -1)]
            )
            if recent_walk:
                city = recent_walk.get("city")
            
            if city:
                city_walkers = await get_city_stats(city)
                if city_walkers and city_walkers > 5:
                    title = f"🚶 {city_walkers} people walked in {city} this week!"
                    body = "Join them and discover new walking routes."
        
        # Fall back to random motivational message
        if not title:
            msg = random.choice(RETENTION_MESSAGES)
            title = msg["title"]
            body = msg["body"]
        
        # Personalize with user's name
        user_name = user.get("name", "").split()[0] if user.get("name") else None
        if user_name and len(user_name) > 1:
            body = f"Hey {user_name}! {body}"
        
        # Send the notification
        try:
            push_response = push_client.publish(
                PushMessage(
                    to=push_token,
                    title=title,
                    body=body,
                    data={"type": "retention", "user_id": user_id}
                )
            )
            
            # Record that we sent this notification
            await db.retention_notifications.insert_one({
                "user_id": user_id,
                "title": title,
                "body": body,
                "sent_at": datetime.now(timezone.utc),
                "response": str(push_response)
            })
            
            logging.info(f"✅ Retention notification sent to {user_id}: {title}")
            return True
            
        except DeviceNotRegisteredError:
            # Remove invalid token
            await db.push_tokens.delete_one({"user_id": user_id})
            logging.warning(f"Removed invalid push token for user {user_id}")
            return False
        except PushServerError as e:
            logging.error(f"Push server error for {user_id}: {e}")
            return False
            
    except Exception as e:
        logging.error(f"Error sending retention notification: {e}")
        return False


async def check_and_send_retention_notifications():
    """Check for inactive users and send retention notifications (runs daily at 5 PM)"""
    try:
        logging.info("🔔 Checking for inactive users to send retention notifications...")
        
        # Find users who haven't logged in for 3+ days
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        
        # Find inactive users with notifications enabled
        inactive_users = await db.users.find({
            "$or": [
                {"lastLoginAt": {"$lt": three_days_ago}},
                {"lastLoginAt": {"$exists": False}}
            ],
            "notifications_enabled": {"$ne": False}  # Include users where it's True or not set
        }).to_list(length=100)  # Limit to 100 per run to avoid overload
        
        logging.info(f"Found {len(inactive_users)} inactive user(s)")
        
        sent_count = 0
        for user in inactive_users:
            success = await send_retention_notification(user)
            if success:
                sent_count += 1
            
            # Add small delay between notifications
            await asyncio.sleep(0.5)
        
        logging.info(f"✅ Sent {sent_count} retention notifications")
        
    except Exception as e:
        logging.error(f"Error in check_and_send_retention_notifications: {e}")


def start_reminder_scheduler():
    """Start the background scheduler for 24h reminders and retention notifications"""
    try:
        # Run every hour to check for walks that need reminders
        scheduler.add_job(
            check_and_send_24h_reminders,
            IntervalTrigger(hours=1),
            id='reminder_job',
            name='24h Walk Reminder',
            replace_existing=True
        )
        
        # Run daily at 5 PM UTC (adjust for your timezone if needed)
        from apscheduler.triggers.cron import CronTrigger
        scheduler.add_job(
            check_and_send_retention_notifications,
            CronTrigger(hour=17, minute=0),  # 5 PM
            id='retention_job',
            name='Retention Notifications',
            replace_existing=True
        )
        
        scheduler.start()
        logging.info("✅ 24h reminder scheduler started (runs every hour)")
        logging.info("✅ Retention notification scheduler started (runs daily at 5 PM)")
    except Exception as e:
        logging.error(f"❌ Failed to start reminder scheduler: {e}")


# ============== Pydantic Models ==============

# Email/Password Auth Models
class UserRegister(BaseModel):
    email: str
    password: str
    name: str

class UserLogin(BaseModel):
    email: str
    password: str

class ForgotPasswordRequest(BaseModel):
    email: str

class ResetPasswordRequest(BaseModel):
    email: str
    reset_code: str
    new_password: str

# GeoNames configuration
GEONAMES_USERNAME = os.getenv("GEONAMES_USERNAME", "demo")  # Replace with actual username

class User(BaseModel):
    user_id: str
    email: str
    name: str
    pseudonym: Optional[str] = None  # Display name for bookings and feedback
    picture: Optional[str] = None
    role: str = "user"  # "admin" or "user"
    age: Optional[int] = None
    sex: Optional[str] = None  # "M", "F", or "X"
    country_of_birth: Optional[str] = None
    country: Optional[str] = None  # Country from GeoNames
    country_code: Optional[str] = None  # ISO country code
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    notifications_enabled: bool = True
    email_notifications: bool = True
    created_at: datetime
    status: str = "active"  # "active", "suspended", "blocked"
    terms_accepted_at: Optional[datetime] = None  # For legal re-acceptance tracking

class SessionDataResponse(BaseModel):
    id: str
    email: str
    name: str
    picture: Optional[str] = None
    session_token: str

class WalkConditions(BaseModel):
    sex: Optional[List[str]] = None  # ["Male", "Female"] - can be both or one
    age_groups: Optional[List[str]] = None  # ["18-24", "25-34", etc.] - non-overlapping ranges
    country_region: Optional[str] = None  # Free text for country/region

class Walk(BaseModel):
    walk_id: str
    title: str
    date: str  # YYYY-MM-DD format
    time: str  # HH:MM format
    city: str
    neighborhood: str
    starting_point: str
    ending_point: Optional[str] = None  # Defaults to starting_point if not provided
    duration_hours: Optional[float] = None  # Approximate duration in hours
    max_participants: int = 10  # Maximum number of participants
    description: Optional[str] = None
    conditions: Optional[dict] = None  # Structured conditions (sex, age_groups, country_region)
    organizer_id: str  # User who created/organizes this walk
    organizer_name: str  # Name of the organizer
    created_at: datetime
    updated_at: datetime

class WalkCreate(BaseModel):
    title: str
    date: str
    time: str
    city: str
    neighborhood: str
    starting_point: str
    ending_point: Optional[str] = None  # Defaults to starting_point if not provided
    duration_hours: Optional[float] = None  # Approximate duration in hours
    max_participants: int = 10  # Maximum number of participants, default 10
    description: Optional[str] = None
    conditions: Optional[dict] = None

class WalkUpdate(BaseModel):
    title: Optional[str] = None
    date: Optional[str] = None
    time: Optional[str] = None
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    starting_point: Optional[str] = None
    ending_point: Optional[str] = None
    duration_hours: Optional[float] = None
    max_participants: Optional[int] = None
    description: Optional[str] = None
    conditions: Optional[dict] = None

class Booking(BaseModel):
    booking_id: str
    walk_id: str
    user_id: str
    user_name: str
    user_email: str
    booked_at: datetime
    status: str = "active"  # "active" or "cancelled"
    # Enhanced fields - populated by get_my_bookings endpoint
    walk_title: Optional[str] = None
    walk_date: Optional[str] = None

class BookingCreate(BaseModel):
    walk_id: str

class ParticipantInfo(BaseModel):
    user_id: str
    name: str
    pseudonym: Optional[str] = None  # Display name instead of real name
    email: str
    picture: Optional[str] = None
    age: Optional[int] = None
    sex: Optional[str] = None
    country_of_birth: Optional[str] = None
    booked_at: datetime
    badge_level: Optional[int] = None
    total_walks: Optional[int] = None

class FeedbackReply(BaseModel):
    reply_id: str
    feedback_id: str
    from_user_id: str
    from_user_name: str
    message: str
    created_at: datetime

class Feedback(BaseModel):
    feedback_id: str
    walk_id: str
    walk_title: str
    from_user_id: str
    from_user_name: str
    to_organizer_id: str
    to_organizer_name: str
    message: str
    created_at: datetime
    replies: Optional[List[FeedbackReply]] = []

class FeedbackCreate(BaseModel):
    walk_id: str
    message: str

class FeedbackReplyCreate(BaseModel):
    message: str

# ============== Walker Experience Models ==============

class WalkReview(BaseModel):
    review_id: str
    walk_id: str
    walk_title: str
    user_id: str
    user_name: str
    rating: int  # 1-5 stars
    comment: Optional[str] = None
    created_at: datetime

class WalkReviewCreate(BaseModel):
    walk_id: str
    rating: int  # 1-5 stars
    comment: Optional[str] = None

class WalkerExperience(BaseModel):
    experience_id: str
    user_id: str
    user_name: str
    user_pseudonym: Optional[str] = None
    user_city: Optional[str] = None
    user_picture: Optional[str] = None
    walk_id: Optional[str] = None
    walk_title: Optional[str] = None
    text: str
    photo: Optional[str] = None  # base64 encoded image
    video: Optional[str] = None  # base64 encoded video
    created_at: datetime

class WalkerExperienceCreate(BaseModel):
    walk_id: Optional[str] = None
    text: str
    photo: Optional[str] = None  # base64 encoded image
    video: Optional[str] = None  # base64 encoded video

class AdminMessage(BaseModel):
    message: str

class ContentReport(BaseModel):
    content_type: str  # "feedback", "experience", "walk"
    content_id: Optional[str] = None
    reported_user_name: Optional[str] = None
    walk_title: Optional[str] = None
    walk_date: Optional[str] = None
    description: str  # User's description of the issue

class UserProfileUpdate(BaseModel):
    pseudonym: Optional[str] = None  # Display name for bookings and feedback
    age: Optional[int] = None
    sex: Optional[str] = None  # "M", "F", or "X"
    country_of_birth: Optional[str] = None
    country: Optional[str] = None  # Country from GeoNames
    country_code: Optional[str] = None  # ISO country code
    city: Optional[str] = None
    neighborhood: Optional[str] = None
    picture: Optional[str] = None
    notifications_enabled: Optional[bool] = None
    email_notifications: Optional[bool] = None
    terms_accepted: Optional[bool] = None  # Has user accepted terms
    terms_accepted_at: Optional[datetime] = None  # When user last accepted terms

class PushTokenRegister(BaseModel):
    push_token: str
    platform: str  # 'ios' or 'android'


# ============== Auth Dependencies ==============

async def get_session_token(request: Request) -> Optional[str]:
    """Extract session token from cookie or Authorization header"""
    # Try cookie first
    session_token = request.cookies.get("session_token")
    if session_token:
        return session_token
    
    # Fallback to Authorization header
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        return auth_header.replace("Bearer ", "")
    
    return None

async def get_current_user(request: Request) -> User:
    """Get current authenticated user"""
    session_token = await get_session_token(request)
    if not session_token:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    # Check session
    session = await db.user_sessions.find_one(
        {"session_token": session_token}, 
        {"_id": 0}
    )
    if not session:
        raise HTTPException(status_code=401, detail="Invalid session")
    
    # Check expiry with timezone awareness
    expires_at = session["expires_at"]
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Session expired")
    
    # Get user
    user_doc = await db.users.find_one(
        {"user_id": session["user_id"]}, 
        {"_id": 0}
    )
    if not user_doc:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Check if user is suspended or blocked
    user_status = user_doc.get("status", "active")
    if user_status == "blocked":
        raise HTTPException(status_code=403, detail="Your account has been blocked. Please contact support.")
    if user_status == "suspended":
        raise HTTPException(status_code=403, detail="Your account has been temporarily suspended. Please contact support.")
    
    return User(**user_doc)

async def require_admin(current_user: User = Depends(get_current_user)) -> User:
    """Require admin role"""
    if current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Admin access required")
    return current_user


# ============== Auth Endpoints ==============

@api_router.post("/auth/session")
async def create_session(request: Request, response: Response):
    """Exchange session_id for user data and create session"""
    try:
        # Get session_id from request body
        body = await request.json()
        session_id = body.get("session_id")
        
        if not session_id:
            raise HTTPException(status_code=400, detail="session_id required")
        
        # Exchange session_id for user data
        async with httpx.AsyncClient() as client:
            auth_response = await client.get(
                "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data",
                headers={"X-Session-ID": session_id},
                timeout=10.0
            )
            
            if auth_response.status_code != 200:
                raise HTTPException(status_code=401, detail="Invalid session_id")
            
            user_data = auth_response.json()
        
        # Create or get user
        user_id = user_data["id"]
        existing_user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        
        if not existing_user:
            # Create new user
            new_user = {
                "user_id": user_id,
                "email": user_data["email"],
                "name": user_data["name"],
                "picture": user_data.get("picture"),
                "role": "user",  # Default role
                "created_at": datetime.now(timezone.utc),
                "lastLoginAt": datetime.now(timezone.utc)
            }
            await db.users.insert_one(new_user)
            user_doc = new_user
        else:
            user_doc = existing_user
            # Update last login timestamp
            await db.users.update_one(
                {"user_id": user_id},
                {"$set": {"lastLoginAt": datetime.now(timezone.utc)}}
            )
        
        # Create session
        session_token = user_data["session_token"]
        session = {
            "user_id": user_id,
            "session_token": session_token,
            "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
            "created_at": datetime.now(timezone.utc)
        }
        await db.user_sessions.insert_one(session)
        
        # Set cookie
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=7*24*60*60,
            path="/"
        )
        
        # Return user data with session_token for mobile clients
        user_response = User(**user_doc).dict()
        user_response["session_token"] = session_token
        return user_response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Session creation error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error")

@api_router.get("/auth/me")
async def get_me(current_user: User = Depends(get_current_user)):
    """Get current user info"""
    return current_user

@api_router.post("/auth/logout")
async def logout(request: Request, response: Response):
    """Logout user"""
    session_token = await get_session_token(request)
    if session_token:
        await db.user_sessions.delete_one({"session_token": session_token})
    
    response.delete_cookie("session_token", path="/")
    return {"message": "Logged out successfully"}


# ============== Apple Sign-In Endpoint ==============

class AppleAuthRequest(BaseModel):
    identity_token: str
    user_id: str
    email: Optional[str] = None
    full_name: Optional[str] = None

@api_router.post("/auth/apple")
async def apple_auth(apple_data: AppleAuthRequest, response: Response):
    """Handle Apple Sign-In authentication"""
    try:
        import jwt
        
        # Decode the identity token (without verification for now - in production you should verify with Apple's public keys)
        # The token contains user info from Apple
        try:
            decoded = jwt.decode(apple_data.identity_token, options={"verify_signature": False})
            apple_user_id = decoded.get("sub")
            email = apple_data.email or decoded.get("email")
        except Exception as e:
            logging.error(f"Apple token decode error: {e}")
            raise HTTPException(status_code=401, detail="Invalid Apple identity token")
        
        if not apple_user_id:
            raise HTTPException(status_code=401, detail="Invalid Apple user ID")
        
        # Create a unique user_id for Apple users
        user_id = f"apple_{apple_user_id}"
        
        # Check if user exists
        existing_user = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        
        if not existing_user:
            # Create new user
            # Apple only provides name on first sign-in, so we might not have it
            name = apple_data.full_name or email.split("@")[0] if email else "Apple User"
            
            new_user = {
                "user_id": user_id,
                "email": email or f"{user_id}@privaterelay.appleid.com",
                "name": name,
                "pseudonym": name,
                "picture": None,
                "role": "user",
                "auth_type": "apple",
                "notifications_enabled": True,
                "email_notifications": True,
                "created_at": datetime.now(timezone.utc),
                "lastLoginAt": datetime.now(timezone.utc)
            }
            await db.users.insert_one(new_user)
            user_doc = new_user
            logging.info(f"New Apple user created: {user_id}")
        else:
            user_doc = existing_user
            # Update last login timestamp
            await db.users.update_one(
                {"user_id": user_id},
                {"$set": {"lastLoginAt": datetime.now(timezone.utc)}}
            )
            logging.info(f"Existing Apple user logged in: {user_id}")
        
        # Create session
        session_token = str(uuid.uuid4())
        session = {
            "user_id": user_id,
            "session_token": session_token,
            "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
            "created_at": datetime.now(timezone.utc)
        }
        await db.user_sessions.insert_one(session)
        
        # Set cookie
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=7*24*60*60,
            path="/"
        )
        
        # Return user data with session_token for mobile clients
        user_response = User(**user_doc).dict()
        user_response["session_token"] = session_token
        return user_response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Apple auth error: {e}")
        raise HTTPException(status_code=500, detail="Apple Sign-In failed")



# ============== Email/Password Auth Endpoints ==============

def hash_password(password: str) -> str:
    """Hash a password using bcrypt"""
    salt = bcrypt.gensalt()
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    """Verify a password against its hash"""
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def is_valid_email(email: str) -> bool:
    """Validate email format"""
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    return re.match(pattern, email) is not None

@api_router.post("/auth/register")
async def register_user(user_data: UserRegister, response: Response):
    """Register a new user with email and password"""
    try:
        # Validate email format
        if not is_valid_email(user_data.email):
            raise HTTPException(status_code=400, detail="Invalid email format")
        
        # Check password length
        if len(user_data.password) < 6:
            raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
        
        # Check if name is provided
        if not user_data.name or len(user_data.name.strip()) < 2:
            raise HTTPException(status_code=400, detail="Name must be at least 2 characters")
        
        # Check if email already exists
        existing_user = await db.users.find_one({"email": user_data.email.lower()})
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already registered")
        
        # Create user
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        hashed_password = hash_password(user_data.password)
        
        new_user = {
            "user_id": user_id,
            "email": user_data.email.lower(),
            "name": user_data.name.strip(),
            "password_hash": hashed_password,
            "auth_type": "email",  # To distinguish from Google OAuth users
            "picture": None,
            "role": "user",
            "notifications_enabled": True,
            "email_notifications": True,
            "created_at": datetime.now(timezone.utc)
        }
        
        await db.users.insert_one(new_user)
        
        # Create session
        session_token = f"sess_{uuid.uuid4().hex}"
        session = {
            "user_id": user_id,
            "session_token": session_token,
            "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
            "created_at": datetime.now(timezone.utc)
        }
        await db.user_sessions.insert_one(session)
        
        # Set cookie
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=7*24*60*60,
            path="/"
        )
        
        # Return user without password_hash and _id
        user_response = {k: v for k, v in new_user.items() if k not in ["password_hash", "_id"]}
        
        logging.info(f"✅ New user registered: {user_data.email}")
        return user_response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Registration error: {e}")
        raise HTTPException(status_code=500, detail="Registration failed")

@api_router.post("/auth/login")
async def login_user(user_data: UserLogin, response: Response):
    """Login user with email and password"""
    try:
        # Find user by email
        user = await db.users.find_one({"email": user_data.email.lower()}, {"_id": 0})
        
        if not user:
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        # Check if user has password (email auth user)
        if "password_hash" not in user:
            raise HTTPException(
                status_code=401, 
                detail="This account uses Google Sign-In. Please use Google to login."
            )
        
        # Verify password
        if not verify_password(user_data.password, user["password_hash"]):
            raise HTTPException(status_code=401, detail="Invalid email or password")
        
        # Update last login timestamp
        await db.users.update_one(
            {"user_id": user["user_id"]},
            {"$set": {"lastLoginAt": datetime.now(timezone.utc)}}
        )
        
        # Create session
        session_token = f"sess_{uuid.uuid4().hex}"
        session = {
            "user_id": user["user_id"],
            "session_token": session_token,
            "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
            "created_at": datetime.now(timezone.utc)
        }
        await db.user_sessions.insert_one(session)
        
        # Set cookie
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=True,
            secure=True,
            samesite="none",
            max_age=7*24*60*60,
            path="/"
        )
        
        # Return user without password_hash
        user_response = {k: v for k, v in user.items() if k != "password_hash"}
        
        logging.info(f"✅ User logged in: {user_data.email}")
        return user_response
        
    except HTTPException:
        raise
    except Exception as e:
        logging.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail="Login failed")


@api_router.post("/auth/make-admin")
async def make_admin(email: str, current_user: User = Depends(get_current_user)):
    """Make a user admin (for testing - first user can promote others)"""
    # Update user role
    result = await db.users.update_one(
        {"email": email},
        {"$set": {"role": "admin"}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    return {"message": f"User {email} is now an admin"}


@api_router.delete("/admin/cleanup-old-data")
async def cleanup_old_data(current_user: User = Depends(get_current_user)):
    """
    Admin endpoint to clean up old test data:
    - Delete walks created before Feb 10, 2025 by specific test users
    - Delete walks scheduled for 2026 (test data)
    - Delete associated bookings
    Only accessible by admin users.
    """
    # Check if user is admin
    if current_user.email not in ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin access required")
    
    test_user_emails = [
        "abdelkaderbeldjoudi@gmail.com",
        "abdelkaderbeldjoudi8@gmail.com", 
        "kbeldjoudi@hotmail.com"
    ]
    
    # Get user IDs for test users
    test_users = await db.users.find({"email": {"$in": test_user_emails}}).to_list(10)
    test_user_ids = [str(u["_id"]) for u in test_users] + [u.get("google_id") for u in test_users if u.get("google_id")]
    
    results = {
        "walks_deleted": 0,
        "bookings_deleted": 0,
        "details": []
    }
    
    # 1. Find walks to delete: created before Feb 10, 2025 by test users
    cutoff_date = datetime(2025, 2, 10)
    
    old_walks_query = {
        "$and": [
            {"organizer_id": {"$in": test_user_ids}},
            {"created_at": {"$lt": cutoff_date}}
        ]
    }
    
    old_walks = await db.walks.find(old_walks_query).to_list(1000)
    old_walk_ids = [w["walk_id"] for w in old_walks]
    
    if old_walk_ids:
        # Delete associated bookings first
        bookings_result = await db.bookings.delete_many({"walk_id": {"$in": old_walk_ids}})
        results["bookings_deleted"] += bookings_result.deleted_count
        
        # Delete the walks
        walks_result = await db.walks.delete_many({"walk_id": {"$in": old_walk_ids}})
        results["walks_deleted"] += walks_result.deleted_count
        results["details"].append(f"Deleted {walks_result.deleted_count} walks created before Feb 10, 2025")
    
    # 2. Find walks scheduled for 2026 (test data with wrong year)
    walks_2026_query = {
        "date": {"$regex": "^2026-"}
    }
    
    walks_2026 = await db.walks.find(walks_2026_query).to_list(1000)
    walks_2026_ids = [w["walk_id"] for w in walks_2026]
    
    if walks_2026_ids:
        # Delete associated bookings first
        bookings_result = await db.bookings.delete_many({"walk_id": {"$in": walks_2026_ids}})
        results["bookings_deleted"] += bookings_result.deleted_count
        
        # Delete the walks
        walks_result = await db.walks.delete_many({"walk_id": {"$in": walks_2026_ids}})
        results["walks_deleted"] += walks_result.deleted_count
        results["details"].append(f"Deleted {walks_result.deleted_count} walks scheduled for 2026")
    
    # 3. Clean up orphaned bookings (bookings where walk no longer exists)
    all_walk_ids = await db.walks.distinct("walk_id")
    orphaned_bookings = await db.bookings.delete_many({"walk_id": {"$nin": all_walk_ids}})
    if orphaned_bookings.deleted_count > 0:
        results["bookings_deleted"] += orphaned_bookings.deleted_count
        results["details"].append(f"Deleted {orphaned_bookings.deleted_count} orphaned bookings")
    
    logging.info(f"[Cleanup] Admin {current_user.email} cleaned up data: {results}")
    
    return {
        "success": True,
        "message": f"Cleanup complete: {results['walks_deleted']} walks and {results['bookings_deleted']} bookings deleted",
        "details": results
    }

@api_router.post("/auth/forgot-password")
async def forgot_password(request: ForgotPasswordRequest):
    """Request a password reset - generates a 6-digit code"""
    import random
    import string
    
    email = request.email.lower().strip()
    logging.info(f"[ForgotPassword] Looking up user: {email}")
    
    # Check if user exists with email/password auth
    user = await db.users.find_one({"email": email})
    logging.info(f"[ForgotPassword] User found: {user is not None}")
    
    if not user:
        # Don't reveal if email exists or not for security
        logging.info(f"[ForgotPassword] User not found, returning generic message")
        return {"message": "If an account exists with this email, a reset code has been generated."}
    
    # Check if user has password auth (not just Google/Apple)
    has_password = "password_hash" in user
    logging.info(f"[ForgotPassword] Has password_hash: {has_password}")
    
    if not has_password:
        return {"message": "This account uses Google or Apple Sign-In. Please use that method to log in."}
    
    # Generate 6-digit reset code
    reset_code = ''.join(random.choices(string.digits, k=6))
    
    # Store reset code with expiration (15 minutes)
    await db.password_resets.update_one(
        {"email": email},
        {
            "$set": {
                "email": email,
                "reset_code": reset_code,
                "created_at": datetime.now(timezone.utc),
                "expires_at": datetime.now(timezone.utc) + timedelta(minutes=15),
                "used": False
            }
        },
        upsert=True
    )
    
    logging.info(f"Password reset code generated for {email}: {reset_code}")
    
    # In production, you would send this via email
    # For now, we return it (you can set up email later)
    return {
        "message": "Reset code generated. Check your email.",
        "reset_code": reset_code,  # Remove this line in production when email is set up
        "expires_in_minutes": 15
    }

@api_router.post("/auth/reset-password")
async def reset_password(request: ResetPasswordRequest):
    """Reset password using the reset code"""
    email = request.email.lower().strip()
    
    # Find the reset request
    reset_request = await db.password_resets.find_one({
        "email": email,
        "reset_code": request.reset_code,
        "used": False
    })
    
    if not reset_request:
        raise HTTPException(status_code=400, detail="Invalid or expired reset code")
    
    # Check if code has expired
    if datetime.now(timezone.utc) > reset_request["expires_at"]:
        raise HTTPException(status_code=400, detail="Reset code has expired. Please request a new one.")
    
    # Validate new password
    if len(request.new_password) < 6:
        raise HTTPException(status_code=400, detail="Password must be at least 6 characters")
    
    # Hash new password
    new_password_hash = hash_password(request.new_password)
    
    # Update user password
    result = await db.users.update_one(
        {"email": email},
        {"$set": {"password_hash": new_password_hash}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Mark reset code as used
    await db.password_resets.update_one(
        {"_id": reset_request["_id"]},
        {"$set": {"used": True}}
    )
    
    logging.info(f"Password reset successful for {email}")
    
    return {"message": "Password has been reset successfully. You can now log in with your new password."}

@api_router.put("/auth/profile")
async def update_profile(
    profile_data: UserProfileUpdate,
    current_user: User = Depends(get_current_user)
):
    """Update user profile information"""
    update_dict = {k: v for k, v in profile_data.dict().items() if v is not None}
    
    if not update_dict:
        raise HTTPException(status_code=400, detail="No update data provided")
    
    result = await db.users.update_one(
        {"user_id": current_user.user_id},
        {"$set": update_dict}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Return updated user
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"_id": 0})
    return User(**user_doc)


# ============== Account Cancellation ==============

class AccountCancellation(BaseModel):
    reason: str
    improvement_suggestion: Optional[str] = None

@api_router.post("/account/cancel")
async def cancel_account(
    cancellation: AccountCancellation,
    current_user: User = Depends(get_current_user)
):
    """Cancel user account (soft delete - keeps data for statistics)"""
    from datetime import datetime, timezone
    
    # Store cancellation record
    cancellation_record = {
        "user_id": current_user.user_id,
        "email": current_user.email,
        "name": current_user.name,
        "reason": cancellation.reason,
        "improvement_suggestion": cancellation.improvement_suggestion,
        "cancelled_at": datetime.now(timezone.utc),
    }
    
    await db.account_cancellations.insert_one(cancellation_record)
    
    # Update user as cancelled (soft delete)
    await db.users.update_one(
        {"user_id": current_user.user_id},
        {
            "$set": {
                "is_cancelled": True,
                "cancelled_at": datetime.now(timezone.utc),
                "cancellation_reason": cancellation.reason,
                # Anonymize personal data but keep walking stats
                "email": f"cancelled_{current_user.user_id}@deleted.local",
                "name": "Deleted User",
                "pseudonym": "Deleted User",
                "picture": None,
            }
        }
    )
    
    # Log the cancellation for admin notification
    try:
        logging.info("📧 ACCOUNT CANCELLATION NOTIFICATION")
        logging.info(f"   User: {current_user.name} ({current_user.email})")
        logging.info(f"   Reason: {cancellation.reason}")
        logging.info(f"   Improvement Suggestion: {cancellation.improvement_suggestion or 'None provided'}")
        logging.info(f"   Cancelled at: {datetime.now(timezone.utc)}")
        
        # Send push notification to admin users
        admin_users = await db.users.find({"is_admin": True}).to_list(10)
        admin_tokens = []
        for admin in admin_users:
            tokens = await db.push_tokens.find({"user_id": admin.get("user_id")}).to_list(10)
            admin_tokens.extend([t["push_token"] for t in tokens])
        
        if admin_tokens:
            await send_push_notification(
                admin_tokens,
                "Account Cancelled",
                f"User {current_user.name} cancelled their account. Reason: {cancellation.reason}"
            )
            logging.info("   Push notification sent to admins")
    except Exception as e:
        logging.error(f"Failed to send admin notification: {e}")
    
    return {"message": "Account cancelled successfully", "user_id": current_user.user_id}


# ============== Weather API ==============

# Weather code to icon/description mapping (WMO Weather interpretation codes)
WEATHER_CODES = {
    0: {"icon": "☀️", "description": "Clear sky"},
    1: {"icon": "🌤️", "description": "Mainly clear"},
    2: {"icon": "⛅", "description": "Partly cloudy"},
    3: {"icon": "☁️", "description": "Overcast"},
    45: {"icon": "🌫️", "description": "Foggy"},
    48: {"icon": "🌫️", "description": "Icy fog"},
    51: {"icon": "🌧️", "description": "Light drizzle"},
    53: {"icon": "🌧️", "description": "Drizzle"},
    55: {"icon": "🌧️", "description": "Heavy drizzle"},
    61: {"icon": "🌧️", "description": "Light rain"},
    63: {"icon": "🌧️", "description": "Rain"},
    65: {"icon": "🌧️", "description": "Heavy rain"},
    66: {"icon": "🌨️", "description": "Freezing rain"},
    67: {"icon": "🌨️", "description": "Heavy freezing rain"},
    71: {"icon": "🌨️", "description": "Light snow"},
    73: {"icon": "🌨️", "description": "Snow"},
    75: {"icon": "❄️", "description": "Heavy snow"},
    77: {"icon": "🌨️", "description": "Snow grains"},
    80: {"icon": "🌦️", "description": "Light showers"},
    81: {"icon": "🌦️", "description": "Showers"},
    82: {"icon": "⛈️", "description": "Heavy showers"},
    85: {"icon": "🌨️", "description": "Snow showers"},
    86: {"icon": "🌨️", "description": "Heavy snow showers"},
    95: {"icon": "⛈️", "description": "Thunderstorm"},
    96: {"icon": "⛈️", "description": "Thunderstorm with hail"},
    99: {"icon": "⛈️", "description": "Thunderstorm with heavy hail"},
}

# Cache for geocoding results to avoid repeated API calls
geocode_cache = {}

async def get_coordinates(city: str, country: str = None) -> Optional[dict]:
    """Get latitude and longitude for a city using Open-Meteo Geocoding API"""
    cache_key = f"{city}_{country or ''}"
    
    if cache_key in geocode_cache:
        return geocode_cache[cache_key]
    
    try:
        search_query = f"{city}, {country}" if country else city
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://geocoding-api.open-meteo.com/v1/search",
                params={"name": search_query, "count": 1, "language": "en", "format": "json"},
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                if data.get("results") and len(data["results"]) > 0:
                    result = data["results"][0]
                    coords = {
                        "latitude": result["latitude"],
                        "longitude": result["longitude"],
                        "name": result.get("name", city)
                    }
                    geocode_cache[cache_key] = coords
                    return coords
    except Exception as e:
        logging.error(f"Geocoding error for {city}: {e}")
    
    return None

async def get_weather_for_date(latitude: float, longitude: float, date: str) -> Optional[dict]:
    """Get weather forecast for a specific date using Open-Meteo API"""
    try:
        # Parse the date
        target_date = datetime.strptime(date, "%Y-%m-%d").date()
        today = datetime.now().date()
        
        # Open-Meteo provides forecasts up to 16 days ahead
        days_ahead = (target_date - today).days
        
        if days_ahead < 0:
            # Past date - return None
            return None
        elif days_ahead > 16:
            # Too far in the future
            return {"unavailable": True, "reason": "Forecast not available yet"}
        
        async with httpx.AsyncClient() as client:
            response = await client.get(
                "https://api.open-meteo.com/v1/forecast",
                params={
                    "latitude": latitude,
                    "longitude": longitude,
                    "daily": "weather_code,temperature_2m_max,temperature_2m_min",
                    "timezone": "auto",
                    "start_date": date,
                    "end_date": date
                },
                timeout=5.0
            )
            
            if response.status_code == 200:
                data = response.json()
                daily = data.get("daily", {})
                
                if daily.get("time") and len(daily["time"]) > 0:
                    weather_code = daily["weather_code"][0] if daily.get("weather_code") else 0
                    temp_max = daily["temperature_2m_max"][0] if daily.get("temperature_2m_max") else None
                    temp_min = daily["temperature_2m_min"][0] if daily.get("temperature_2m_min") else None
                    
                    weather_info = WEATHER_CODES.get(weather_code, {"icon": "🌡️", "description": "Unknown"})
                    
                    return {
                        "icon": weather_info["icon"],
                        "description": weather_info["description"],
                        "temp_max": round(temp_max) if temp_max is not None else None,
                        "temp_min": round(temp_min) if temp_min is not None else None,
                        "weather_code": weather_code
                    }
    except Exception as e:
        logging.error(f"Weather API error: {e}")
    
    return None

@api_router.get("/weather")
async def get_weather(
    city: str,
    date: str,
    country: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    """Get weather forecast for a city and date"""
    # Validate date format
    try:
        datetime.strptime(date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")
    
    # Get coordinates for the city
    coords = await get_coordinates(city, country)
    if not coords:
        return {"unavailable": True, "reason": "City not found"}
    
    # Get weather for the date
    weather = await get_weather_for_date(coords["latitude"], coords["longitude"], date)
    if not weather:
        return {"unavailable": True, "reason": "Weather data not available"}
    
    return weather


# ============== Badge System Endpoints ==============

BADGE_LEVELS = [10, 50, 100, 200, 500]

def get_badge_level(total_walks: int) -> int:
    """Get the highest badge level achieved"""
    for level in reversed(BADGE_LEVELS):
        if total_walks >= level:
            return level
    return 0

@api_router.get("/badge/info")
async def get_badge_info(current_user: User = Depends(get_current_user)):
    """Get badge information for current user"""
    from datetime import date
    today = date.today().isoformat()
    
    # Count walks organized by user (past dates only = completed)
    organized_count = await db.walks.count_documents({
        "organizer_id": current_user.user_id,
        "date": {"$lt": today}
    })
    
    # Count walks participated in (past dates only = completed, active bookings)
    participated_bookings = await db.bookings.find(
        {"user_id": current_user.user_id, "status": "active"},
        {"walk_id": 1}
    ).to_list(10000)
    
    participated_walk_ids = [b["walk_id"] for b in participated_bookings]
    
    # Count only past walks from bookings
    participated_count = 0
    if participated_walk_ids:
        participated_count = await db.walks.count_documents({
            "walk_id": {"$in": participated_walk_ids},
            "date": {"$lt": today}
        })
    
    total_walks = organized_count + participated_count
    current_badge_level = get_badge_level(total_walks)
    
    # Get user's last celebrated badge level
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"_id": 0})
    last_celebrated_badge = user_doc.get("last_celebrated_badge", 0)
    
    # Check if there's a new badge to celebrate
    should_celebrate = current_badge_level > last_celebrated_badge and current_badge_level > 0
    
    return {
        "organized_count": organized_count,
        "participated_count": participated_count,
        "total_walks": total_walks,
        "badge_level": current_badge_level,
        "should_celebrate": should_celebrate,
        "last_celebrated_badge": last_celebrated_badge
    }

@api_router.post("/badge/celebrate")
async def mark_badge_celebrated(current_user: User = Depends(get_current_user)):
    """Mark the current badge level as celebrated (so confetti doesn't show again)"""
    # Get current badge level
    badge_info = await get_badge_info(current_user)
    current_level = badge_info["badge_level"]
    
    # Update user's last celebrated badge
    await db.users.update_one(
        {"user_id": current_user.user_id},
        {"$set": {"last_celebrated_badge": current_level}}
    )
    
    return {"message": "Badge celebration marked", "badge_level": current_level}

@api_router.get("/badge/user/{user_id}")
async def get_user_badge(user_id: str, current_user: User = Depends(get_current_user)):
    """Get badge information for any user (for displaying on walk cards, participant lists)"""
    from datetime import date
    today = date.today().isoformat()
    
    # Count walks organized by user (past dates only)
    organized_count = await db.walks.count_documents({
        "organizer_id": user_id,
        "date": {"$lt": today}
    })
    
    # Count walks participated in (past dates only)
    participated_bookings = await db.bookings.find(
        {"user_id": user_id, "status": "active"},
        {"walk_id": 1}
    ).to_list(10000)
    
    participated_walk_ids = [b["walk_id"] for b in participated_bookings]
    
    participated_count = 0
    if participated_walk_ids:
        participated_count = await db.walks.count_documents({
            "walk_id": {"$in": participated_walk_ids},
            "date": {"$lt": today}
        })
    
    total_walks = organized_count + participated_count
    badge_level = get_badge_level(total_walks)
    
    return {
        "user_id": user_id,
        "total_walks": total_walks,
        "badge_level": badge_level
    }


# ============== Location Auto-suggestion Endpoints ==============

@api_router.get("/locations/cities")
async def get_cities(
    q: str = "",
    current_user: User = Depends(get_current_user)
):
    """Get unique cities from users and walks for auto-suggestion"""
    # Get unique cities from users (exclude None and empty strings)
    user_cities = await db.users.distinct("city", {"city": {"$nin": [None, ""]}})
    
    # Get unique cities from walks
    walk_cities = await db.walks.distinct("city", {"city": {"$nin": [None, ""]}})
    
    # Combine and deduplicate
    all_cities = list(set(user_cities + walk_cities))
    
    # Filter by query if provided
    if q:
        q_lower = q.lower()
        all_cities = [city for city in all_cities if q_lower in city.lower()]
    
    # Sort alphabetically
    all_cities.sort()
    
    return {"cities": all_cities}

@api_router.get("/locations/neighborhoods")
async def get_neighborhoods(
    city: str = "",
    q: str = "",
    current_user: User = Depends(get_current_user)
):
    """Get unique neighborhoods, optionally filtered by city, for auto-suggestion"""
    # Build query filter (exclude None and empty strings)
    user_filter = {"neighborhood": {"$nin": [None, ""]}}
    walk_filter = {"neighborhood": {"$nin": [None, ""]}}
    
    if city:
        user_filter["city"] = city
        walk_filter["city"] = city
    
    # Get unique neighborhoods from users
    user_neighborhoods = await db.users.distinct("neighborhood", user_filter)
    
    # Get unique neighborhoods from walks
    walk_neighborhoods = await db.walks.distinct("neighborhood", walk_filter)
    
    # Combine and deduplicate
    all_neighborhoods = list(set(user_neighborhoods + walk_neighborhoods))
    
    # Filter by query if provided
    if q:
        q_lower = q.lower()
        all_neighborhoods = [n for n in all_neighborhoods if q_lower in n.lower()]
    
    # Sort alphabetically
    all_neighborhoods.sort()
    
    return {"neighborhoods": all_neighborhoods}


# ============== Walk Endpoints ==============

@api_router.post("/walks")
async def create_walk(
    walk_data: WalkCreate,
    current_user: User = Depends(get_current_user)
):
    """Create a new walk (any authenticated user can organize a walk)"""
    walk_id = f"walk_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc)
    
    logging.info(f"🚶 Creating walk: title='{walk_data.title}', city='{walk_data.city}', user={current_user.user_id}")
    
    # Set ending_point to starting_point if not provided
    ending_point = walk_data.ending_point if walk_data.ending_point else walk_data.starting_point
    
    walk = {
        "walk_id": walk_id,
        "title": walk_data.title,
        "date": walk_data.date,
        "time": walk_data.time,
        "city": walk_data.city,
        "neighborhood": walk_data.neighborhood,
        "starting_point": walk_data.starting_point,
        "ending_point": ending_point,
        "duration_hours": walk_data.duration_hours,
        "max_participants": walk_data.max_participants,
        "description": walk_data.description,
        "conditions": walk_data.conditions,
        "organizer_id": current_user.user_id,
        "organizer_name": current_user.pseudonym or current_user.name,  # Use pseudonym if available
        "created_at": now,
        "updated_at": now
    }
    
    result = await db.walks.insert_one(walk.copy())
    logging.info(f"✅ Walk created successfully: walk_id={walk_id}, inserted_id={result.inserted_id}")
    
    # Send notifications to all previous participants (async, don't wait)
    await notify_previous_participants(walk)
    
    # Remove MongoDB's _id before returning
    walk_response = {k: v for k, v in walk.items() if k != '_id'}
    return walk_response

@api_router.get("/walks")
async def get_walks(
    show_past: bool = False,
    all_locations: bool = False,  # Admin can see all walks across all locations
    current_user: User = Depends(get_current_user)
):
    """Get walks filtered by user's city, age, and sex (neighborhood is informational only)
    
    Admin users can pass all_locations=true to see walks in ALL cities (global view)
    """
    from datetime import date
    
    # Build base query filter
    query = {}
    
    # Filter by date (upcoming only by default)
    if not show_past:
        today = date.today().isoformat()
        query["date"] = {"$gte": today}
    
    # Admin global view - if admin requests all locations, skip city filter
    if all_locations and is_admin(current_user):
        logging.info(f"Admin {current_user.user_id} viewing all walks globally (all_locations=true)")
        # No city filter for admin global view
    elif current_user.city:
        # Filter by user's city ONLY (neighborhood is informational, not for matching)
        query["city"] = current_user.city
    else:
        # If user hasn't set city, show no walks (they need to set their location first)
        logging.info(f"User {current_user.user_id} has no city set - returning no walks")
        return []
    
    # Get walks matching city and date
    walks = await db.walks.find(query, {"_id": 0}).sort("date", 1).to_list(1000)
    
    # Get current date and time for filtering today's walks that have already passed
    now = datetime.now()
    today_str = now.strftime("%Y-%m-%d")
    current_time_minutes = now.hour * 60 + now.minute
    
    # Get user's booked walk IDs to exclude already booked walks
    user_bookings = await db.bookings.find(
        {"user_id": current_user.user_id, "status": "active"},
        {"walk_id": 1}
    ).to_list(1000)
    booked_walk_ids = set(b["walk_id"] for b in user_bookings)
    
    # Admin global view - skip age/sex filtering if admin is viewing all locations
    skip_condition_filters = all_locations and is_admin(current_user)
    
    # Further filter by age and sex conditions
    filtered_walks = []
    for walk in walks:
        # Skip walks the user has already booked (they appear in My Schedule)
        if walk["walk_id"] in booked_walk_ids:
            continue
        
        # Skip walks that are today but the time has already passed
        walk_date = walk.get("date", "")
        walk_time = walk.get("time", "")
        if walk_date == today_str and walk_time:
            # Parse the walk time (format: "HH:MM" or "H:MM")
            try:
                time_parts = walk_time.split(":")
                if len(time_parts) >= 2:
                    walk_hour = int(time_parts[0])
                    walk_minute = int(time_parts[1].split()[0])  # Handle "2:30 PM" format
                    # Check for AM/PM
                    if "PM" in walk_time.upper() and walk_hour != 12:
                        walk_hour += 12
                    elif "AM" in walk_time.upper() and walk_hour == 12:
                        walk_hour = 0
                    walk_time_minutes = walk_hour * 60 + walk_minute
                    if walk_time_minutes <= current_time_minutes:
                        logging.info(f"Skipping walk '{walk.get('title')}' - time has passed ({walk_time} vs current {now.strftime('%H:%M')})")
                        continue
            except (ValueError, IndexError) as e:
                logging.warning(f"Could not parse walk time '{walk_time}': {e}")
        
        # Skip condition filtering for admin global view
        if not skip_condition_filters:
            # Check if walk has conditions
            conditions = walk.get("conditions", {})
            
            # Filter by sex (normalize both user's sex and walk's sex requirements)
            if conditions and conditions.get("sex"):
                # Normalize walk's sex requirements
                walk_sex_normalized = normalize_sex_list(conditions["sex"])
                # Normalize user's sex
                user_sex_normalized = normalize_sex(current_user.sex)
                
                # If walk specifies sex requirements, check if user's sex matches
                if user_sex_normalized not in walk_sex_normalized:
                    continue  # Skip this walk - user's sex doesn't match
            
            # Filter by age group
            if conditions and conditions.get("age_groups"):
                # Check if user's age falls in any of the required age groups
                user_age = current_user.age
                if user_age is None:
                    continue  # Skip if user hasn't set age and walk has age requirements
                
                # Non-overlapping age ranges: 18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75+
                age_match = False
                for age_group in conditions["age_groups"]:
                    if age_group == "18-24" and 18 <= user_age <= 24:
                        age_match = True
                    elif age_group == "25-34" and 25 <= user_age <= 34:
                        age_match = True
                    elif age_group == "35-44" and 35 <= user_age <= 44:
                        age_match = True
                    elif age_group == "45-54" and 45 <= user_age <= 54:
                        age_match = True
                    elif age_group == "55-64" and 55 <= user_age <= 64:
                        age_match = True
                    elif age_group == "65-74" and 65 <= user_age <= 74:
                        age_match = True
                    elif age_group == "75+" and user_age >= 75:
                        age_match = True
                
                if not age_match:
                    continue  # Skip this walk - user's age doesn't match any group
        
        # Walk passed all filters, add participant count
        participant_count = await db.bookings.count_documents({
            "walk_id": walk["walk_id"],
            "status": "active"
        })
        walk["participant_count"] = participant_count
        max_p = walk.get("max_participants", 0)
        is_full = max_p > 0 and participant_count >= max_p
        logging.info(f"   Walk '{walk.get('title')}': participants={participant_count}, max={max_p}, full={is_full}")
        filtered_walks.append(walk)
    
    logging.info(f"User {current_user.user_id} (city: {current_user.city}, age: {current_user.age}, sex: {current_user.sex}) - showing {len(filtered_walks)} matching walks")
    return filtered_walks

@api_router.get("/walks/my-organized")
async def get_my_organized_walks(
    current_user: User = Depends(get_current_user)
):
    """Get all walks organized by the current user (including past events)"""
    walks = await db.walks.find(
        {"organizer_id": current_user.user_id},
        {"_id": 0}
    ).sort("date", -1).to_list(1000)
    
    # Add participant count for each walk
    for walk in walks:
        participant_count = await db.bookings.count_documents({
            "walk_id": walk["walk_id"],
            "status": "active"
        })
        walk["participant_count"] = participant_count
    
    return walks

@api_router.get("/walks/feedback-eligible")
async def get_walks_for_feedback(
    current_user: User = Depends(get_current_user)
):
    """Get walks the user can give feedback on (booked walks or any walk in their area)"""
    
    # Get user's bookings to identify walks they've interacted with
    user_bookings = await db.bookings.find(
        {"user_id": current_user.user_id},
        {"walk_id": 1}
    ).to_list(1000)
    booked_walk_ids = [b["walk_id"] for b in user_bookings]
    
    # Build query - walks in user's location OR walks they've booked
    # This allows feedback on both attended and booked walks
    query = {"$or": []}
    
    # Add booked walks (regardless of location)
    if booked_walk_ids:
        query["$or"].append({"walk_id": {"$in": booked_walk_ids}})
    
    # Add walks in user's city/neighborhood (if they have location set)
    if current_user.city and current_user.neighborhood:
        query["$or"].append({
            "city": current_user.city,
            "neighborhood": current_user.neighborhood
        })
    
    # If no criteria, return empty list
    if not query["$or"]:
        return []
    
    # Get walks (exclude user's own walks)
    walks = await db.walks.find(
        {
            **query,
            "organizer_id": {"$ne": current_user.user_id}  # Can't feedback your own walks
        },
        {"_id": 0}
    ).sort("date", -1).to_list(1000)
    
    return walks


@api_router.get("/walks/reviewable")
async def get_reviewable_walks(
    current_user: User = Depends(get_current_user)
):
    """Get walks the user can review (booked but not yet reviewed)"""
    import time
    start = time.time()
    
    # Get user's bookings
    bookings = await db.bookings.find(
        {"user_id": current_user.user_id},
        {"walk_id": 1}
    ).to_list(1000)
    
    booked_walk_ids = [b["walk_id"] for b in bookings]
    
    t1 = time.time()
    logging.info(f"[Reviewable] Got {len(booked_walk_ids)} bookings in {(t1-start)*1000:.0f}ms")
    
    if not booked_walk_ids:
        return []
    
    # Get user's existing reviews
    existing_reviews = await db.reviews.find(
        {"user_id": current_user.user_id},
        {"walk_id": 1}
    ).to_list(1000)
    
    reviewed_walk_ids = [r["walk_id"] for r in existing_reviews]
    
    t2 = time.time()
    logging.info(f"[Reviewable] Got {len(reviewed_walk_ids)} reviews in {(t2-t1)*1000:.0f}ms")
    
    # Get walks that are booked but not reviewed
    reviewable_walk_ids = [wid for wid in booked_walk_ids if wid not in reviewed_walk_ids]
    
    if not reviewable_walk_ids:
        logging.info(f"[Reviewable] Total: {(time.time()-start)*1000:.0f}ms - no reviewable walks")
        return []
    
    walks = await db.walks.find(
        {"walk_id": {"$in": reviewable_walk_ids}},
        {"_id": 0}
    ).to_list(100)
    
    t3 = time.time()
    logging.info(f"[Reviewable] Total: {(t3-start)*1000:.0f}ms for {len(walks)} reviewable walks")
    
    return walks


@api_router.get("/walks/{walk_id}")
async def get_walk(
    walk_id: str,
    current_user: User = Depends(get_current_user)
):
    """Get walk by ID with participant count"""
    walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Add participant count
    participant_count = await db.bookings.count_documents({
        "walk_id": walk_id,
        "status": "active"
    })
    walk["participant_count"] = participant_count
    
    return walk

@api_router.put("/walks/{walk_id}")
async def update_walk(
    walk_id: str,
    walk_data: WalkUpdate,
    current_user: User = Depends(get_current_user)
):
    """Update walk (organizer or app admin only)"""
    # Get the walk to check ownership
    walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Check if user is organizer or admin
    if walk["organizer_id"] != current_user.user_id and current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Only the organizer or app admin can edit this walk")
    
    # Build update dict
    update_dict = {k: v for k, v in walk_data.dict().items() if v is not None}
    if not update_dict:
        raise HTTPException(status_code=400, detail="No update data provided")
    
    update_dict["updated_at"] = datetime.now(timezone.utc)
    
    result = await db.walks.update_one(
        {"walk_id": walk_id},
        {"$set": update_dict}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    updated_walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    return updated_walk

@api_router.delete("/walks/{walk_id}")
async def delete_walk(
    walk_id: str,
    current_user: User = Depends(get_current_user)
):
    """Delete walk (organizer or app admin only)"""
    # Get the walk to check ownership
    walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Check if user is organizer or admin
    if walk["organizer_id"] != current_user.user_id and current_user.role != "admin":
        raise HTTPException(status_code=403, detail="Only the organizer or app admin can delete this walk")
    
    # Delete walk
    result = await db.walks.delete_one({"walk_id": walk_id})
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Delete all bookings for this walk
    await db.bookings.delete_many({"walk_id": walk_id})
    
    return {"message": "Walk deleted successfully"}

@api_router.post("/walks/{walk_id}/send-reminder")
async def send_reminder_endpoint(
    walk_id: str,
    current_user: User = Depends(require_admin)
):
    """Send reminder notifications to all participants (admin only)"""
    walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    await send_walk_reminder(walk_id)
    return {"message": "Reminders sent successfully"}


# ============== Push Notification Endpoints ==============

@api_router.post("/push/register")
async def register_push_token(
    token_data: PushTokenRegister,
    current_user: User = Depends(get_current_user)
):
    """
    Register a push token for the current user's device.
    Called by the mobile app after login or when token changes.
    """
    if not token_data.push_token:
        raise HTTPException(status_code=400, detail="Push token required")
    
    if not token_data.push_token.startswith("ExponentPushToken["):
        raise HTTPException(status_code=400, detail="Invalid Expo push token format")
    
    # Upsert the push token (update if exists, create if not)
    await db.push_tokens.update_one(
        {
            "user_id": current_user.user_id,
            "push_token": token_data.push_token
        },
        {
            "$set": {
                "user_id": current_user.user_id,
                "push_token": token_data.push_token,
                "platform": token_data.platform,
                "active": True,
                "updated_at": datetime.now(timezone.utc)
            },
            "$setOnInsert": {
                "created_at": datetime.now(timezone.utc)
            }
        },
        upsert=True
    )
    
    logging.info(f"📲 Registered push token for user {current_user.user_id} on {token_data.platform}")
    
    return {"status": "success", "message": "Push token registered"}


@api_router.delete("/push/unregister")
async def unregister_push_token(
    push_token: str,
    current_user: User = Depends(get_current_user)
):
    """Unregister a push token (called on logout)"""
    await db.push_tokens.update_one(
        {"user_id": current_user.user_id, "push_token": push_token},
        {"$set": {"active": False}}
    )
    return {"status": "success", "message": "Push token unregistered"}


# ============== Booking Endpoints ==============

@api_router.post("/bookings", response_model=Booking)
async def create_booking(
    booking_data: BookingCreate,
    current_user: User = Depends(get_current_user)
):
    """Book a walk"""
    # Check if walk exists
    walk = await db.walks.find_one({"walk_id": booking_data.walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Hard limit of 10 participants - no bookings beyond this
    HARD_LIMIT = 10
    current_participants = await db.bookings.count_documents({
        "walk_id": booking_data.walk_id,
        "status": "active"
    })
    
    if current_participants >= HARD_LIMIT:
        raise HTTPException(status_code=400, detail=f"This walk has reached the maximum limit of {HARD_LIMIT} participants")
    
    # Check if already booked
    existing = await db.bookings.find_one({
        "walk_id": booking_data.walk_id,
        "user_id": current_user.user_id,
        "status": "active"
    })
    if existing:
        raise HTTPException(status_code=400, detail="Already booked this walk")
    
    booking_id = f"booking_{uuid.uuid4().hex[:12]}"
    booking = {
        "booking_id": booking_id,
        "walk_id": booking_data.walk_id,
        "user_id": current_user.user_id,
        "user_name": current_user.pseudonym or current_user.name,  # Use pseudonym if available
        "user_email": current_user.email,
        "booked_at": datetime.now(timezone.utc),
        "status": "active"
    }
    
    await db.bookings.insert_one(booking)
    return Booking(**booking)

@api_router.get("/bookings", response_model=List[Booking])
async def get_my_bookings(current_user: User = Depends(get_current_user)):
    """Get current user's bookings with walk details (optimized - no N+1)"""
    import time
    start = time.time()
    
    bookings = await db.bookings.find(
        {"user_id": current_user.user_id, "status": "active"},
        {"_id": 0}
    ).to_list(1000)
    
    t1 = time.time()
    logging.info(f"[Bookings] Fetched {len(bookings)} bookings in {(t1-start)*1000:.0f}ms")
    
    # Get all unique walk_ids from bookings
    walk_ids = list(set(b.get("walk_id") for b in bookings if b.get("walk_id")))
    
    # Fetch all walks in ONE query (avoid N+1 problem)
    walks_map = {}
    if walk_ids:
        walks = await db.walks.find(
            {"walk_id": {"$in": walk_ids}},
            {"walk_id": 1, "title": 1, "date": 1, "_id": 0}
        ).to_list(1000)
        walks_map = {w["walk_id"]: w for w in walks}
    
    t2 = time.time()
    logging.info(f"[Bookings] Fetched {len(walks_map)} walks in {(t2-t1)*1000:.0f}ms")
    
    # Enrich bookings with walk details
    enriched_bookings = []
    for booking in bookings:
        walk = walks_map.get(booking.get("walk_id"), {})
        booking["walk_title"] = walk.get("title", "Unknown Walk")
        booking["walk_date"] = walk.get("date", "Unknown Date")
        enriched_bookings.append(Booking(**booking))
    
    t3 = time.time()
    logging.info(f"[Bookings] Total: {(t3-start)*1000:.0f}ms for {len(enriched_bookings)} enriched bookings")
    
    return enriched_bookings

@api_router.delete("/bookings/{booking_id}")
async def cancel_booking(
    booking_id: str,
    current_user: User = Depends(get_current_user)
):
    """Cancel a booking"""
    result = await db.bookings.update_one(
        {"booking_id": booking_id, "user_id": current_user.user_id},
        {"$set": {"status": "cancelled"}}
    )
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Booking not found")
    
    return {"message": "Booking cancelled successfully"}

@api_router.get("/walks/{walk_id}/participants", response_model=List[ParticipantInfo])
async def get_walk_participants(
    walk_id: str,
    current_user: User = Depends(get_current_user)
):
    """Get list of participants for a walk"""
    from datetime import date
    today = date.today().isoformat()
    
    # Check if walk exists
    walk = await db.walks.find_one({"walk_id": walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Get all active bookings
    bookings = await db.bookings.find(
        {"walk_id": walk_id, "status": "active"},
        {"_id": 0}
    ).to_list(1000)
    
    if not bookings:
        return []
    
    # Extract unique user_ids
    user_ids = list(set(booking["user_id"] for booking in bookings))
    
    # Fetch all users in a single query
    users_cursor = db.users.find(
        {"user_id": {"$in": user_ids}},
        {"_id": 0}
    )
    users_list = await users_cursor.to_list(1000)
    
    # Create user map for O(1) lookups
    user_map = {user["user_id"]: user for user in users_list}
    
    # Calculate badge info for each participant
    async def get_user_badge_info(user_id: str):
        # Count walks organized by user (past dates only = completed)
        organized_count = await db.walks.count_documents({
            "organizer_id": user_id,
            "date": {"$lt": today}
        })
        
        # Count walks participated in (past dates only = completed, active bookings)
        participated_bookings = await db.bookings.find(
            {"user_id": user_id, "status": "active"},
            {"walk_id": 1}
        ).to_list(10000)
        
        participated_walk_ids = [b["walk_id"] for b in participated_bookings]
        
        participated_count = 0
        if participated_walk_ids:
            participated_count = await db.walks.count_documents({
                "walk_id": {"$in": participated_walk_ids},
                "date": {"$lt": today}
            })
        
        total_walks = organized_count + participated_count
        badge_level = get_badge_level(total_walks)
        return badge_level, total_walks
    
    # Build participants list
    participants = []
    for booking in bookings:
        user = user_map.get(booking["user_id"])
        if user:
            badge_level, total_walks = await get_user_badge_info(user["user_id"])
            participants.append(ParticipantInfo(
                user_id=user["user_id"],
                name=user["name"],
                pseudonym=user.get("pseudonym"),  # Include pseudonym
                email=user["email"],
                picture=user.get("picture"),
                age=user.get("age"),
                sex=user.get("sex"),
                country_of_birth=user.get("country_of_birth"),
                booked_at=booking["booked_at"],
                badge_level=badge_level,
                total_walks=total_walks
            ))
    
    return participants


# ============== Feedback Endpoints ==============

@api_router.post("/feedback", response_model=Feedback)
async def create_feedback(
    feedback_data: FeedbackCreate,
    current_user: User = Depends(get_current_user)
):
    """Send feedback to a walk organizer"""
    # Get the walk to get organizer info
    walk = await db.walks.find_one({"walk_id": feedback_data.walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Prevent sending feedback to yourself
    if walk["organizer_id"] == current_user.user_id:
        raise HTTPException(status_code=400, detail="Cannot send feedback to yourself")
    
    # Check message length
    if not feedback_data.message or len(feedback_data.message.strip()) < 5:
        raise HTTPException(status_code=400, detail="Message must be at least 5 characters")
    
    if len(feedback_data.message) > 2000:
        raise HTTPException(status_code=400, detail="Message must be less than 2000 characters")
    
    feedback_id = f"feedback_{uuid.uuid4().hex[:12]}"
    
    # Get user pseudonym for privacy
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"pseudonym": 1})
    user_pseudonym = user_doc.get("pseudonym") if user_doc else None
    display_name = user_pseudonym or current_user.name
    
    feedback = {
        "feedback_id": feedback_id,
        "walk_id": feedback_data.walk_id,
        "walk_title": walk["title"],
        "from_user_id": current_user.user_id,
        "from_user_name": display_name,  # Use pseudonym for privacy
        "to_organizer_id": walk["organizer_id"],
        "to_organizer_name": walk["organizer_name"],
        "message": feedback_data.message.strip(),
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.feedback.insert_one(feedback)
    logging.info(f"💬 Feedback sent from {current_user.name} to {walk['organizer_name']} about walk '{walk['title']}'")
    
    return Feedback(**feedback)


@api_router.get("/feedback/sent")
async def get_sent_feedback(
    current_user: User = Depends(get_current_user)
):
    """Get all feedback sent by the current user"""
    feedback_list = await db.feedback.find(
        {"from_user_id": current_user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(1000)
    
    return feedback_list


@api_router.get("/feedback/received")
async def get_received_feedback(
    current_user: User = Depends(get_current_user)
):
    """Get all feedback received by the current user (as organizer)"""
    feedback_list = await db.feedback.find(
        {"to_organizer_id": current_user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(1000)
    
    return feedback_list


@api_router.post("/feedback/{feedback_id}/reply")
async def reply_to_feedback(
    feedback_id: str,
    reply_data: FeedbackReplyCreate,
    current_user: User = Depends(get_current_user)
):
    """Reply to a feedback message (only organizer who received the feedback can reply)"""
    # Get the feedback
    feedback = await db.feedback.find_one({"feedback_id": feedback_id}, {"_id": 0})
    
    if not feedback:
        raise HTTPException(status_code=404, detail="Feedback not found")
    
    # Only the organizer who received the feedback OR the original sender can reply
    if current_user.user_id != feedback["to_organizer_id"] and current_user.user_id != feedback["from_user_id"]:
        raise HTTPException(status_code=403, detail="You can only reply to feedback you received or sent")
    
    # Validate message
    if not reply_data.message or len(reply_data.message.strip()) < 1:
        raise HTTPException(status_code=400, detail="Reply message cannot be empty")
    
    if len(reply_data.message) > 1000:
        raise HTTPException(status_code=400, detail="Reply message too long (max 1000 characters)")
    
    # Create reply
    reply_id = f"reply_{uuid.uuid4().hex[:12]}"
    
    # Get user pseudonym for privacy
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"pseudonym": 1})
    user_pseudonym = user_doc.get("pseudonym") if user_doc else None
    display_name = user_pseudonym or current_user.name
    
    reply = {
        "reply_id": reply_id,
        "feedback_id": feedback_id,
        "from_user_id": current_user.user_id,
        "from_user_name": display_name,  # Use pseudonym for privacy
        "message": reply_data.message.strip(),
        "created_at": datetime.now(timezone.utc)
    }
    
    # Add reply to feedback document
    await db.feedback.update_one(
        {"feedback_id": feedback_id},
        {"$push": {"replies": reply}}
    )
    
    return reply


# ============== Walker Experiences API ==============

@api_router.post("/experiences", response_model=WalkerExperience)
async def create_experience(
    experience_data: WalkerExperienceCreate,
    current_user: User = Depends(get_current_user)
):
    """Create a new walker experience post"""
    if not experience_data.text or len(experience_data.text.strip()) < 5:
        raise HTTPException(status_code=400, detail="Experience text must be at least 5 characters")
    
    if len(experience_data.text) > 2000:
        raise HTTPException(status_code=400, detail="Experience text too long (max 2000 characters)")
    
    # Get walk title if walk_id provided
    walk_title = None
    if experience_data.walk_id:
        walk = await db.walks.find_one({"walk_id": experience_data.walk_id}, {"title": 1})
        if walk:
            walk_title = walk.get("title")
    
    # Get user info
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"picture": 1, "pseudonym": 1, "city": 1})
    user_picture = user_doc.get("picture") if user_doc else None
    user_pseudonym = user_doc.get("pseudonym") if user_doc else None
    user_city = user_doc.get("city") if user_doc else None
    
    experience_id = f"exp_{uuid.uuid4().hex[:12]}"
    experience = {
        "experience_id": experience_id,
        "user_id": current_user.user_id,
        "user_name": user_pseudonym or current_user.name,  # Use pseudonym for privacy, fallback to name
        "user_pseudonym": user_pseudonym,
        "user_city": user_city,
        "user_picture": user_picture,
        "walk_id": experience_data.walk_id,
        "walk_title": walk_title,
        "text": experience_data.text.strip(),
        "photo": experience_data.photo,
        "video": experience_data.video,
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.experiences.insert_one(experience)
    return WalkerExperience(**experience)


@api_router.get("/experiences")
async def get_experiences(
    current_user: User = Depends(get_current_user)
):
    """Get walker experiences (filtered by same city for non-admins, hidden experiences excluded for non-admins)"""
    # Get current user's city
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"city": 1})
    user_city = user_doc.get("city") if user_doc else None
    
    # If admin, show all experiences (including hidden); otherwise filter by city and visibility
    if is_admin(current_user):
        experiences = await db.experiences.find(
            {},
            {"_id": 0}
        ).sort("created_at", -1).to_list(100)
    elif user_city:
        # Show visible experiences from users in the same city
        experiences = await db.experiences.find(
            {"user_city": user_city, "is_visible": {"$ne": False}},
            {"_id": 0}
        ).sort("created_at", -1).to_list(100)
    else:
        # No city set, show no experiences
        experiences = []
    
    return experiences


@api_router.get("/experiences/my")
async def get_my_experiences(
    current_user: User = Depends(get_current_user)
):
    """Get current user's experiences"""
    experiences = await db.experiences.find(
        {"user_id": current_user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    return experiences


@api_router.post("/admin/message")
async def send_admin_message(
    message_data: AdminMessage,
    current_user: User = Depends(get_current_user)
):
    """Send a message/suggestion to app admins"""
    if not message_data.message or len(message_data.message.strip()) < 10:
        raise HTTPException(status_code=400, detail="Message must be at least 10 characters")
    
    if len(message_data.message) > 1000:
        raise HTTPException(status_code=400, detail="Message too long (max 1000 characters)")
    
    # Store the message
    admin_msg = {
        "message_id": f"msg_{uuid.uuid4().hex[:12]}",
        "from_user_id": current_user.user_id,
        "from_user_name": current_user.name,
        "from_user_email": current_user.email,
        "message": message_data.message.strip(),
        "created_at": datetime.now(timezone.utc),
        "read": False
    }
    
    await db.admin_messages.insert_one(admin_msg)
    
    # Log for admins
    logging.info(f"📧 ADMIN MESSAGE from {current_user.name} ({current_user.email}): {message_data.message[:100]}...")
    
    # Send email notification to all admins in ADMIN_EMAILS list
    try:
        email_subject = f"WalkWithUs - New Message from {current_user.name}"
        email_body = f"""
        <h2>New Message from App User</h2>
        <p><strong>From:</strong> {current_user.name}</p>
        <p><strong>Email:</strong> {current_user.email}</p>
        <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        <hr>
        <p><strong>Message:</strong></p>
        <p style="background-color: #f5f5f5; padding: 15px; border-radius: 5px;">
            {message_data.message.strip()}
        </p>
        <hr>
        <p><em>This message was sent via the WalkWithUs app contact form.</em></p>
        """
        
        # Send to all admin emails in the ADMIN_EMAILS list
        for admin_email in ADMIN_EMAILS:
            logging.info(f"📧 Sending admin message notification to {admin_email}")
            await send_email_notification(admin_email, email_subject, email_body)
        
        logging.info(f"✅ Admin message email sent to {len(ADMIN_EMAILS)} admin(s)")
            
    except Exception as e:
        logging.error(f"❌ Error sending admin message email: {e}")
        # Don't fail the request - message is already stored
    
    return {"message": "Message sent successfully"}


@api_router.post("/report/content")
async def report_non_compliant_content(
    report_data: ContentReport,
    current_user: User = Depends(get_current_user)
):
    """Report non-compliant content to admins"""
    if not report_data.description or len(report_data.description.strip()) < 10:
        raise HTTPException(status_code=400, detail="Description must be at least 10 characters")
    
    if len(report_data.description) > 2000:
        raise HTTPException(status_code=400, detail="Description too long (max 2000 characters)")
    
    # Store the report
    content_report = {
        "report_id": f"rpt_{uuid.uuid4().hex[:12]}",
        "reporter_user_id": current_user.user_id,
        "reporter_user_name": current_user.name,
        "reporter_user_email": current_user.email,
        "content_type": report_data.content_type,
        "content_id": report_data.content_id,
        "reported_user_name": report_data.reported_user_name,
        "walk_title": report_data.walk_title,
        "walk_date": report_data.walk_date,
        "description": report_data.description.strip(),
        "created_at": datetime.now(timezone.utc),
        "status": "pending",  # pending, reviewed, resolved
        "reviewed_by": None,
        "reviewed_at": None
    }
    
    await db.content_reports.insert_one(content_report)
    
    # Log for admins
    logging.info(f"🚨 CONTENT REPORT from {current_user.name} ({current_user.email}): {report_data.content_type} - {report_data.description[:100]}...")
    
    # Send email notification to all admins
    try:
        email_subject = f"🚨 WalkWithUs - Content Report from {current_user.name}"
        
        # Build the details section
        details = []
        if report_data.reported_user_name:
            details.append(f"<p><strong>Reported User:</strong> {report_data.reported_user_name}</p>")
        if report_data.walk_title:
            details.append(f"<p><strong>Walk Title:</strong> {report_data.walk_title}</p>")
        if report_data.walk_date:
            details.append(f"<p><strong>Walk Date:</strong> {report_data.walk_date}</p>")
        
        details_html = "".join(details) if details else ""
        
        email_body = f"""
        <h2>🚨 Non-Compliant Content Report</h2>
        <p><strong>Reported by:</strong> {current_user.name}</p>
        <p><strong>Reporter Email:</strong> {current_user.email}</p>
        <p><strong>Content Type:</strong> {report_data.content_type}</p>
        <p><strong>Date:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        {details_html}
        <hr>
        <p><strong>Report Description:</strong></p>
        <p style="background-color: #fff3cd; padding: 15px; border-radius: 5px; border-left: 4px solid #ffc107;">
            {report_data.description.strip()}
        </p>
        <hr>
        <p><em>This report requires admin review. Please check the reported content in the admin panel.</em></p>
        """
        
        # Send to all admin emails in the ADMIN_EMAILS list
        for admin_email in ADMIN_EMAILS:
            logging.info(f"📧 Sending content report notification to {admin_email}")
            await send_email_notification(admin_email, email_subject, email_body)
        
        logging.info(f"✅ Content report email sent to {len(ADMIN_EMAILS)} admin(s)")
            
    except Exception as e:
        logging.error(f"❌ Error sending content report email: {e}")
        # Don't fail the request - report is already stored
    
    return {"message": "Report submitted successfully. Thank you for helping keep our community safe."}


# ============== Admin Messages & User Search API ==============

@api_router.get("/admin/messages")
async def get_admin_messages(
    current_user: User = Depends(get_current_user),
    skip: int = 0,
    limit: int = 50
):
    """Get all messages sent to admins (admin only)"""
    # Check if user is admin using email list
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    messages = await db.admin_messages.find(
        {},
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.admin_messages.count_documents({})
    
    return {
        "messages": messages,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@api_router.put("/admin/messages/{message_id}/read")
async def mark_message_read(
    message_id: str,
    current_user: User = Depends(get_current_user)
):
    """Mark an admin message as read"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    result = await db.admin_messages.update_one(
        {"message_id": message_id},
        {"$set": {"read": True, "read_at": datetime.now(timezone.utc)}}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Message not found")
    
    return {"message": "Message marked as read"}


@api_router.get("/admin/reports")
async def get_content_reports(
    current_user: User = Depends(get_current_user),
    skip: int = 0,
    limit: int = 50
):
    """Get all content reports (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    reports = await db.content_reports.find(
        {},
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.content_reports.count_documents({})
    
    return {
        "reports": reports,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@api_router.put("/admin/reports/{report_id}/status")
async def update_report_status(
    report_id: str,
    status: str,
    current_user: User = Depends(get_current_user)
):
    """Update a content report status (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    if status not in ["pending", "reviewed", "resolved"]:
        raise HTTPException(status_code=400, detail="Invalid status. Must be: pending, reviewed, or resolved")
    
    result = await db.content_reports.update_one(
        {"report_id": report_id},
        {"$set": {
            "status": status,
            "reviewed_by": current_user.email,
            "reviewed_at": datetime.now(timezone.utc)
        }}
    )
    
    if result.modified_count == 0:
        raise HTTPException(status_code=404, detail="Report not found")
    
    return {"message": f"Report status updated to {status}"}


@api_router.get("/admin/feedback/all")
async def get_all_feedback_admin(
    current_user: User = Depends(get_current_user),
    skip: int = 0,
    limit: int = 100
):
    """Get all feedback messages for admin moderation"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    feedback_items = await db.feedback.find(
        {},
        {"_id": 0}
    ).sort("created_at", -1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.feedback.count_documents({})
    
    return {
        "feedback": feedback_items,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@api_router.delete("/admin/feedback/{feedback_id}")
async def delete_feedback_admin(
    feedback_id: str,
    current_user: User = Depends(get_current_user)
):
    """Delete a feedback message (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    result = await db.feedback.delete_one({"feedback_id": feedback_id})
    
    if result.deleted_count == 0:
        raise HTTPException(status_code=404, detail="Feedback not found")
    
    logging.info(f"🗑️ Admin {current_user.email} deleted feedback {feedback_id}")
    
    return {"message": "Feedback deleted successfully"}


@api_router.get("/admin/users/search")
async def search_users(
    current_user: User = Depends(get_current_user),
    q: Optional[str] = None,
    city: Optional[str] = None,
    neighborhood: Optional[str] = None,
    skip: int = 0,
    limit: int = 20
):
    """Search users by pseudonym, name, email, city, or neighborhood (admin only)
    
    Search uses OR logic - any matching field will return the user.
    """
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Build query using OR logic - any matching field will return user
    conditions = []
    
    if q:
        # Search in pseudonym, name, or email (case-insensitive)
        search_regex = {"$regex": q, "$options": "i"}
        conditions.append({"pseudonym": search_regex})
        conditions.append({"name": search_regex})
        conditions.append({"email": search_regex})
    
    if city:
        conditions.append({"city": {"$regex": city, "$options": "i"}})
    
    if neighborhood:
        conditions.append({"neighborhood": {"$regex": neighborhood, "$options": "i"}})
    
    # Use $or so ANY matching condition returns the user
    if conditions:
        query = {"$or": conditions}
    else:
        query = {}
    
    users = await db.users.find(
        query,
        {"_id": 0, "password_hash": 0}
    ).sort("name", 1).skip(skip).limit(limit).to_list(limit)
    
    total = await db.users.count_documents(query)
    
    # Add basic stats for each user
    for user in users:
        user_id = user.get("user_id")
        
        # Count walks organized
        user["walks_organized"] = await db.walks.count_documents({"organizer_id": user_id})
        
        # Count bookings
        user["total_bookings"] = await db.bookings.count_documents({"user_id": user_id})
        
        # Count experiences shared
        user["experiences_count"] = await db.experiences.count_documents({"user_id": user_id})
    
    return {
        "users": users,
        "total": total,
        "skip": skip,
        "limit": limit
    }


@api_router.get("/admin/users/{user_id}/details")
async def get_user_details(
    user_id: str,
    current_user: User = Depends(get_current_user)
):
    """Get detailed information about a specific user (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Get user info
    user = await db.users.find_one(
        {"user_id": user_id},
        {"_id": 0, "password_hash": 0}
    )
    
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Get user's statistics
    stats = {
        "walks_organized": await db.walks.count_documents({"organizer_id": user_id}),
        "walks_booked": await db.bookings.count_documents({"user_id": user_id, "status": "active"}),
        "walks_cancelled": await db.bookings.count_documents({"user_id": user_id, "status": "cancelled"}),
        "experiences_shared": await db.experiences.count_documents({"user_id": user_id}),
        "reviews_given": await db.reviews.count_documents({"user_id": user_id}),
        "feedbacks_sent": await db.feedbacks.count_documents({"from_user_id": user_id}),
        "feedbacks_received": await db.feedbacks.count_documents({"to_organizer_id": user_id}),
        "admin_messages_sent": await db.admin_messages.count_documents({"from_user_id": user_id})
    }
    
    # Get badge info
    badge_doc = await db.badges.find_one({"user_id": user_id}, {"_id": 0})
    
    # Get recent walks organized
    recent_walks = await db.walks.find(
        {"organizer_id": user_id},
        {"_id": 0, "walk_id": 1, "title": 1, "date": 1, "city": 1}
    ).sort("date", -1).limit(5).to_list(5)
    
    # Get recent bookings
    recent_bookings = await db.bookings.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("booked_at", -1).limit(5).to_list(5)
    
    # Get walk titles for bookings
    for booking in recent_bookings:
        walk = await db.walks.find_one(
            {"walk_id": booking.get("walk_id")},
            {"title": 1, "date": 1, "_id": 0}
        )
        if walk:
            booking["walk_title"] = walk.get("title", "Unknown")
            booking["walk_date"] = walk.get("date", "Unknown")
    
    # Get experiences
    experiences = await db.experiences.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # Get feedbacks sent by user
    feedbacks_sent = await db.feedbacks.find(
        {"from_user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # Get feedbacks received (as organizer)
    feedbacks_received = await db.feedbacks.find(
        {"to_organizer_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # Get admin messages from this user
    admin_messages = await db.admin_messages.find(
        {"from_user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    # Get reviews by user
    reviews = await db.reviews.find(
        {"user_id": user_id},
        {"_id": 0}
    ).sort("created_at", -1).limit(10).to_list(10)
    
    return {
        "user": user,
        "stats": stats,
        "badge": badge_doc,
        "recent_walks": recent_walks,
        "recent_bookings": recent_bookings,
        "experiences": experiences,
        "feedbacks_sent": feedbacks_sent,
        "feedbacks_received": feedbacks_received,
        "admin_messages": admin_messages,
        "reviews": reviews
    }


# ============== Walk Reviews API ==============

@api_router.post("/reviews", response_model=WalkReview)
async def create_review(
    review_data: WalkReviewCreate,
    current_user: User = Depends(get_current_user)
):
    """Create a review for an attended walk"""
    # Validate rating
    if review_data.rating < 1 or review_data.rating > 5:
        raise HTTPException(status_code=400, detail="Rating must be between 1 and 5")
    
    # Check if walk exists
    walk = await db.walks.find_one({"walk_id": review_data.walk_id}, {"_id": 0})
    if not walk:
        raise HTTPException(status_code=404, detail="Walk not found")
    
    # Check if user has booked this walk
    booking = await db.bookings.find_one({
        "walk_id": review_data.walk_id,
        "user_id": current_user.user_id
    })
    if not booking:
        raise HTTPException(status_code=403, detail="You can only review walks you have booked")
    
    # Check if already reviewed
    existing_review = await db.reviews.find_one({
        "walk_id": review_data.walk_id,
        "user_id": current_user.user_id
    })
    if existing_review:
        raise HTTPException(status_code=400, detail="You have already reviewed this walk")
    
    review_id = f"review_{uuid.uuid4().hex[:12]}"
    
    # Get user pseudonym for privacy
    user_doc = await db.users.find_one({"user_id": current_user.user_id}, {"pseudonym": 1})
    user_pseudonym = user_doc.get("pseudonym") if user_doc else None
    display_name = user_pseudonym or current_user.name
    
    review = {
        "review_id": review_id,
        "walk_id": review_data.walk_id,
        "walk_title": walk.get("title", "Unknown Walk"),
        "user_id": current_user.user_id,
        "user_name": display_name,  # Use pseudonym for privacy
        "rating": review_data.rating,
        "comment": review_data.comment.strip() if review_data.comment else None,
        "created_at": datetime.now(timezone.utc)
    }
    
    await db.reviews.insert_one(review)
    return WalkReview(**review)


@api_router.get("/reviews/my")
async def get_my_reviews(
    current_user: User = Depends(get_current_user)
):
    """Get current user's reviews"""
    reviews = await db.reviews.find(
        {"user_id": current_user.user_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    return reviews


@api_router.get("/reviews/walk/{walk_id}")
async def get_walk_reviews(
    walk_id: str,
    current_user: User = Depends(get_current_user)
):
    """Get all reviews for a specific walk"""
    reviews = await db.reviews.find(
        {"walk_id": walk_id},
        {"_id": 0}
    ).sort("created_at", -1).to_list(100)
    
    return reviews


# ============== ADMIN STATISTICS ==============

def is_admin(user: User) -> bool:
    """Check if user is an admin based on email"""
    return user.email.lower() in [e.lower() for e in ADMIN_EMAILS]

@api_router.get("/admin/statistics")
async def get_admin_statistics(
    start_date: str = None,
    end_date: str = None,
    current_user: User = Depends(get_current_user)
):
    """Get app statistics (admin only)"""
    # Check if user is admin
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Build date filter for walks
    walk_date_filter = {}
    if start_date:
        walk_date_filter["$gte"] = start_date
    if end_date:
        walk_date_filter["$lte"] = end_date
    
    walk_query = {}
    if walk_date_filter:
        walk_query["date"] = walk_date_filter
    
    # Get all walks in date range
    walks = await db.walks.find(walk_query, {"_id": 0}).to_list(10000)
    
    # Build date filter for users (based on created_at)
    user_date_filter = {}
    if start_date:
        from datetime import datetime
        try:
            start_dt = datetime.fromisoformat(start_date + "T00:00:00")
            user_date_filter["$gte"] = start_dt
        except ValueError:
            pass
    if end_date:
        from datetime import datetime
        try:
            end_dt = datetime.fromisoformat(end_date + "T23:59:59")
            user_date_filter["$lte"] = end_dt
        except ValueError:
            pass
    
    user_query = {}
    if user_date_filter:
        user_query["created_at"] = user_date_filter
    
    # Get users (filtered by date if specified)
    users = await db.users.find(user_query, {"_id": 0}).to_list(10000)
    
    # Build date filter for bookings (based on booked_at)
    booking_query = {"status": "active"}
    if user_date_filter:
        booking_query["booked_at"] = user_date_filter
    
    # Get all bookings
    bookings = await db.bookings.find(booking_query, {"_id": 0}).to_list(10000)
    
    # Build date filter for reviews (based on created_at)
    review_query = {}
    if user_date_filter:
        review_query["created_at"] = user_date_filter
    
    # Get all reviews
    reviews = await db.reviews.find(review_query, {"_id": 0}).to_list(10000)
    
    # === WALK STATISTICS ===
    walks_by_country = {}
    walks_by_city = {}
    walks_by_neighborhood = {}
    walks_by_community = {}
    
    for walk in walks:
        # By Country (from organizer profile or walk data)
        country = walk.get("country") or "Unknown"
        walks_by_country[country] = walks_by_country.get(country, 0) + 1
        
        # By City
        city = walk.get("city") or "Unknown"
        walks_by_city[city] = walks_by_city.get(city, 0) + 1
        
        # By Neighborhood
        neighborhood = walk.get("neighborhood") or "Unknown"
        walks_by_neighborhood[neighborhood] = walks_by_neighborhood.get(neighborhood, 0) + 1
        
        # By Community (from conditions)
        conditions = walk.get("conditions") or {}
        community = conditions.get("country_region") or "All"
        walks_by_community[community] = walks_by_community.get(community, 0) + 1
    
    # === USER STATISTICS ===
    users_by_sex = {}
    users_by_age_group = {}
    users_by_city = {}
    
    for user in users:
        # By Sex
        sex = user.get("sex") or "Unknown"
        users_by_sex[sex] = users_by_sex.get(sex, 0) + 1
        
        # By Age Group - Non-overlapping ranges: 18-24, 25-34, 35-44, 45-54, 55-64, 65-74, 75+
        age = user.get("age")
        if age:
            if age < 25:
                age_group = "18-24"
            elif age < 35:
                age_group = "25-34"
            elif age < 45:
                age_group = "35-44"
            elif age < 55:
                age_group = "45-54"
            elif age < 65:
                age_group = "55-64"
            elif age < 75:
                age_group = "65-74"
            else:
                age_group = "75+"
        else:
            age_group = "Unknown"
        users_by_age_group[age_group] = users_by_age_group.get(age_group, 0) + 1
        
        # By City
        city = user.get("city") or "Unknown"
        users_by_city[city] = users_by_city.get(city, 0) + 1
    
    # === REVIEW STATISTICS ===
    total_reviews = len(reviews)
    avg_rating = 0
    if reviews:
        ratings = [r.get("rating", 0) for r in reviews if r.get("rating")]
        avg_rating = round(sum(ratings) / len(ratings), 1) if ratings else 0
    
    ratings_distribution = {1: 0, 2: 0, 3: 0, 4: 0, 5: 0}
    for review in reviews:
        rating = review.get("rating")
        if rating and 1 <= rating <= 5:
            ratings_distribution[rating] += 1
    
    return {
        "summary": {
            "total_walks": len(walks),
            "total_users": len(users),
            "total_bookings": len(bookings),
            "total_reviews": total_reviews,
            "average_rating": avg_rating
        },
        "walks_by_country": walks_by_country,
        "walks_by_city": walks_by_city,
        "walks_by_neighborhood": walks_by_neighborhood,
        "walks_by_community": walks_by_community,
        "users_by_sex": users_by_sex,
        "users_by_age_group": users_by_age_group,
        "users_by_city": users_by_city,
        "ratings_distribution": ratings_distribution,
        "active_users": await get_active_users_list()
    }

async def get_active_users_list():
    """Get list of active users with their walk statistics"""
    from datetime import date
    today = date.today().isoformat()
    
    # Get all active users (not suspended/blocked)
    users = await db.users.find(
        {"status": {"$nin": ["suspended", "blocked"]}, "is_cancelled": {"$ne": True}},
        {"_id": 0, "password_hash": 0}
    ).to_list(10000)
    
    active_users = []
    for user in users:
        user_id = user.get("user_id")
        
        # Count walks organized by user (past dates only = completed)
        organized_count = await db.walks.count_documents({
            "organizer_id": user_id,
            "date": {"$lt": today}
        })
        
        # Count walks participated in (past dates only = completed, active bookings)
        participated_bookings = await db.bookings.find(
            {"user_id": user_id, "status": "active"},
            {"walk_id": 1}
        ).to_list(10000)
        
        participated_walk_ids = [b["walk_id"] for b in participated_bookings]
        
        participated_count = 0
        if participated_walk_ids:
            participated_count = await db.walks.count_documents({
                "walk_id": {"$in": participated_walk_ids},
                "date": {"$lt": today}
            })
        
        total_walks = organized_count + participated_count
        
        active_users.append({
            "user_id": user_id,
            "full_name": user.get("name", "Unknown"),
            "pseudonym": user.get("pseudonym"),
            "email": user.get("email"),
            "city": user.get("city", "Not set"),
            "neighborhood": user.get("neighborhood", "Not set"),
            "walks_organized": organized_count,
            "walks_participated": participated_count,
            "walks_achieved": total_walks,
            "badge_level": get_badge_level(total_walks),
            "created_at": user.get("created_at"),
            "status": user.get("status", "active")
        })
    
    # Sort by walks_achieved descending
    active_users.sort(key=lambda x: x["walks_achieved"], reverse=True)
    
    return active_users

@api_router.get("/admin/check")
async def check_admin_status(current_user: User = Depends(get_current_user)):
    """Check if current user is an admin"""
    return {"is_admin": is_admin(current_user)}


@api_router.post("/admin/test-retention-notifications")
async def test_retention_notifications(current_user: User = Depends(get_current_user)):
    """
    [ADMIN ONLY] Manually trigger retention notifications for testing.
    This will send notifications to users who haven't logged in for 3+ days.
    """
    # Check if user is admin
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        logging.info(f"🔔 Admin {current_user.email} triggered retention notification test")
        
        # Find users who haven't logged in for 3+ days
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        
        inactive_users = await db.users.find({
            "$or": [
                {"lastLoginAt": {"$lt": three_days_ago}},
                {"lastLoginAt": {"$exists": False}}
            ],
            "notifications_enabled": {"$ne": False}
        }).to_list(length=100)
        
        results = {
            "total_inactive_users": len(inactive_users),
            "notifications_sent": 0,
            "notifications_skipped": 0,
            "details": []
        }
        
        for user in inactive_users:
            user_id = user.get("user_id")
            user_name = user.get("name", "Unknown")
            last_login = user.get("lastLoginAt")
            
            # Check for push token
            push_token_doc = await db.push_tokens.find_one({"user_id": user_id})
            has_token = bool(push_token_doc and push_token_doc.get("push_token"))
            
            if has_token:
                success = await send_retention_notification(user)
                if success:
                    results["notifications_sent"] += 1
                    results["details"].append({
                        "user": user_name,
                        "status": "sent",
                        "last_login": str(last_login) if last_login else "never"
                    })
                else:
                    results["notifications_skipped"] += 1
                    results["details"].append({
                        "user": user_name,
                        "status": "skipped (already sent recently)",
                        "last_login": str(last_login) if last_login else "never"
                    })
            else:
                results["notifications_skipped"] += 1
                results["details"].append({
                    "user": user_name,
                    "status": "skipped (no push token)",
                    "last_login": str(last_login) if last_login else "never"
                })
            
            # Small delay between notifications
            await asyncio.sleep(0.3)
        
        logging.info(f"✅ Retention test complete: {results['notifications_sent']} sent, {results['notifications_skipped']} skipped")
        
        return results
        
    except Exception as e:
        logging.error(f"Error in test retention notifications: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@api_router.get("/admin/retention-stats")
async def get_retention_stats(current_user: User = Depends(get_current_user)):
    """
    [ADMIN ONLY] Get statistics about user retention and notifications sent.
    """
    # Check if user is admin
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Count total users
        total_users = await db.users.count_documents({})
        
        # Count users with lastLoginAt
        users_with_login_tracking = await db.users.count_documents({"lastLoginAt": {"$exists": True}})
        
        # Count inactive users (3+ days)
        three_days_ago = datetime.now(timezone.utc) - timedelta(days=3)
        inactive_users = await db.users.count_documents({
            "$or": [
                {"lastLoginAt": {"$lt": three_days_ago}},
                {"lastLoginAt": {"$exists": False}}
            ]
        })
        
        # Count active users (logged in within 3 days)
        active_users = await db.users.count_documents({
            "lastLoginAt": {"$gte": three_days_ago}
        })
        
        # Count retention notifications sent
        total_notifications = await db.retention_notifications.count_documents({})
        
        # Notifications sent in last 7 days
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        recent_notifications = await db.retention_notifications.count_documents({
            "sent_at": {"$gte": week_ago}
        })
        
        # Users with push tokens
        users_with_tokens = await db.push_tokens.count_documents({})
        
        return {
            "total_users": total_users,
            "users_with_login_tracking": users_with_login_tracking,
            "active_users_3_days": active_users,
            "inactive_users_3_days": inactive_users,
            "users_with_push_tokens": users_with_tokens,
            "total_retention_notifications_sent": total_notifications,
            "notifications_sent_last_7_days": recent_notifications
        }
        
    except Exception as e:
        logging.error(f"Error getting retention stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============== Walk Tracking Stats Endpoints ==============

class WalkStatsSync(BaseModel):
    distance: float  # meters
    steps: int
    calories: int
    duration: float  # seconds
    country: str = "Unknown"

@api_router.post("/walk-stats/sync")
async def sync_walk_stats(
    stats: WalkStatsSync,
    current_user: User = Depends(get_current_user)
):
    """
    Sync aggregate walk tracking stats to server for admin statistics.
    Only aggregate data is stored - no route/location data for privacy.
    """
    try:
        # Store the walk stats
        walk_stat = {
            "user_id": current_user.user_id,
            "country": stats.country or current_user.country or "Unknown",
            "distance": stats.distance,
            "steps": stats.steps,
            "calories": stats.calories,
            "duration": stats.duration,
            "synced_at": datetime.now(timezone.utc)
        }
        
        await db.walk_tracking_stats.insert_one(walk_stat)
        
        logging.info(f"📊 Walk stats synced: user={current_user.user_id}, distance={stats.distance}m, steps={stats.steps}")
        
        return {"message": "Stats synced successfully"}
    except Exception as e:
        logging.error(f"Error syncing walk stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to sync stats")

@api_router.get("/admin/walk-tracking-stats")
async def get_walk_tracking_stats(
    current_user: User = Depends(get_current_user)
):
    """
    Get aggregated walk tracking statistics by country (admin only).
    Used for marketing and app promotion.
    """
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    try:
        # Aggregate stats by country
        pipeline = [
            {
                "$group": {
                    "_id": "$country",
                    "total_walks": {"$sum": 1},
                    "total_distance": {"$sum": "$distance"},
                    "total_steps": {"$sum": "$steps"},
                    "total_calories": {"$sum": "$calories"},
                    "total_duration": {"$sum": "$duration"},
                    "unique_users": {"$addToSet": "$user_id"}
                }
            },
            {
                "$project": {
                    "country": "$_id",
                    "total_walks": 1,
                    "total_distance": 1,
                    "total_steps": 1,
                    "total_calories": 1,
                    "total_duration": 1,
                    "unique_users_count": {"$size": "$unique_users"}
                }
            },
            {"$sort": {"total_distance": -1}}
        ]
        
        stats_by_country = await db.walk_tracking_stats.aggregate(pipeline).to_list(1000)
        
        # Get unique users count first
        unique_users = await db.walk_tracking_stats.distinct("user_id")
        
        # Calculate global totals
        global_totals = {
            "total_walks": sum(s["total_walks"] for s in stats_by_country),
            "total_distance_km": round(sum(s["total_distance"] for s in stats_by_country) / 1000, 2),
            "total_steps": sum(s["total_steps"] for s in stats_by_country),
            "total_calories": sum(s["total_calories"] for s in stats_by_country),
            "total_duration_hours": round(sum(s["total_duration"] for s in stats_by_country) / 3600, 1),
            "total_duration_seconds": sum(s["total_duration"] for s in stats_by_country),
            "total_countries": len(stats_by_country),
            "total_unique_users": len(unique_users)
        }
        
        # Format country stats for response
        country_stats = []
        for stat in stats_by_country:
            country_stats.append({
                "country": stat.get("country", "Unknown"),
                "total_walks": stat["total_walks"],
                "total_distance_km": round(stat["total_distance"] / 1000, 2),
                "total_steps": stat["total_steps"],
                "total_calories": stat["total_calories"],
                "total_duration_hours": round(stat["total_duration"] / 3600, 1),
                "unique_users": stat["unique_users_count"]
            })
        
        return {
            "global_totals": global_totals,
            "by_country": country_stats
        }
    except Exception as e:
        logging.error(f"Error getting walk tracking stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to get stats")


@api_router.post("/terms/accept")
async def accept_terms(current_user: User = Depends(get_current_user)):
    """Record that user has read and accepted terms"""
    now = datetime.now(timezone.utc)
    await db.users.update_one(
        {"user_id": current_user.user_id},
        {"$set": {
            "terms_accepted": True,
            "terms_accepted_at": now
        }}
    )
    return {
        "success": True,
        "terms_accepted": True,
        "terms_accepted_at": now.isoformat()
    }

@api_router.get("/terms/status")
async def get_terms_status(current_user: User = Depends(get_current_user)):
    """Get user's terms acceptance status including whether re-acceptance is needed"""
    user = await db.users.find_one({"user_id": current_user.user_id})
    
    terms_accepted = user.get("terms_accepted", False)
    terms_accepted_at = user.get("terms_accepted_at")
    
    # Check if user needs to re-accept terms (monthly re-acceptance required)
    needs_reacceptance = False
    days_since_acceptance = None
    
    if not terms_accepted:
        needs_reacceptance = True
    elif terms_accepted_at:
        # Calculate days since last acceptance
        if isinstance(terms_accepted_at, datetime):
            accepted_dt = terms_accepted_at
        else:
            # Handle string format if stored that way
            try:
                accepted_dt = datetime.fromisoformat(str(terms_accepted_at).replace('Z', '+00:00'))
            except (ValueError, TypeError):
                accepted_dt = None
        
        if accepted_dt:
            # Ensure timezone awareness
            if accepted_dt.tzinfo is None:
                accepted_dt = accepted_dt.replace(tzinfo=timezone.utc)
            
            now = datetime.now(timezone.utc)
            days_since = (now - accepted_dt).days
            days_since_acceptance = days_since
            
            # Require re-acceptance every 30 days
            if days_since >= 30:
                needs_reacceptance = True
    else:
        # terms_accepted is True but no timestamp - require re-acceptance
        needs_reacceptance = True
    
    return {
        "terms_accepted": terms_accepted,
        "terms_accepted_at": terms_accepted_at,
        "needs_reacceptance": needs_reacceptance,
        "days_since_acceptance": days_since_acceptance,
        "reacceptance_period_days": 30
    }


# ============== Admin Moderation Endpoints ==============

class UserStatusUpdate(BaseModel):
    status: str  # "active", "suspended", "blocked"
    reason: Optional[str] = None

@api_router.get("/admin/users")
async def get_all_users(
    status: str = None,  # Filter by status: "active", "suspended", "blocked"
    current_user: User = Depends(get_current_user)
):
    """Get all users with optional status filter (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    query = {}
    if status:
        query["status"] = status
    
    users = await db.users.find(query, {"_id": 0, "password_hash": 0}).sort("created_at", -1).to_list(10000)
    return users

@api_router.put("/admin/users/{user_id}/status")
async def update_user_status(
    user_id: str,
    status_update: UserStatusUpdate,
    current_user: User = Depends(get_current_user)
):
    """Update user status (suspend/block/activate) - Admin only"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Validate status
    valid_statuses = ["active", "suspended", "blocked"]
    if status_update.status not in valid_statuses:
        raise HTTPException(status_code=400, detail=f"Invalid status. Must be one of: {valid_statuses}")
    
    # Don't allow admins to block themselves
    if user_id == current_user.user_id:
        raise HTTPException(status_code=400, detail="Cannot change your own status")
    
    # Check if user exists
    user = await db.users.find_one({"user_id": user_id})
    if not user:
        raise HTTPException(status_code=404, detail="User not found")
    
    # Update user status
    update_data = {
        "status": status_update.status,
        "status_updated_at": datetime.now(timezone.utc),
        "status_updated_by": current_user.user_id
    }
    if status_update.reason:
        update_data["status_reason"] = status_update.reason
    
    await db.users.update_one(
        {"user_id": user_id},
        {"$set": update_data}
    )
    
    # Log the action
    await db.moderation_log.insert_one({
        "action": "user_status_change",
        "target_user_id": user_id,
        "target_user_email": user.get("email"),
        "new_status": status_update.status,
        "reason": status_update.reason,
        "admin_id": current_user.user_id,
        "admin_email": current_user.email,
        "created_at": datetime.now(timezone.utc)
    })
    
    logging.info(f"🔒 Admin {current_user.email} changed user {user.get('email')} status to {status_update.status}")
    
    return {
        "message": f"User status updated to {status_update.status}",
        "user_id": user_id,
        "new_status": status_update.status
    }

class ExperienceVisibilityUpdate(BaseModel):
    is_visible: bool
    reason: Optional[str] = None

@api_router.put("/admin/experiences/{experience_id}/visibility")
async def update_experience_visibility(
    experience_id: str,
    visibility_update: ExperienceVisibilityUpdate,
    current_user: User = Depends(get_current_user)
):
    """Hide or show a walker experience (admin moderation) - Admin only"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Check if experience exists
    experience = await db.experiences.find_one({"experience_id": experience_id})
    if not experience:
        raise HTTPException(status_code=404, detail="Experience not found")
    
    # Update visibility
    await db.experiences.update_one(
        {"experience_id": experience_id},
        {"$set": {
            "is_visible": visibility_update.is_visible,
            "visibility_updated_at": datetime.now(timezone.utc),
            "visibility_updated_by": current_user.user_id,
            "visibility_reason": visibility_update.reason
        }}
    )
    
    # Log the action
    await db.moderation_log.insert_one({
        "action": "experience_visibility_change",
        "experience_id": experience_id,
        "experience_user_id": experience.get("user_id"),
        "new_visibility": visibility_update.is_visible,
        "reason": visibility_update.reason,
        "admin_id": current_user.user_id,
        "admin_email": current_user.email,
        "created_at": datetime.now(timezone.utc)
    })
    
    action = "shown" if visibility_update.is_visible else "hidden"
    logging.info(f"👁️ Admin {current_user.email} {action} experience {experience_id}")
    
    return {
        "message": f"Experience {action}",
        "experience_id": experience_id,
        "is_visible": visibility_update.is_visible
    }

@api_router.delete("/admin/experiences/{experience_id}")
async def delete_experience_admin(
    experience_id: str,
    reason: str = None,
    current_user: User = Depends(get_current_user)
):
    """Permanently delete a walker experience (admin moderation) - Admin only"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    # Check if experience exists
    experience = await db.experiences.find_one({"experience_id": experience_id})
    if not experience:
        raise HTTPException(status_code=404, detail="Experience not found")
    
    # Log before deletion
    await db.moderation_log.insert_one({
        "action": "experience_deleted",
        "experience_id": experience_id,
        "experience_user_id": experience.get("user_id"),
        "experience_text": experience.get("text", "")[:200],  # Store first 200 chars
        "reason": reason,
        "admin_id": current_user.user_id,
        "admin_email": current_user.email,
        "created_at": datetime.now(timezone.utc)
    })
    
    # Delete the experience
    await db.experiences.delete_one({"experience_id": experience_id})
    
    logging.info(f"🗑️ Admin {current_user.email} deleted experience {experience_id}")
    
    return {"message": "Experience deleted", "experience_id": experience_id}

@api_router.get("/admin/moderation-log")
async def get_moderation_log(
    limit: int = 100,
    current_user: User = Depends(get_current_user)
):
    """Get moderation action log (admin only)"""
    if not is_admin(current_user):
        raise HTTPException(status_code=403, detail="Admin access required")
    
    logs = await db.moderation_log.find(
        {},
        {"_id": 0}
    ).sort("created_at", -1).limit(limit).to_list(limit)
    
    return logs


# ============== GEONAMES API - Countries & Cities ==============

@api_router.get("/geo/countries")
async def get_countries():
    """Get list of all countries from GeoNames"""
    try:
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"http://api.geonames.org/countryInfoJSON?username={GEONAMES_USERNAME}",
                timeout=10.0
            )
            data = response.json()
            
            if "geonames" not in data:
                # Fallback to static list if API fails
                return {"countries": get_static_countries()}
            
            countries = []
            for country in data["geonames"]:
                countries.append({
                    "name": country.get("countryName"),
                    "code": country.get("countryCode"),
                    "geonameId": country.get("geonameId")
                })
            
            # Sort alphabetically
            countries.sort(key=lambda x: x["name"])
            return {"countries": countries}
    except Exception as e:
        logging.error(f"GeoNames API error: {e}")
        # Return fallback static list
        return {"countries": get_static_countries()}

@api_router.get("/geo/cities/{country_code}")
async def get_cities_by_country(country_code: str, query: str = ""):
    """Get cities for a specific country from GeoNames
    
    Args:
        country_code: ISO 2-letter country code (e.g., 'DZ' for Algeria)
        query: Optional search query to filter cities
    """
    try:
        async with httpx.AsyncClient() as client:
            # Use GeoNames search to get cities
            params = {
                "country": country_code.upper(),
                "featureClass": "P",  # Populated places
                "maxRows": 1000,
                "username": GEONAMES_USERNAME,
                "orderby": "population",
                "cities": "cities1000"  # Cities with population > 1000
            }
            
            if query:
                params["name_startsWith"] = query
            
            response = await client.get(
                "http://api.geonames.org/searchJSON",
                params=params,
                timeout=15.0
            )
            data = response.json()
            
            if "geonames" not in data:
                logging.error(f"GeoNames response: {data}")
                return {"cities": [], "error": "Failed to fetch cities"}
            
            cities = []
            seen_names = set()  # Avoid duplicates
            
            for place in data["geonames"]:
                city_name = place.get("name") or place.get("toponymName")
                if city_name and city_name not in seen_names:
                    seen_names.add(city_name)
                    cities.append({
                        "name": city_name,
                        "adminName1": place.get("adminName1", ""),  # State/Province
                        "population": place.get("population", 0),
                        "geonameId": place.get("geonameId")
                    })
            
            # Sort by population (largest first), then alphabetically
            cities.sort(key=lambda x: (-x.get("population", 0), x["name"]))
            
            return {"cities": cities, "total": len(cities)}
    except Exception as e:
        logging.error(f"GeoNames cities API error: {e}")
        return {"cities": [], "error": str(e)}

@api_router.get("/geo/search-cities")
async def search_cities(query: str, country_code: Optional[str] = None):
    """Search for cities by name with optional country filter"""
    if len(query) < 2:
        return {"cities": []}
    
    try:
        async with httpx.AsyncClient() as client:
            params = {
                "name_startsWith": query,
                "featureClass": "P",
                "maxRows": 50,
                "username": GEONAMES_USERNAME,
                "orderby": "relevance"
            }
            
            if country_code:
                params["country"] = country_code.upper()
            
            response = await client.get(
                "http://api.geonames.org/searchJSON",
                params=params,
                timeout=10.0
            )
            data = response.json()
            
            if "geonames" not in data:
                return {"cities": []}
            
            cities = []
            for place in data["geonames"]:
                cities.append({
                    "name": place.get("name") or place.get("toponymName"),
                    "country": place.get("countryName"),
                    "countryCode": place.get("countryCode"),
                    "adminName1": place.get("adminName1", ""),
                    "population": place.get("population", 0)
                })
            
            return {"cities": cities}
    except Exception as e:
        logging.error(f"GeoNames search error: {e}")
        return {"cities": []}

def get_static_countries():
    """Fallback list of countries if GeoNames API fails"""
    return [
        {"name": "Algeria", "code": "DZ"},
        {"name": "Argentina", "code": "AR"},
        {"name": "Australia", "code": "AU"},
        {"name": "Austria", "code": "AT"},
        {"name": "Belgium", "code": "BE"},
        {"name": "Brazil", "code": "BR"},
        {"name": "Canada", "code": "CA"},
        {"name": "Chile", "code": "CL"},
        {"name": "China", "code": "CN"},
        {"name": "Colombia", "code": "CO"},
        {"name": "Czech Republic", "code": "CZ"},
        {"name": "Denmark", "code": "DK"},
        {"name": "Egypt", "code": "EG"},
        {"name": "Finland", "code": "FI"},
        {"name": "France", "code": "FR"},
        {"name": "Germany", "code": "DE"},
        {"name": "Greece", "code": "GR"},
        {"name": "Hungary", "code": "HU"},
        {"name": "India", "code": "IN"},
        {"name": "Indonesia", "code": "ID"},
        {"name": "Ireland", "code": "IE"},
        {"name": "Israel", "code": "IL"},
        {"name": "Italy", "code": "IT"},
        {"name": "Japan", "code": "JP"},
        {"name": "Malaysia", "code": "MY"},
        {"name": "Mexico", "code": "MX"},
        {"name": "Morocco", "code": "MA"},
        {"name": "Netherlands", "code": "NL"},
        {"name": "New Zealand", "code": "NZ"},
        {"name": "Norway", "code": "NO"},
        {"name": "Philippines", "code": "PH"},
        {"name": "Poland", "code": "PL"},
        {"name": "Portugal", "code": "PT"},
        {"name": "Romania", "code": "RO"},
        {"name": "Russia", "code": "RU"},
        {"name": "Saudi Arabia", "code": "SA"},
        {"name": "Singapore", "code": "SG"},
        {"name": "South Africa", "code": "ZA"},
        {"name": "South Korea", "code": "KR"},
        {"name": "Spain", "code": "ES"},
        {"name": "Sweden", "code": "SE"},
        {"name": "Switzerland", "code": "CH"},
        {"name": "Thailand", "code": "TH"},
        {"name": "Tunisia", "code": "TN"},
        {"name": "Turkey", "code": "TR"},
        {"name": "United Arab Emirates", "code": "AE"},
        {"name": "United Kingdom", "code": "GB"},
        {"name": "United States", "code": "US"},
        {"name": "Vietnam", "code": "VN"}
    ]


# Include the router in the main app
app.include_router(api_router)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logging.info("🚀 Starting WalkApp backend...")
    
    # Create database indexes for better performance
    try:
        logging.info("📊 Creating database indexes...")
        
        # Users collection indexes
        await db.users.create_index("user_id", unique=True)
        await db.users.create_index("email")
        await db.users.create_index("city")
        
        # Walks collection indexes
        await db.walks.create_index("walk_id", unique=True)
        await db.walks.create_index("organizer_id")
        await db.walks.create_index("date")
        await db.walks.create_index("city")
        await db.walks.create_index([("city", 1), ("date", 1)])
        
        # Bookings collection indexes
        await db.bookings.create_index("booking_id", unique=True)
        await db.bookings.create_index("user_id")
        await db.bookings.create_index("walk_id")
        await db.bookings.create_index("status")
        await db.bookings.create_index([("user_id", 1), ("status", 1)])
        await db.bookings.create_index([("walk_id", 1), ("status", 1)])
        
        # Feedback collection indexes
        await db.feedback.create_index("feedback_id", unique=True)
        await db.feedback.create_index("from_user_id")
        await db.feedback.create_index("to_user_id")
        await db.feedback.create_index("walk_id")
        
        # Experiences collection indexes
        await db.experiences.create_index("experience_id", unique=True)
        await db.experiences.create_index("user_id")
        await db.experiences.create_index("created_at")
        
        # Sessions collection indexes
        await db.user_sessions.create_index("session_token")
        await db.user_sessions.create_index("user_id")
        await db.user_sessions.create_index("expires_at")
        
        # Admin messages indexes
        await db.admin_messages.create_index("created_at")
        await db.admin_messages.create_index("read")
        
        # Content reports indexes
        await db.content_reports.create_index("created_at")
        await db.content_reports.create_index("status")
        
        # Reviews indexes
        await db.reviews.create_index("walk_id")
        await db.reviews.create_index("reviewer_id")
        
        logging.info("✅ Database indexes created successfully")
    except Exception as e:
        logging.warning(f"⚠️ Index creation warning (may already exist): {e}")
    
    # Start the 24h reminder scheduler
    start_reminder_scheduler()
    
    # Run initial check for reminders
    await check_and_send_24h_reminders()
    
    logging.info("✅ WalkApp backend started successfully")

@app.on_event("shutdown")
async def shutdown_db_client():
    """Cleanup on shutdown"""
    scheduler.shutdown()
    client.close()
    logging.info("🛑 WalkApp backend shut down")
