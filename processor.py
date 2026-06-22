import os
import asyncio
import re
import time
import json
import hashlib
import requests
import subprocess
import boto3
from botocore.config import Config
from telethon import TelegramClient
from telethon.sessions import StringSession

# ── Env ───────────────────────────────────────────────────────────────────────
API_ID             = int(os.environ.get("API_ID", 0))
API_HASH           = os.environ.get("API_HASH", "")
STRING_SESSION     = os.environ.get("STRING_SESSION_1", "")
BOT_TOKEN          = os.environ.get("BOT_TOKEN", "")
E2_ACCESS_KEY_ID   = os.environ.get("E2_ACCESS_KEY_ID", "")
E2_SECRET_ACCESS_KEY = os.environ.get("E2_SECRET_ACCESS_KEY", "")
E2_ENDPOINT        = os.environ.get("E2_ENDPOINT", "")
E2_BUCKET_NAME     = os.environ.get("E2_BUCKET_NAME", "")
E2_REGION          = os.environ.get("E2_REGION", "ap-northeast-1")
CF_AUTH_EMAIL      = os.environ.get("CF_AUTH_EMAIL", "")
CF_AUTH_KEY        = os.environ.get("CF_AUTH_KEY", "")
CF_ACCOUNT_ID      = os.environ.get("CF_ACCOUNT_ID", "")
CF_KV_NAMESPACE_ID = os.environ.get("CF_KV_NAMESPACE_ID", "")
GITHUB_TOKEN       = os.environ.get("GITHUB_TOKEN", "")
WORKER_URL         = os.environ.get("WORKER_URL", "").rstrip("/")
SUPABASE_URL       = "https://guotpdwaswaybjiiezax.supabase.co"
SUPABASE_KEY       = os.environ.get("SUPABASE_KEY", "")
