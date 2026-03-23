import threading

from sqlalchemy import (
BigInteger,
Column,
ForeignKey,
String,
UnicodeText,
UniqueConstraint,
)

from FallenRobot import dispatcher
from FallenRobot.modules.sql import BASE, SESSION

class Users(BASE):
tablename = "users"
user_id = Column(BigInteger, primary_key=True)
username = Column(UnicodeText)

def __init__(self, user_id, username=None):
    self.user_id = user_id
    self.username = username

class Chats(BASE):
tablename = "chats"
chat_id = Column(String(14), primary_key=True)
chat_name = Column(UnicodeText, nullable=False)

def __init__(self, chat_id, chat_name):
    self.chat_id = str(chat_id)
    self.chat_name = chat_name

class ChatMembers(BASE):
tablename = "chat_members"
priv_chat_id = Column(BigInteger, primary_key=True)
chat = Column(
String(14),
ForeignKey("chats.chat_id", onupdate="CASCADE", ondelete="CASCADE"),
nullable=False,
)
user = Column(
BigInteger,
ForeignKey("users.user_id", onupdate="CASCADE", ondelete="CASCADE"),
nullable=False,
)
table_args = (UniqueConstraint("chat", "user", name="_chat_members_uc"),)

def __init__(self, chat, user):
    self.chat = chat
    self.user = user

SAFE TABLE CREATE

try:
if SESSION:
bind = SESSION.get_bind()
Users.table.create(bind=bind, checkfirst=True)
Chats.table.create(bind=bind, checkfirst=True)
ChatMembers.table.create(bind=bind, checkfirst=True)
except Exception:
pass

INSERTION_LOCK = threading.RLock()

def ensure_bot_in_db():
if not SESSION:
return

with INSERTION_LOCK:
    bot = Users(dispatcher.bot.id, dispatcher.bot.username)
    SESSION.merge(bot)
    SESSION.commit()

def update_user(user_id, username, chat_id=None, chat_name=None):
if not SESSION:
return

with INSERTION_LOCK:
    user = SESSION.query(Users).get(user_id)

    if not user:
        user = Users(user_id, username)
        SESSION.add(user)
        SESSION.flush()
    else:
        user.username = username

    if not chat_id or not chat_name:
        SESSION.commit()
        return

    chat = SESSION.query(Chats).get(str(chat_id))

    if not chat:
        chat = Chats(str(chat_id), chat_name)
        SESSION.add(chat)
        SESSION.flush()
    else:
        chat.chat_name = chat_name

    member = (
        SESSION.query(ChatMembers)
        .filter(
            ChatMembers.chat == chat.chat_id,
            ChatMembers.user == user.user_id,
        )
        .first()
    )

    if not member:
        member = ChatMembers(chat.chat_id, user.user_id)
        SESSION.add(member)

    SESSION.commit()

=========================

REQUIRED FUNCTIONS

=========================

def get_all_users():
if not SESSION:
return []
try:
return SESSION.query(Users).all()
except:
return []

def get_user_com_chats(user_id):
if not SESSION:
return []

try:
    chats = (
        SESSION.query(ChatMembers.chat)
        .filter(ChatMembers.user == user_id)
        .all()
    )
    return [chat[0] for chat in chats]
except:
    return []

def get_user_num_chats(user_id):
if not SESSION:
return 0

try:
    return (
        SESSION.query(ChatMembers)
        .filter(ChatMembers.user == user_id)
        .count()
    )
except:
    return 0
