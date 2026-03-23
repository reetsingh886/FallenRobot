import threading
import time
from typing import Union

from sqlalchemy import BigInteger, Boolean, Column, String, UnicodeText

from FallenRobot.modules.sql import BASE, SESSION


class ChatAccessConnectionSettings(BASE):
    __tablename__ = "access_connection"
    chat_id = Column(String(14), primary_key=True)
    allow_connect_to_chat = Column(Boolean, default=True)

    def __init__(self, chat_id, allow_connect_to_chat):
        self.chat_id = str(chat_id)
        self.allow_connect_to_chat = allow_connect_to_chat  # ✅ FIXED


class Connection(BASE):
    __tablename__ = "connection"
    user_id = Column(BigInteger, primary_key=True)
    chat_id = Column(String(14))

    def __init__(self, user_id, chat_id):
        self.user_id = user_id
        self.chat_id = str(chat_id)


class ConnectionHistory(BASE):
    __tablename__ = "connection_history"
    user_id = Column(BigInteger, primary_key=True)
    chat_id = Column(String(14), primary_key=True)
    chat_name = Column(UnicodeText)
    conn_time = Column(BigInteger)

    def __init__(self, user_id, chat_id, chat_name, conn_time):
        self.user_id = user_id
        self.chat_id = str(chat_id)
        self.chat_name = str(chat_name)
        self.conn_time = int(conn_time)


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        bind = SESSION.get_bind()
        ChatAccessConnectionSettings.__table__.create(bind=bind, checkfirst=True)
        Connection.__table__.create(bind=bind, checkfirst=True)
        ConnectionHistory.__table__.create(bind=bind, checkfirst=True)
except:
    pass


CHAT_ACCESS_LOCK = threading.RLock()
CONNECTION_INSERTION_LOCK = threading.RLock()
CONNECTION_HISTORY_LOCK = threading.RLock()

HISTORY_CONNECT = {}


def allow_connect_to_chat(chat_id: Union[str, int]) -> bool:
    if not SESSION:
        return False

    try:
        chat_setting = SESSION.query(ChatAccessConnectionSettings).get(str(chat_id))
        if chat_setting:
            return chat_setting.allow_connect_to_chat
        return False
    except:
        return False
    finally:
        SESSION.close()


def set_allow_connect_to_chat(chat_id, setting: bool):
    if not SESSION:
        return

    with CHAT_ACCESS_LOCK:
        chat_setting = SESSION.query(ChatAccessConnectionSettings).get(str(chat_id))
        if not chat_setting:
            chat_setting = ChatAccessConnectionSettings(chat_id, setting)

        chat_setting.allow_connect_to_chat = setting
        SESSION.add(chat_setting)
        SESSION.commit()


def connect(user_id, chat_id):
    if not SESSION:
        return False

    with CONNECTION_INSERTION_LOCK:
        prev = SESSION.query(Connection).get(int(user_id))
        if prev:
            SESSION.delete(prev)

        connect_to_chat = Connection(int(user_id), chat_id)
        SESSION.add(connect_to_chat)
        SESSION.commit()
        return True


def get_connected_chat(user_id):
    if not SESSION:
        return None

    try:
        return SESSION.query(Connection).get(int(user_id))
    finally:
        SESSION.close()


def curr_connection(chat_id):
    if not SESSION:
        return None

    try:
        return SESSION.query(Connection).get(str(chat_id))
    finally:
        SESSION.close()


def disconnect(user_id):
    if not SESSION:
        return False

    with CONNECTION_INSERTION_LOCK:
        disconnect = SESSION.query(Connection).get(int(user_id))
        if disconnect:
            SESSION.delete(disconnect)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def add_history_conn(user_id, chat_id, chat_name):
    if not SESSION:
        return

    global HISTORY_CONNECT
    with CONNECTION_HISTORY_LOCK:
        conn_time = int(time.time())

        if int(user_id) not in HISTORY_CONNECT:
            HISTORY_CONNECT[int(user_id)] = {}

        history = ConnectionHistory(int(user_id), str(chat_id), chat_name, conn_time)

        SESSION.merge(history)
        SESSION.commit()

        HISTORY_CONNECT[int(user_id)][conn_time] = {
            "chat_name": chat_name,
            "chat_id": str(chat_id),
        }


def get_history_conn(user_id):
    return HISTORY_CONNECT.get(int(user_id), {})


def clear_history_conn(user_id):
    if not SESSION:
        return False

    global HISTORY_CONNECT

    with CONNECTION_HISTORY_LOCK:
        if int(user_id) not in HISTORY_CONNECT:
            return False

        for x in list(HISTORY_CONNECT[int(user_id)]):
            chat_old = HISTORY_CONNECT[int(user_id)][x]["chat_id"]
            delold = SESSION.query(ConnectionHistory).get(
                (int(user_id), str(chat_old))
            )
            if delold:
                SESSION.delete(delold)

        SESSION.commit()
        HISTORY_CONNECT[int(user_id)] = {}

        return True


def __load_user_history():
    global HISTORY_CONNECT

    if not SESSION:
        HISTORY_CONNECT = {}
        return

    try:
        qall = SESSION.query(ConnectionHistory).all()
        HISTORY_CONNECT = {}

        for x in qall:
            HISTORY_CONNECT.setdefault(x.user_id, {})
            HISTORY_CONNECT[x.user_id][x.conn_time] = {
                "chat_name": x.chat_name,
                "chat_id": x.chat_id,
            }

    except:
        HISTORY_CONNECT = {}

    finally:
        SESSION.close()


__load_user_history()
