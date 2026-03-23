import threading

from sqlalchemy import Column, String

from FallenRobot.modules.sql import BASE, SESSION


class FallenChats(BASE):
    __tablename__ = "fallen_chats"
    chat_id = Column(String(14), primary_key=True)

    def __init__(self, chat_id):
        self.chat_id = str(chat_id)


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        bind = SESSION.get_bind()
        FallenChats.__table__.create(bind=bind, checkfirst=True)
except:
    pass


INSERTION_LOCK = threading.RLock()


def is_fallen(chat_id):
    if not SESSION:
        return False

    try:
        chat = SESSION.query(FallenChats).get(str(chat_id))
        return bool(chat)
    except:
        return False
    finally:
        SESSION.close()


def set_fallen(chat_id):
    if not SESSION:
        return

    with INSERTION_LOCK:
        fallenchat = SESSION.query(FallenChats).get(str(chat_id))
        if not fallenchat:
            fallenchat = FallenChats(str(chat_id))

        SESSION.add(fallenchat)
        SESSION.commit()


def rem_fallen(chat_id):
    if not SESSION:
        return

    with INSERTION_LOCK:
        fallenchat = SESSION.query(FallenChats).get(str(chat_id))
        if fallenchat:
            SESSION.delete(fallenchat)

        SESSION.commit()
