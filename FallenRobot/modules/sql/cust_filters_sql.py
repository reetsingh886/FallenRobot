import threading

from sqlalchemy import BigInteger, Boolean, Column, String, UnicodeText, distinct, func

from FallenRobot.modules.helper_funcs.msg_types import Types
from FallenRobot.modules.sql import BASE, SESSION


class CustomFilters(BASE):
    __tablename__ = "cust_filters"
    chat_id = Column(String(14), primary_key=True)
    keyword = Column(UnicodeText, primary_key=True, nullable=False)
    reply = Column(UnicodeText, nullable=False)
    is_sticker = Column(Boolean, default=False)
    is_document = Column(Boolean, default=False)
    is_image = Column(Boolean, default=False)
    is_audio = Column(Boolean, default=False)
    is_voice = Column(Boolean, default=False)
    is_video = Column(Boolean, default=False)
    has_buttons = Column(Boolean, default=False)
    has_markdown = Column(Boolean, default=False)

    reply_text = Column(UnicodeText)
    file_type = Column(BigInteger, default=1)
    file_id = Column(UnicodeText)


class Buttons(BASE):
    __tablename__ = "cust_filter_urls"
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    chat_id = Column(String(14))
    keyword = Column(UnicodeText)
    name = Column(UnicodeText)
    url = Column(UnicodeText)
    same_line = Column(Boolean, default=False)


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        bind = SESSION.get_bind()
        CustomFilters.__table__.create(bind=bind, checkfirst=True)
        Buttons.__table__.create(bind=bind, checkfirst=True)
except:
    pass


CUST_FILT_LOCK = threading.RLock()
BUTTON_LOCK = threading.RLock()
CHAT_FILTERS = {}


def add_filter(chat_id, keyword, reply, buttons=None):
    if not SESSION:
        return

    if buttons is None:
        buttons = []

    with CUST_FILT_LOCK:
        prev = SESSION.query(CustomFilters).get((str(chat_id), keyword))
        if prev:
            SESSION.delete(prev)

        filt = CustomFilters(
            chat_id=str(chat_id),
            keyword=keyword,
            reply=reply,
            has_buttons=bool(buttons),
        )

        SESSION.add(filt)
        SESSION.commit()

    for b_name, url, same_line in buttons:
        add_note_button_to_db(chat_id, keyword, b_name, url, same_line)


def remove_filter(chat_id, keyword):
    if not SESSION:
        return False

    with CUST_FILT_LOCK:
        filt = SESSION.query(CustomFilters).get((str(chat_id), keyword))
        if filt:
            SESSION.delete(filt)
            SESSION.commit()
            return True
        return False


def get_chat_filters(chat_id):
    if not SESSION:
        return []

    try:
        return (
            SESSION.query(CustomFilters)
            .filter(CustomFilters.chat_id == str(chat_id))
            .all()
        )
    finally:
        SESSION.close()


def add_note_button_to_db(chat_id, keyword, b_name, url, same_line):
    if not SESSION:
        return

    with BUTTON_LOCK:
        button = Buttons(chat_id, keyword, b_name, url, same_line)
        SESSION.add(button)
        SESSION.commit()


def get_buttons(chat_id, keyword):
    if not SESSION:
        return []

    try:
        return (
            SESSION.query(Buttons)
            .filter(Buttons.chat_id == str(chat_id), Buttons.keyword == keyword)
            .all()
        )
    finally:
        SESSION.close()


def __load_chat_filters():
    global CHAT_FILTERS

    if not SESSION:
        CHAT_FILTERS = {}
        return

    try:
        chats = SESSION.query(CustomFilters.chat_id).distinct().all()
        for (chat_id,) in chats:
            CHAT_FILTERS[chat_id] = []

        all_filters = SESSION.query(CustomFilters).all()
        for x in all_filters:
            CHAT_FILTERS[x.chat_id].append(x.keyword)

    except:
        CHAT_FILTERS = {}

    finally:
        SESSION.close()


__load_chat_filters()
