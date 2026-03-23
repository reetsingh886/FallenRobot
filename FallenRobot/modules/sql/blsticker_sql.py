import threading

from sqlalchemy import BigInteger, Column, String, UnicodeText, distinct, func

from FallenRobot.modules.sql import BASE, SESSION


class StickersFilters(BASE):
    __tablename__ = "blacklist_stickers"
    chat_id = Column(String(14), primary_key=True)
    trigger = Column(UnicodeText, primary_key=True, nullable=False)

    def __init__(self, chat_id, trigger):
        self.chat_id = str(chat_id)
        self.trigger = trigger


class StickerSettings(BASE):
    __tablename__ = "blsticker_settings"
    chat_id = Column(String(14), primary_key=True)
    blacklist_type = Column(BigInteger, default=1)
    value = Column(UnicodeText, default="0")

    def __init__(self, chat_id, blacklist_type=1, value="0"):
        self.chat_id = str(chat_id)
        self.blacklist_type = blacklist_type
        self.value = value


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        bind = SESSION.get_bind()
        StickersFilters.__table__.create(bind=bind, checkfirst=True)
        StickerSettings.__table__.create(bind=bind, checkfirst=True)
except:
    pass


STICKERS_FILTER_INSERTION_LOCK = threading.RLock()
STICKSET_FILTER_INSERTION_LOCK = threading.RLock()

CHAT_STICKERS = {}
CHAT_BLSTICK_BLACKLISTS = {}


def add_to_stickers(chat_id, trigger):
    if not SESSION:
        return

    with STICKERS_FILTER_INSERTION_LOCK:
        stickers_filt = StickersFilters(str(chat_id), trigger)

        SESSION.merge(stickers_filt)
        SESSION.commit()

        CHAT_STICKERS.setdefault(str(chat_id), set()).add(trigger)


def rm_from_stickers(chat_id, trigger):
    if not SESSION:
        return False

    with STICKERS_FILTER_INSERTION_LOCK:
        stickers_filt = SESSION.query(StickersFilters).get((str(chat_id), trigger))
        if stickers_filt:
            CHAT_STICKERS.get(str(chat_id), set()).discard(trigger)
            SESSION.delete(stickers_filt)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def get_chat_stickers(chat_id):
    return CHAT_STICKERS.get(str(chat_id), set())


def num_stickers_filters():
    if not SESSION:
        return 0

    try:
        return SESSION.query(StickersFilters).count()
    except:
        return 0
    finally:
        SESSION.close()


def num_stickers_chat_filters(chat_id):
    if not SESSION:
        return 0

    try:
        return (
            SESSION.query(StickersFilters.chat_id)
            .filter(StickersFilters.chat_id == str(chat_id))
            .count()
        )
    except:
        return 0
    finally:
        SESSION.close()


def num_stickers_filter_chats():
    if not SESSION:
        return 0

    try:
        return SESSION.query(func.count(distinct(StickersFilters.chat_id))).scalar()
    except:
        return 0
    finally:
        SESSION.close()


def set_blacklist_strength(chat_id, blacklist_type, value):
    if not SESSION:
        return

    with STICKSET_FILTER_INSERTION_LOCK:
        curr_setting = SESSION.query(StickerSettings).get(str(chat_id))
        if not curr_setting:
            curr_setting = StickerSettings(
                chat_id, blacklist_type=int(blacklist_type), value=value
            )

        curr_setting.blacklist_type = int(blacklist_type)
        curr_setting.value = str(value)

        CHAT_BLSTICK_BLACKLISTS[str(chat_id)] = {
            "blacklist_type": int(blacklist_type),
            "value": value,
        }

        SESSION.add(curr_setting)
        SESSION.commit()


def get_blacklist_setting(chat_id):
    setting = CHAT_BLSTICK_BLACKLISTS.get(str(chat_id))
    if setting:
        return setting["blacklist_type"], setting["value"]
    return 1, "0"


def __load_CHAT_STICKERS():
    global CHAT_STICKERS

    if not SESSION:
        CHAT_STICKERS = {}
        return

    try:
        chats = SESSION.query(StickersFilters.chat_id).distinct().all()
        for (chat_id,) in chats:
            CHAT_STICKERS[chat_id] = []

        all_filters = SESSION.query(StickersFilters).all()
        for x in all_filters:
            CHAT_STICKERS[x.chat_id].append(x.trigger)

        CHAT_STICKERS = {x: set(y) for x, y in CHAT_STICKERS.items()}

    except:
        CHAT_STICKERS = {}

    finally:
        SESSION.close()


def __load_chat_stickerset_blacklists():
    global CHAT_BLSTICK_BLACKLISTS

    if not SESSION:
        CHAT_BLSTICK_BLACKLISTS = {}
        return

    try:
        chats_settings = SESSION.query(StickerSettings).all()
        for x in chats_settings:
            CHAT_BLSTICK_BLACKLISTS[x.chat_id] = {
                "blacklist_type": x.blacklist_type,
                "value": x.value,
            }

    except:
        CHAT_BLSTICK_BLACKLISTS = {}

    finally:
        SESSION.close()


def migrate_chat(old_chat_id, new_chat_id):
    if not SESSION:
        return

    with STICKERS_FILTER_INSERTION_LOCK:
        chat_filters = (
            SESSION.query(StickersFilters)
            .filter(StickersFilters.chat_id == str(old_chat_id))
            .all()
        )
        for filt in chat_filters:
            filt.chat_id = str(new_chat_id)

        SESSION.commit()


__load_CHAT_STICKERS()
__load_chat_stickerset_blacklists()
