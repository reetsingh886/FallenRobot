import threading

from sqlalchemy import BigInteger, Column, String, UnicodeText, distinct, func

from FallenRobot.modules.sql import BASE, SESSION


class BlackListFilters(BASE):
    __tablename__ = "blacklist"
    chat_id = Column(String(14), primary_key=True)
    trigger = Column(UnicodeText, primary_key=True, nullable=False)

    def __init__(self, chat_id, trigger):
        self.chat_id = str(chat_id)
        self.trigger = trigger


class BlacklistSettings(BASE):
    __tablename__ = "blacklist_settings"
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
        BlackListFilters.__table__.create(bind=bind, checkfirst=True)
        BlacklistSettings.__table__.create(bind=bind, checkfirst=True)
except:
    pass


BLACKLIST_FILTER_INSERTION_LOCK = threading.RLock()
BLACKLIST_SETTINGS_INSERTION_LOCK = threading.RLock()

CHAT_BLACKLISTS = {}
CHAT_SETTINGS_BLACKLISTS = {}


def add_to_blacklist(chat_id, trigger):
    if not SESSION:
        return

    with BLACKLIST_FILTER_INSERTION_LOCK:
        blacklist_filt = BlackListFilters(str(chat_id), trigger)

        SESSION.merge(blacklist_filt)
        SESSION.commit()

        CHAT_BLACKLISTS.setdefault(str(chat_id), set()).add(trigger)


def rm_from_blacklist(chat_id, trigger):
    if not SESSION:
        return False

    with BLACKLIST_FILTER_INSERTION_LOCK:
        blacklist_filt = SESSION.query(BlackListFilters).get((str(chat_id), trigger))
        if blacklist_filt:
            CHAT_BLACKLISTS.get(str(chat_id), set()).discard(trigger)
            SESSION.delete(blacklist_filt)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def get_chat_blacklist(chat_id):
    return CHAT_BLACKLISTS.get(str(chat_id), set())


def num_blacklist_filters():
    if not SESSION:
        return 0

    try:
        return SESSION.query(BlackListFilters).count()
    except:
        return 0
    finally:
        SESSION.close()


def num_blacklist_chat_filters(chat_id):
    if not SESSION:
        return 0

    try:
        return (
            SESSION.query(BlackListFilters.chat_id)
            .filter(BlackListFilters.chat_id == str(chat_id))
            .count()
        )
    except:
        return 0
    finally:
        SESSION.close()


def num_blacklist_filter_chats():
    if not SESSION:
        return 0

    try:
        return SESSION.query(func.count(distinct(BlackListFilters.chat_id))).scalar()
    except:
        return 0
    finally:
        SESSION.close()


def set_blacklist_strength(chat_id, blacklist_type, value):
    if not SESSION:
        return

    with BLACKLIST_SETTINGS_INSERTION_LOCK:
        curr_setting = SESSION.query(BlacklistSettings).get(str(chat_id))
        if not curr_setting:
            curr_setting = BlacklistSettings(
                chat_id, blacklist_type=int(blacklist_type), value=value
            )

        curr_setting.blacklist_type = int(blacklist_type)
        curr_setting.value = str(value)

        CHAT_SETTINGS_BLACKLISTS[str(chat_id)] = {
            "blacklist_type": int(blacklist_type),
            "value": value,
        }

        SESSION.add(curr_setting)
        SESSION.commit()


def get_blacklist_setting(chat_id):
    setting = CHAT_SETTINGS_BLACKLISTS.get(str(chat_id))
    if setting:
        return setting["blacklist_type"], setting["value"]
    return 1, "0"


def __load_chat_blacklists():
    global CHAT_BLACKLISTS

    if not SESSION:
        CHAT_BLACKLISTS = {}
        return

    try:
        chats = SESSION.query(BlackListFilters.chat_id).distinct().all()
        for (chat_id,) in chats:
            CHAT_BLACKLISTS[chat_id] = []

        all_filters = SESSION.query(BlackListFilters).all()
        for x in all_filters:
            CHAT_BLACKLISTS[x.chat_id].append(x.trigger)

        CHAT_BLACKLISTS = {x: set(y) for x, y in CHAT_BLACKLISTS.items()}

    except:
        CHAT_BLACKLISTS = {}

    finally:
        SESSION.close()


def __load_chat_settings_blacklists():
    global CHAT_SETTINGS_BLACKLISTS

    if not SESSION:
        CHAT_SETTINGS_BLACKLISTS = {}
        return

    try:
        chats_settings = SESSION.query(BlacklistSettings).all()
        for x in chats_settings:
            CHAT_SETTINGS_BLACKLISTS[x.chat_id] = {
                "blacklist_type": x.blacklist_type,
                "value": x.value,
            }

    except:
        CHAT_SETTINGS_BLACKLISTS = {}

    finally:
        SESSION.close()


def migrate_chat(old_chat_id, new_chat_id):
    if not SESSION:
        return

    with BLACKLIST_FILTER_INSERTION_LOCK:
        chat_filters = (
            SESSION.query(BlackListFilters)
            .filter(BlackListFilters.chat_id == str(old_chat_id))
            .all()
        )
        for filt in chat_filters:
            filt.chat_id = str(new_chat_id)

        SESSION.commit()


__load_chat_blacklists()
__load_chat_settings_blacklists()
