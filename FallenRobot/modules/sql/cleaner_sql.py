import threading

from sqlalchemy import Boolean, Column, UnicodeText

from FallenRobot.modules.sql import BASE, SESSION


class CleanerBlueTextChatSettings(BASE):
    __tablename__ = "cleaner_bluetext_chat_setting"
    chat_id = Column(UnicodeText, primary_key=True)
    is_enable = Column(Boolean, default=False)

    def __init__(self, chat_id, is_enable):
        self.chat_id = str(chat_id)
        self.is_enable = is_enable


class CleanerBlueTextChat(BASE):
    __tablename__ = "cleaner_bluetext_chat_ignore_commands"
    chat_id = Column(UnicodeText, primary_key=True)
    command = Column(UnicodeText, primary_key=True)

    def __init__(self, chat_id, command):
        self.chat_id = str(chat_id)
        self.command = command


class CleanerBlueTextGlobal(BASE):
    __tablename__ = "cleaner_bluetext_global_ignore_commands"
    command = Column(UnicodeText, primary_key=True)

    def __init__(self, command):
        self.command = command


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        bind = SESSION.get_bind()
        CleanerBlueTextChatSettings.__table__.create(bind=bind, checkfirst=True)
        CleanerBlueTextChat.__table__.create(bind=bind, checkfirst=True)
        CleanerBlueTextGlobal.__table__.create(bind=bind, checkfirst=True)
except:
    pass


CLEANER_CHAT_SETTINGS = threading.RLock()
CLEANER_CHAT_LOCK = threading.RLock()
CLEANER_GLOBAL_LOCK = threading.RLock()

CLEANER_CHATS = {}
GLOBAL_IGNORE_COMMANDS = set()


def set_cleanbt(chat_id, is_enable):
    if not SESSION:
        return

    with CLEANER_CHAT_SETTINGS:
        curr = SESSION.query(CleanerBlueTextChatSettings).get(str(chat_id))
        if curr:
            SESSION.delete(curr)

        newcurr = CleanerBlueTextChatSettings(str(chat_id), is_enable)

        SESSION.add(newcurr)
        SESSION.commit()


def chat_ignore_command(chat_id, ignore):
    if not SESSION:
        return False

    ignore = ignore.lower()
    with CLEANER_CHAT_LOCK:
        ignored = SESSION.query(CleanerBlueTextChat).get((str(chat_id), ignore))

        if not ignored:
            CLEANER_CHATS.setdefault(
                str(chat_id),
                {"setting": False, "commands": set()},
            )

            CLEANER_CHATS[str(chat_id)]["commands"].add(ignore)

            ignored = CleanerBlueTextChat(str(chat_id), ignore)
            SESSION.add(ignored)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def chat_unignore_command(chat_id, unignore):
    if not SESSION:
        return False

    unignore = unignore.lower()
    with CLEANER_CHAT_LOCK:
        unignored = SESSION.query(CleanerBlueTextChat).get((str(chat_id), unignore))

        if unignored:
            CLEANER_CHATS.setdefault(
                str(chat_id),
                {"setting": False, "commands": set()},
            )

            CLEANER_CHATS[str(chat_id)]["commands"].discard(unignore)

            SESSION.delete(unignored)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def global_ignore_command(command):
    if not SESSION:
        return False

    command = command.lower()
    with CLEANER_GLOBAL_LOCK:
        ignored = SESSION.query(CleanerBlueTextGlobal).get(str(command))

        if not ignored:
            GLOBAL_IGNORE_COMMANDS.add(command)

            ignored = CleanerBlueTextGlobal(str(command))
            SESSION.add(ignored)
            SESSION.commit()
            return True

        SESSION.close()
        return False


def global_unignore_command(command):
    if not SESSION:
        return False

    command = command.lower()
    with CLEANER_GLOBAL_LOCK:
        unignored = SESSION.query(CleanerBlueTextGlobal).get(str(command))

        if unignored:
            GLOBAL_IGNORE_COMMANDS.discard(command)

            SESSION.delete(unignored)  # ✅ FIXED
            SESSION.commit()
            return True

        SESSION.close()
        return False


def is_command_ignored(chat_id, command):
    if command.lower() in GLOBAL_IGNORE_COMMANDS:
        return True

    if str(chat_id) in CLEANER_CHATS:
        if command.lower() in CLEANER_CHATS[str(chat_id)]["commands"]:
            return True

    return False


def is_enabled(chat_id):
    if not SESSION:
        return False

    try:
        resultcurr = SESSION.query(CleanerBlueTextChatSettings).get(str(chat_id))
        if resultcurr:
            return resultcurr.is_enable
        return False
    finally:
        SESSION.close()


def get_all_ignored(chat_id):
    LOCAL_IGNORE_COMMANDS = CLEANER_CHATS.get(str(chat_id), {}).get("commands", set())
    return GLOBAL_IGNORE_COMMANDS, LOCAL_IGNORE_COMMANDS


def __load_cleaner_list():
    global GLOBAL_IGNORE_COMMANDS
    global CLEANER_CHATS

    if not SESSION:
        GLOBAL_IGNORE_COMMANDS = set()
        CLEANER_CHATS = {}
        return

    try:
        GLOBAL_IGNORE_COMMANDS = {
            x.command for x in SESSION.query(CleanerBlueTextGlobal).all()
        }
    except:
        GLOBAL_IGNORE_COMMANDS = set()
    finally:
        SESSION.close()

    try:
        for x in SESSION.query(CleanerBlueTextChatSettings).all():
            CLEANER_CHATS.setdefault(x.chat_id, {"setting": False, "commands": set()})
            CLEANER_CHATS[x.chat_id]["setting"] = x.is_enable
    except:
        pass
    finally:
        SESSION.close()

    try:
        for x in SESSION.query(CleanerBlueTextChat).all():
            CLEANER_CHATS.setdefault(x.chat_id, {"setting": False, "commands": set()})
            CLEANER_CHATS[x.chat_id]["commands"].add(x.command)
    except:
        pass
    finally:
        SESSION.close()


__load_cleaner_list()
