import threading

from sqlalchemy import Column, String
from sqlalchemy.sql.sqltypes import BigInteger

from FallenRobot.modules.sql import BASE, SESSION


class Approvals(BASE):
    __tablename__ = "approval"
    chat_id = Column(String(14), primary_key=True)
    user_id = Column(BigInteger, primary_key=True)

    def __init__(self, chat_id, user_id):
        self.chat_id = str(chat_id)
        self.user_id = user_id

    def __repr__(self):
        return "<Approve %s>" % self.user_id


# ✅ SAFE TABLE CREATE
try:
    if SESSION:
        Approvals.__table__.create(bind=SESSION.get_bind(), checkfirst=True)
except:
    pass


APPROVE_INSERTION_LOCK = threading.RLock()


def approve(chat_id, user_id):
    if not SESSION:
        return

    with APPROVE_INSERTION_LOCK:
        approve_user = Approvals(str(chat_id), user_id)
        SESSION.add(approve_user)
        SESSION.commit()


def is_approved(chat_id, user_id):
    if not SESSION:
        return None

    try:
        return SESSION.query(Approvals).get((str(chat_id), user_id))
    except:
        return None
    finally:
        SESSION.close()


def disapprove(chat_id, user_id):
    if not SESSION:
        return False

    with APPROVE_INSERTION_LOCK:
        disapprove_user = SESSION.query(Approvals).get((str(chat_id), user_id))
        if disapprove_user:
            SESSION.delete(disapprove_user)
            SESSION.commit()
            return True
        else:
            SESSION.close()
            return False


def list_approved(chat_id):
    if not SESSION:
        return []

    try:
        return (
            SESSION.query(Approvals)
            .filter(Approvals.chat_id == str(chat_id))
            .order_by(Approvals.user_id.asc())
            .all()
        )
    except:
        return []
    finally:
        SESSION.close()
