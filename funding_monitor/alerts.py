"""Local alerts: transitions, recovery and optional cooldown reminders."""
import logging
from .models import record


class Alerts:
    def __init__(self, cooldown, publish):
        self.cooldown = cooldown
        self.publish = publish
        self.state = {}

    def check(self, key, name, active, now, wall, detail=""):
        previous, last = self.state.get((key, name), (False, float('-inf')))
        changed = active != previous
        reminder = active and now - last >= self.cooldown
        if changed or reminder:
            status = "active" if active else "recovered"
            self.publish(record("alert", key, {"name": name, "status": status, "detail": detail}, wall))
            logging.warning("%s %s %s %s", name, status, key, detail)
            last = now
        self.state[(key, name)] = (active, last)

    def prune(self, keys):
        self.state = {k: v for k, v in self.state.items() if k[0] in keys}
