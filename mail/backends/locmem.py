# -*- coding: utf-8 -*-
"""
Backend for test environment.
"""
import sys

import play.mail.send_mail as mail

from play.mail.backends import BaseEmailBackend

class EmailBackend(BaseEmailBackend):
    """A email backend for use during test sessions.

    The test connection stores email messages in a dummy outbox,
    rather than sending them out on the wire.

    The dummy outbox is accessible through the outbox instance attribute.
    """
    def __init__(self, *args, **kwargs):
        super(EmailBackend, self).__init__(*args, **kwargs)
        if not hasattr(mail, 'outbox'):
            mail.outbox = []

    def send_messages(self, messages):
        """Redirect messages to the dummy outbox"""
        mail.outbox.extend(messages)
        return len(messages)

mail_proxy = EmailBackend