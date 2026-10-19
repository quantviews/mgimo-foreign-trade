import asyncio
import smtplib
import unittest
from unittest.mock import patch

from demo.app import email


class DeliveryTests(unittest.TestCase):
    def setUp(self):
        self.user = patch.object(email.settings, "fallback_smtp_user", "backup@example.com")
        self.password = patch.object(email.settings, "fallback_smtp_password", "test-password")
        self.user.start()
        self.password.start()
        self.addCleanup(self.user.stop)
        self.addCleanup(self.password.stop)

    def deliver(self):
        return asyncio.run(email.send_verification("recipient@example.com", "https://example.com/secret"))

    def test_primary_success_does_not_use_backup(self):
        with patch.object(email, "_send_sync") as send:
            self.assertTrue(self.deliver())
            self.assertEqual([c.kwargs for c in send.call_args_list], [{"fallback": False}])

    def test_primary_network_failure_uses_backup(self):
        with patch.object(email, "_send_sync", side_effect=[OSError(), None]) as send:
            self.assertTrue(self.deliver())
            self.assertEqual([c.kwargs for c in send.call_args_list], [{"fallback": False}, {"fallback": True}])

    def test_both_fail(self):
        with patch.object(email, "_send_sync", side_effect=OSError()):
            self.assertFalse(self.deliver())

    def test_recipient_rejection_does_not_retry(self):
        with patch.object(email, "_send_sync", side_effect=smtplib.SMTPRecipientsRefused({})) as send:
            self.assertFalse(self.deliver())
            self.assertEqual(send.call_count, 1)

    def test_backup_sender_and_reply_to(self):
        msg = email._build("recipient@example.com", "https://example.com/secret")
        with patch.object(email.smtplib, "SMTP_SSL") as smtp:
            email._send_sync(msg, fallback=True)
            sent = smtp.return_value.__enter__.return_value.send_message.call_args.args[0]
            self.assertEqual(sent["From"], "backup@example.com")
            self.assertEqual(sent["Reply-To"], email.settings.mail_from)
            self.assertEqual(msg["From"], email.settings.mail_from)


if __name__ == "__main__":
    unittest.main()
