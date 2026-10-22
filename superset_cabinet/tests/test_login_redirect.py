"""Run with Superset's dependencies: python -m unittest discover -s .../tests."""
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from flask import Flask, url_for
from superset_cabinet import api_cabinet as cabinet


class LoginRedirectTest(unittest.TestCase):
    def setUp(self):
        self.app = Flask(__name__)
        self.app.add_url_rule('/login/', endpoint='AuthDBView.login', view_func=lambda: '')
        self.app.add_url_rule('/apikey/', endpoint='ApiKeyView.index', view_func=lambda: '')
        class Builder:
            @property
            def get_url_for_login(self):
                return url_for('AuthDBView.login')
        self.view = cabinet.ApiKeyView()
        self.view.endpoint = 'ApiKeyView'
        self.view.appbuilder = Builder()

    def test_anonymous_return_to_get_page_with_and_without_prefix(self):
        for prefix in ('', '/superset'):
            for path, method in [('/apikey/', 'GET'), ('/apikey/generate', 'POST')]:
                with self.subTest(prefix=prefix, path=path), self.app.test_request_context(
                    path, method=method, environ_overrides={'SCRIPT_NAME': prefix}
                ), patch.object(cabinet, 'current_user', SimpleNamespace(is_authenticated=False)):
                    response = self.view._guard()
                    self.assertEqual(response.status_code, 302)
                    location = urlsplit(response.location)
                    self.assertEqual(location.path, prefix + '/login/')
                    self.assertEqual(parse_qs(location.query), {'next': [prefix + '/apikey/']})

    def test_authenticated_access_still_requires_role(self):
        with patch.object(cabinet, 'current_user', SimpleNamespace(
            is_authenticated=True, roles=[SimpleNamespace(name=cabinet.REQUIRED_ROLE)]
        )):
            self.assertIsNone(self.view._guard())
        with patch.object(cabinet, 'current_user', SimpleNamespace(
            is_authenticated=True, roles=[]
        )), patch.object(self.view, 'render_template', return_value='denied'):
            self.assertEqual(self.view._guard(), 'denied')


if __name__ == '__main__':
    unittest.main()
