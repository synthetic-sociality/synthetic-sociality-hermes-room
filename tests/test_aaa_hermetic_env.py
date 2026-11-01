"""Keep this suite off any proxy of the machine it runs on.

Sorted first, so that test discovery imports it before any test imports protocol, whose opener reads the proxies once
at import. On macOS urllib falls back to the system proxy (scutil --proxy) when no proxy variable is set, so a tunnel on
the host sent the fixtures' connects to 127.0.0.1 instead of room.example (Mac128, gate reports of 2026-10-31).
"""
import os
import unittest
import urllib.request

for name in ("http_proxy", "https_proxy", "all_proxy", "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
    os.environ.pop(name, None)
os.environ["no_proxy"] = os.environ["NO_PROXY"] = "*"


class TheSuiteIsOffTheProxy(unittest.TestCase):
    def test_no_proxy_of_the_machine_reaches_the_tests(self):
        self.assertEqual({k: v for k, v in urllib.request.getproxies().items() if k != "no"}, {})
