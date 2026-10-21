"""The stack picker page of the router's `/authorize`."""

from collections.abc import Iterable
from html import escape
from urllib.parse import urlencode

from keboola_mcp_server.router.stacks import Stack

# Nothing inline can run or style the page (no script at all, the stylesheet is a file of the router's own), nothing on
# it submits anywhere, and no other site can frame it. No referrer: the page URL carries `state` and the PKCE challenge.
PICKER_HEADERS = {
    'Content-Security-Policy': (
        "default-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
    ),
    'Referrer-Policy': 'no-referrer',
    'X-Content-Type-Options': 'nosniff',
    'Cache-Control': 'no-store',
}
PICKER_STYLESHEET_PATH = '/router-static/picker.css'

PICKER_CSS = """\
body { font-family: system-ui, sans-serif; margin: 0; padding: 2rem 1rem; background: #f6f7f9; color: #1f2933; }
main { max-width: 32rem; margin: 0 auto; background: #fff; border-radius: 8px; padding: 1.5rem 2rem; }
h1 { font-size: 1.25rem; margin-top: 0; }
a.stack { display: block; padding: 0.75rem; margin: 0.5rem 0; border: 1px solid #d5dae1; border-radius: 6px;
  color: inherit; text-decoration: none; }
a.stack:hover, a.stack.last-used { border-color: #1f8fff; }
.host { display: block; color: #616e7c; font-size: 0.875rem; }
.hint { color: #1f8fff; font-size: 0.875rem; }
"""


def stack_link(params: Iterable[tuple[str, str]], stack_key: str) -> str:
    """`/authorize` with every original parameter except `stack`, re-encoded, and the chosen stack last. Re-encoding
    the parsed values means a value can never end early and smuggle in a parameter (an encoded `&stack=` stays part of
    its value)."""
    return '/authorize?' + urlencode([*((k, v) for k, v in params if k != 'stack'), ('stack', stack_key)])


def render_picker(*, stacks: Iterable[Stack], params: Iterable[tuple[str, str]], preselected: str | None) -> str:
    """The picker page: one plain link per stack. Not a form: browsers apply `form-action` to every redirect after a
    form submission, and the chain after this page (router, then the stack's `/authorize`, then Connection) leaves the
    router's origin. A link navigation is not subject to it. The links are HTML-escaped; nothing else from the request
    is rendered. The stack used last (the cookie) is highlighted."""
    params = list(params)
    hint = ' <span class="hint">(used last time)</span>'
    links = '\n'.join(
        f'<a class="stack{" last-used" if s.key == preselected else ""}" href="{escape(stack_link(params, s.key))}">'
        f'{escape(s.name)}{hint if s.key == preselected else ""}'
        f'<span class="host">{escape(s.login_host)}</span></a>'
        for s in stacks
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>Choose your Keboola stack</title><link rel="stylesheet" href="{PICKER_STYLESHEET_PATH}">'
        '</head><body><main><h1>Where is your Keboola account?</h1>'
        '<p>Pick the stack you log in to. Its login address is shown under each name.</p>'
        f'<nav>{links}</nav></main></body></html>'
    )
