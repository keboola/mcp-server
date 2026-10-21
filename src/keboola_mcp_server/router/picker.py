"""The stack picker page of the router's `/authorize`."""

from collections.abc import Iterable
from html import escape

from keboola_mcp_server.router.stacks import Stack


def picker_headers(stacks: Iterable[Stack]) -> dict[str, str]:
    """Nothing inline can run or style the page (no script at all, the stylesheet is a file of the router's own), and
    no other site can frame it. The form may submit to the router only; `form-action` also lists the stacks' upstreams
    because browsers apply it to the redirect that follows the submit, which goes to the chosen stack. No referrer:
    the page URL carries `state` and the PKCE challenge."""
    form_action = ' '.join(["'self'", *(s.upstream for s in stacks)])
    return {
        'Content-Security-Policy': (
            f"default-src 'self'; form-action {form_action}; frame-ancestors 'none'; base-uri 'none'; object-src 'none'"
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
label { display: block; padding: 0.75rem; margin: 0.5rem 0; border: 1px solid #d5dae1; border-radius: 6px; }
label:hover { border-color: #1f8fff; }
.host { display: block; color: #616e7c; font-size: 0.875rem; margin-left: 1.5rem; }
button { margin-top: 1rem; padding: 0.6rem 1.4rem; border: 0; border-radius: 6px; background: #1f8fff; color: #fff; }
"""


def render_picker(*, stacks: Iterable[Stack], params: Iterable[tuple[str, str]], preselected: str | None) -> str:
    """The picker page: a `GET` form back to `/authorize` whose only visible input is the stack key. Every original
    parameter rides along as a hidden input, HTML-escaped, because a `GET` form drops the query string of its action;
    nothing else from the request is rendered."""
    options = '\n'.join(
        f'<label><input type="radio" name="stack" value="{escape(s.key)}" required'
        f'{" checked" if s.key == preselected else ""}> {escape(s.name)}'
        f'<span class="host">{escape(s.login_host)}</span></label>'
        for s in stacks
    )
    hidden = ''.join(
        f'<input type="hidden" name="{escape(name, quote=True)}" value="{escape(value, quote=True)}">'
        for name, value in params
        if name != 'stack'
    )
    return (
        '<!doctype html><html lang="en"><head><meta charset="utf-8">'
        '<meta name="viewport" content="width=device-width, initial-scale=1">'
        f'<title>Choose your Keboola stack</title><link rel="stylesheet" href="{PICKER_STYLESHEET_PATH}">'
        '</head><body><main><h1>Where is your Keboola account?</h1>'
        '<p>Pick the stack you log in to. Its login address is shown under each name.</p>'
        f'<form method="get" action="/authorize">{hidden}{options}<button type="submit">Continue to login</button>'
        '</form></main></body></html>'
    )
