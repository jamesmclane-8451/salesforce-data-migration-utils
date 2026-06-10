# oauth_login.py
import os
import webbrowser
import threading
import json
import requests
import socket
from typing import Dict, Any, Optional, Tuple, Union, List
from flask import Flask, request
from simple_salesforce import Salesforce
from simple_salesforce.exceptions import SalesforceExpiredSession
from dotenv import load_dotenv

load_dotenv()

# ---- Shared defaults (overridable per-env via SF_ENV_<LABEL>_*) ----
DEFAULT_CLIENT_ID = os.getenv("SF_CONSUMER_KEY")
DEFAULT_CLIENT_SECRET = os.getenv("SF_CONSUMER_SECRET")
DEFAULT_REDIRECT_HOST = os.getenv("SF_REDIRECT_HOST", "http://localhost")

# Legacy boolean-mode fallbacks
LEGACY_PROD_HOST = os.getenv("SF_ENV_PROD_HOST", "login.salesforce.com")
LEGACY_SANDBOX_HOST = os.getenv("SF_ENV_SANDBOX_HOST", "test.salesforce.com")

# TLS / CA handling
VERIFY_TLS = os.getenv("SF_VERIFY_TLS", "true").lower() == "true"
CA_BUNDLE = os.getenv("SF_CA_BUNDLE")  # optional path to corporate PEM


def _verify_arg():
    # If CA bundle provided, use it; else boolean verify flag
    return CA_BUNDLE if CA_BUNDLE else VERIFY_TLS


# Local callback ports (first free wins) , add each to your Connected App callback allowlist
REDIRECT_PORTS: List[int] = [
    int(p.strip()) for p in os.getenv("SF_REDIRECT_PORTS", "8080,18080,28080").split(",") if p.strip()
]

# Global, in-memory token capture during interactive login
token_data: Dict[str, Any] = {}
app = Flask(__name__)


# ----------------------- helpers -----------------------
def _env_keyize(name: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in name.strip()).upper()


def _normalize_oauth_host(host: str) -> str:
    """
    Keep the host EXACTLY as provided (strip scheme and trailing slash only).
    Supports *.lightning.force.com or *.my.salesforce.com without rewriting.
    """
    host = (host or "").strip().replace("https://", "").replace("http://", "")
    return host[:-1] if host.endswith("/") else host


def _build_oauth_endpoints(host: str) -> Dict[str, str]:
    base = f"https://{host}"
    return {
        "auth_url": f"{base}/services/oauth2/authorize",
        "token_url": f"{base}/services/oauth2/token",
        "revoke_url": f"{base}/services/oauth2/revoke",
    }


def _find_free_port() -> int:
    for port in REDIRECT_PORTS:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", port))
            s.close()
            return port
        except OSError:
            s.close()
            continue
    raise RuntimeError(
        f"No free redirect ports available from {REDIRECT_PORTS}. "
        f"Free one or set SF_REDIRECT_PORTS to a different list."
    )


def _load_env_config(env_label: Optional[str], use_sandbox_bool: Optional[bool]) -> Tuple[str, str, str, str, str, str]:
    """
    Returns: (display_name, host, client_id, client_secret, redirect_base, token_file)
    """
    # Legacy boolean mode
    if isinstance(use_sandbox_bool, bool) and env_label is None:
        display = "SANDBOX" if use_sandbox_bool else "PROD"
        host = LEGACY_SANDBOX_HOST if use_sandbox_bool else LEGACY_PROD_HOST
        host = _normalize_oauth_host(host)
        token_file = f"token_store_{'sandbox' if use_sandbox_bool else 'prod'}.json"
        return (display, host, DEFAULT_CLIENT_ID, DEFAULT_CLIENT_SECRET, DEFAULT_REDIRECT_HOST, token_file)

    # Named env path
    if not env_label:
        raise ValueError("Provide an env label (e.g. 'MCUAT8451') or pass boolean use_sandbox.")
    key = _env_keyize(env_label)

    host = os.getenv(f"SF_ENV_{key}_HOST")
    if not host:
        if key in {"PROD", "PRODUCTION"}:
            host = LEGACY_PROD_HOST
        elif key in {"SBX", "SANDBOX"}:
            host = LEGACY_SANDBOX_HOST
    if not host:
        raise RuntimeError(
            f"Missing host for env '{env_label}'. Set SF_ENV_{key}_HOST in .env "
            f"(e.g. 'your-domain.my.salesforce.com' or 'your-domain.lightning.force.com') "
            f"or use boolean True/False."
        )
    host = _normalize_oauth_host(host)

    client_id = os.getenv(f"SF_ENV_{key}_CLIENT_ID", DEFAULT_CLIENT_ID)
    client_secret = os.getenv(f"SF_ENV_{key}_CLIENT_SECRET", DEFAULT_CLIENT_SECRET)
    redirect_base = os.getenv(f"SF_ENV_{key}_REDIRECT_HOST", DEFAULT_REDIRECT_HOST)

    if not client_id or not client_secret:
        raise RuntimeError(
            f"Client ID/Secret not configured for env '{env_label}'. "
            f"Set SF_ENV_{key}_CLIENT_ID / SF_ENV_{key}_CLIENT_SECRET, or defaults "
            f"SF_CONSUMER_KEY / SF_CONSUMER_SECRET."
        )

    token_file = f"token_store_{key.lower()}.json"
    return (env_label.upper(), host, client_id, client_secret, redirect_base, token_file)


@app.route("/callback")
def callback():
    code = request.args.get("code")
    if not code:
        return "❌ Authorization code not found in callback."

    payload = {
        "grant_type": "authorization_code",
        "code": code,
        "client_id": app.config["CLIENT_ID"],
        "client_secret": app.config["CLIENT_SECRET"],
        "redirect_uri": app.config["REDIRECT_URI"],
    }
    try:
        response = requests.post(app.config["TOKEN_URL"], data=payload, timeout=60, verify=_verify_arg())
        response.raise_for_status()
    except requests.exceptions.RequestException as e:
        return f"Token request failed: {e}"

    token_json = response.json()
    token_data.update(token_json)

    # Persist tokens immediately
    with open(app.config["TOKEN_FILE"], "w") as f:
        json.dump(token_json, f)

    return "✅ Authentication successful. You may now close this tab."


def _start_flask_app(port: int):
    app.run(port=port)


def refresh_access_token(refresh_token: str, token_url: str, client_id: str, client_secret: str) -> Dict[str, Any]:
    payload = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "client_secret": client_secret,
        "refresh_token": refresh_token,
    }
    r = requests.post(token_url, data=payload, timeout=60, verify=_verify_arg())
    r.raise_for_status()
    return r.json()


def revoke_token(token: str, revoke_url: str):
    try:
        requests.post(revoke_url, data={"token": token}, timeout=30, verify=_verify_arg())
    except Exception:
        pass  # best-effort


def _probe_chatter_scope(session, base_url) -> bool:
    """Best-effort probe; never forces relogin in this build."""
    try:
        resp = session.get(f"{base_url}chatter/users/me", timeout=15)
        return resp.status_code == 200 and resp.headers.get("Content-Type", "").startswith("application/json")
    except Exception:
        return False


def _session_alive(sf) -> bool:
    """
    Cheap ping; returns False on INVALID_SESSION_ID or any auth failure.
    Using /limits because it's lightweight and requires a valid session.
    """
    try:
        sf.restful("limits")
        return True
    except SalesforceExpiredSession:
        return False
    except Exception:
        # any auth/permission error should be treated as not alive here
        return False


# ----------------------- main entry -----------------------
def get_salesforce_connection(
    use_sandbox: Union[bool, str] = False,
    *,
    env: Optional[str] = None,
    force_refresh: bool = False,
    force_new_login: bool = False,
    requires_chatter: bool = False,  # default False to avoid forcing relogin
    include_openid: bool = False
) -> Salesforce:
    """
    Returns an authenticated simple_salesforce.Salesforce client.

    Token reuse rules in this build:
      • If a cached token file exists, we will try to reuse it immediately.
      • We PROBE the session; if expired, we attempt a refresh grant.
      • Only if reuse+refresh fail (or force_new_login=True) will we open the browser.

    Environment selection
    ---------------------
    There are two supported ways to choose the target Salesforce environment:

    1) Preferred: env label via `env` (string)
       - Example: env="MCUAT8451"
       - Requires SF_ENV_<LABEL>_HOST in .env (plus optional per-env client id/secret overrides)

    2) Legacy: boolean/sandbox selector via `use_sandbox` (bool)
       - True  -> sandbox host (SF_ENV_SANDBOX_HOST, default test.salesforce.com)
       - False -> prod host    (SF_ENV_PROD_HOST, default login.salesforce.com)

    Backwards compatibility
    -----------------------
    - If `env` is provided, it takes precedence and `use_sandbox` is ignored for env selection.
    - If `env` is not provided, behavior remains unchanged from the prior version.

    Parameters
    ----------
    use_sandbox : Union[bool, str], optional
        Legacy selector:
          - bool: True for sandbox, False for prod
          - str : env label like "MCUAT8451" (legacy-but-supported)
        Default is False.

    env : Optional[str], optional
        Explicit Salesforce environment label.
        This is the recommended way to choose an org because it is unambiguous.
        Example: "MCUAT8451", "MCUAT", "SF2UAT"
        Default is None.

    force_refresh : bool, optional
        If True, forces a refresh token grant attempt when a cache exists (still falls back to interactive login if needed).
        Default is False.

    force_new_login : bool, optional
        If True, revokes cached tokens (best-effort), deletes the cache file, and forces interactive login.
        Default is False.

    requires_chatter : bool, optional
        If True, includes chatter_api scope and probes chatter endpoint after login.
        Default is False.

    include_openid : bool, optional
        If True, includes openid scope in the OAuth request.
        Default is False.

    Returns
    -------
    Salesforce
        Authenticated simple_salesforce.Salesforce client.
    """
    # Resolve env / host / secrets
    env_label: Optional[str] = None
    use_sandbox_bool: Optional[bool] = None

    # env takes precedence if provided
    if env:
        env_label = env
    else:
        # Preserve legacy behavior: accept env label via use_sandbox=str
        if isinstance(use_sandbox, str):
            env_label = use_sandbox
        else:
            use_sandbox_bool = bool(use_sandbox)

    display_name, host, client_id, client_secret, redirect_base, token_file = _load_env_config(
        env_label, use_sandbox_bool
    )
    endpoints = _build_oauth_endpoints(host)
    auth_url, token_url, revoke_url = endpoints["auth_url"], endpoints["token_url"], endpoints["revoke_url"]

    print(f"\n🚀 Connecting to Salesforce [{display_name}] using host [{host}]\n")

    # Make config available to callback
    app.config["TOKEN_URL"] = token_url
    app.config["TOKEN_FILE"] = token_file
    app.config["CLIENT_ID"] = client_id
    app.config["CLIENT_SECRET"] = client_secret

    # ----- 0) Forced fresh login path -----
    if force_new_login and os.path.exists(token_file):
        try:
            with open(token_file, "r") as f:
                saved = json.load(f)
            if "access_token" in saved:
                revoke_token(saved["access_token"], revoke_url)
            if "refresh_token" in saved:
                revoke_token(saved["refresh_token"], revoke_url)
        finally:
            try:
                os.remove(token_file)
            except Exception:
                pass

    # ----- 1) Cached token: reuse, validate, refresh if needed -----
    if os.path.exists(token_file) and not force_new_login:
        with open(token_file, "r") as f:
            saved_tokens = json.load(f)

        # (a) Try immediate reuse and probe
        try:
            sf = Salesforce(
                instance_url=saved_tokens["instance_url"],
                session_id=saved_tokens["access_token"],
                client_id=client_id,
            )
            if _session_alive(sf):
                print("✅ Reusing saved Salesforce token.")
                if requires_chatter:
                    _probe_chatter_scope(sf.session, sf.base_url)
                return sf
            else:
                print("ℹ️ Cached access token is expired; attempting refresh…")
        except Exception as e:
            print(f"ℹ️ Cached token reuse failed early: {e}. Attempting refresh…")

        # (b) Attempt refresh
        if saved_tokens.get("refresh_token") or force_refresh:
            try:
                refresh_token_val = saved_tokens.get("refresh_token")
                if not refresh_token_val:
                    raise RuntimeError("No refresh_token available in cache.")
                new_tokens = refresh_access_token(refresh_token_val, token_url, client_id, client_secret)
                # Some orgs don't return refresh_token on refresh; preserve existing
                if "refresh_token" not in new_tokens:
                    new_tokens["refresh_token"] = refresh_token_val
                merged = {**saved_tokens, **new_tokens}
                with open(token_file, "w") as f:
                    json.dump(merged, f)

                sf = Salesforce(
                    instance_url=merged.get("instance_url", saved_tokens["instance_url"]),
                    session_id=merged.get("access_token", saved_tokens["access_token"]),
                    client_id=client_id,
                )
                if _session_alive(sf):
                    print("🔄 Refreshed and reused Salesforce token.")
                    if requires_chatter:
                        _probe_chatter_scope(sf.session, sf.base_url)
                    return sf
                else:
                    print("⚠️ Refresh returned but session still invalid; proceeding to interactive login.")
            except Exception as e:
                print(f"❌ Refresh grant failed: {e}. Proceeding to interactive login.")
        else:
            print("ℹ️ No refresh_token found in cache; proceeding to interactive login.")

    # ----- 2) Interactive OAuth login (only if all else failed) -----
    port = _find_free_port()
    redirect_uri = f"{DEFAULT_REDIRECT_HOST if redirect_base is None else redirect_base}:{port}/callback"
    app.config["REDIRECT_URI"] = redirect_uri
    threading.Thread(target=_start_flask_app, args=(port,), daemon=True).start()

    scopes = ["refresh_token", "api"]
    if requires_chatter:
        scopes.append("chatter_api")
    if include_openid:
        scopes.append("openid")

    auth_params = {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "prompt": "login consent",  # forces consent to ensure refresh_token on first grant
        "scope": " ".join(scopes),
    }

    full_auth_url = requests.Request("GET", auth_url, params=auth_params).prepare().url
    print(f"🔑 Opening browser for Salesforce login with scopes: {auth_params['scope']}\n{full_auth_url}")
    webbrowser.open(full_auth_url)

    print("⏳ Waiting for authentication to complete...")
    while not token_data.get("access_token"):
        pass

    # Persist the newly issued tokens
    with open(token_file, "w") as f:
        json.dump(token_data, f)

    sf = Salesforce(
        instance_url=token_data["instance_url"],
        session_id=token_data["access_token"],
        client_id=client_id,
    )
    if requires_chatter:
        _probe_chatter_scope(sf.session, sf.base_url)

    print("✅ New OAuth session established.")
    return sf
