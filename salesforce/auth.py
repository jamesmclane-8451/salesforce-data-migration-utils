# Thin adapter so gtm_ai_os_core.salesforce.ui (org_identity.resolve_expected_org_id,
# diagnostics.py) can resolve the trusted OAuth-side Salesforce connection via
# "import salesforce.auth" - the same top-level shape gtm-ai-os itself uses
# (lib/salesforce/auth.py). EventBridge's real OAuth implementation lives in
# the project's own root-level auth.py; this module re-exports it rather than
# duplicating any connection/token logic.
from auth import get_salesforce_connection

__all__ = ["get_salesforce_connection"]
