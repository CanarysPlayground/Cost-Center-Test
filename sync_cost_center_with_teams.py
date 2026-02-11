import os
import requests
import logging
from typing import Set

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def parse_next_link(link_header: str) -> str | None:
    """Parse pagination link from GitHub API response headers."""
    if not link_header:
        return None
    parts = [p.strip() for p in link_header.split(",")]
    for part in parts:
        if 'rel="next"' in part:
            start = part.find("<") + 1
            end = part.find(">")
            if start > 0 and end > start:
                return part[start:end]
    return None


def extract_memberships(payload):
    """Extract memberships from API response payload."""
    if isinstance(payload, list):
        return payload

    if isinstance(payload, dict):
        for key in ("memberships", "items", "value", "data"):
            if key in payload and isinstance(payload[key], list):
                return payload[key]

        for v in payload.values():
            if isinstance(v, list) and (len(v) == 0 or isinstance(v[0], dict)):
                return v

    return []


def github_headers(token: str) -> dict:
    """Create standard GitHub API headers."""
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def fetch_enterprise_team_members(
    base: str, enterprise: str, team_slug: str, token: str
) -> Set[str]:
    """
    Fetch all members from the specified enterprise team.
    Returns a set of user logins.
    """
    url = f"{base}/enterprises/{enterprise}/teams/{team_slug}/memberships"
    headers = github_headers(token)
    
    logins: Set[str] = set()
    next_url = url
    page = 1

    logging.info(f"Fetching enterprise team members from {team_slug}...")

    while next_url:
        resp = requests.get(next_url, headers=headers, timeout=30)
        
        if resp.status_code != 200:
            logging.error(
                f"Failed to fetch team memberships (page {page}): "
                f"HTTP {resp.status_code} - {resp.text[:500]}"
            )
            raise SystemExit(f"Failed to fetch team memberships: HTTP {resp.status_code}")

        try:
            payload = resp.json()
        except Exception as e:
            logging.error(f"Failed to parse JSON response: {e}")
            raise SystemExit(f"Non-JSON response from API")

        memberships = extract_memberships(payload)
        logging.info(f"Fetched page {page}: {len(memberships)} memberships")

        for m in memberships:
            if not isinstance(m, dict):
                continue
            
            user = m.get("user") or {}
            login = None
            
            if isinstance(user, dict):
                login = user.get("login")
            login = login or m.get("login")
            
            if login:
                logins.add(login)

        next_url = parse_next_link(resp.headers.get("Link"))
        page += 1
        
        if page > 200:
            logging.warning("Stopping after 200 pages (pagination safety limit)")
            break

    logging.info(f"Total enterprise team members found: {len(logins)}")
    return logins


def fetch_cost_center_members(
    base: str, enterprise: str, cost_center_id: str, token: str
) -> Set[str]:
    """
    Fetch all members currently in the cost center.
    Returns a set of user logins.
    """
    url = f"{base}/enterprises/{enterprise}/settings/billing/cost-centers/{cost_center_id}"
    headers = github_headers(token)
    
    logging.info(f"Fetching cost center members from cost center {cost_center_id}...")

    resp = requests.get(url, headers=headers, timeout=30)
    
    if resp.status_code != 200:
        logging.error(
            f"Failed to fetch cost center info: HTTP {resp.status_code} - {resp.text[:500]}"
        )
        raise SystemExit(f"Failed to fetch cost center: HTTP {resp.status_code}")

    try:
        data = resp.json()
    except Exception as e:
        logging.error(f"Failed to parse cost center response: {e}")
        raise SystemExit(f"Non-JSON response from cost center API")

    # Extract users from cost center response
    logins: Set[str] = set()
    
    # Try different possible structures
    if "resources" in data:
        resources = data.get("resources", {})
        users = resources.get("users", [])
        for user in users:
            if isinstance(user, dict):
                login = user.get("login") or user.get("username")
            else:
                login = user
            if login:
                logins.add(login)
    elif "users" in data:
        users = data.get("users", [])
        for user in users:
            if isinstance(user, dict):
                login = user.get("login") or user.get("username")
            else:
                login = user
            if login:
                logins.add(login)

    logging.info(f"Total cost center members found: {len(logins)}")
    return logins


def add_user_to_cost_center(
    base: str, enterprise: str, cost_center_id: str, username: str, token: str
) -> bool:
    """Add a single user to the cost center."""
    url = f"{base}/enterprises/{enterprise}/settings/billing/cost-centers/{cost_center_id}/resource"
    headers = github_headers(token)
    
    payload = {"users": [username]}
    
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    
    if resp.status_code in (200, 201, 204):
        logging.info(f"✓ Added {username} to cost center")
        return True
    else:
        logging.error(
            f"✗ Failed to add {username} to cost center: "
            f"HTTP {resp.status_code} - {resp.text[:300]}"
        )
        return False


def remove_user_from_cost_center(
    base: str, enterprise: str, cost_center_id: str, username: str, token: str
) -> bool:
    """Remove a single user from the cost center."""
    url = f"{base}/enterprises/{enterprise}/settings/billing/cost-centers/{cost_center_id}/resource"
    headers = github_headers(token)
    
    payload = {"users": [username]}
    
    resp = requests.delete(url, headers=headers, json=payload, timeout=30)
    
    if resp.status_code in (200, 204):
        logging.info(f"✓ Removed {username} from cost center")
        return True
    else:
        logging.error(
            f"✗ Failed to remove {username} from cost center: "
            f"HTTP {resp.status_code} - {resp.text[:300]}"
        )
        return False


def sync_cost_center_with_team(
    base: str, enterprise: str, team_slug: str, cost_center_id: str, token: str
) -> None:
    """
    Synchronize cost center with enterprise team:
    - Remove users from cost center who are NOT in the team
    - Add users to cost center who are in the team but not in cost center
    """
    logging.info("=" * 60)
    logging.info("Starting Cost Center Sync")
    logging.info("=" * 60)
    
    # Fetch current state
    team_members = fetch_enterprise_team_members(base, enterprise, team_slug, token)
    cost_center_members = fetch_cost_center_members(base, enterprise, cost_center_id, token)
    
    # Calculate differences
    to_remove = cost_center_members - team_members  # In cost center but NOT in team
    to_add = team_members - cost_center_members     # In team but NOT in cost center
    
    logging.info("")
    logging.info("=" * 60)
    logging.info("Sync Analysis")
    logging.info("=" * 60)
    logging.info(f"Enterprise team members: {len(team_members)}")
    logging.info(f"Cost center members: {len(cost_center_members)}")
    logging.info(f"Users to REMOVE from cost center: {len(to_remove)}")
    logging.info(f"Users to ADD to cost center: {len(to_add)}")
    
    if to_remove:
        logging.info(f"Users to remove: {sorted(to_remove)}")
    if to_add:
        logging.info(f"Users to add: {sorted(to_add)}")
    
    # Remove users not in team
    if to_remove:
        logging.info("")
        logging.info("=" * 60)
        logging.info(f"Removing {len(to_remove)} users from cost center")
        logging.info("=" * 60)
        
        removed_count = 0
        for username in sorted(to_remove):
            if remove_user_from_cost_center(base, enterprise, cost_center_id, username, token):
                removed_count += 1
        
        logging.info(f"Successfully removed {removed_count}/{len(to_remove)} users")
    
    # Add users from team
    if to_add:
        logging.info("")
        logging.info("=" * 60)
        logging.info(f"Adding {len(to_add)} users to cost center")
        logging.info("=" * 60)
        
        added_count = 0
        for username in sorted(to_add):
            if add_user_to_cost_center(base, enterprise, cost_center_id, username, token):
                added_count += 1
        
        logging.info(f"Successfully added {added_count}/{len(to_add)} users")
    
    if not to_remove and not to_add:
        logging.info("")
        logging.info("✓ Cost center is already in sync with enterprise team!")
    
    logging.info("")
    logging.info("=" * 60)
    logging.info("Sync Complete")
    logging.info("=" * 60)


def main():
    """Main entry point for the sync script."""
    # Load configuration from environment variables (GitHub Secrets)
    base = os.getenv("GITHUB_API_BASE", "https://api.github.com").rstrip("/")
    enterprise = os.getenv("GITHUB_ENTERPRISE")
    team_slug = os.getenv("GITHUB_TEAM_SLUG")
    cost_center_id = os.getenv("GITHUB_COST_CENTER_ID")
    token = os.getenv("GITHUB_TOKEN")

    # Validate required environment variables
    missing = []
    if not enterprise:
        missing.append("GITHUB_ENTERPRISE")
    if not team_slug:
        missing.append("GITHUB_TEAM_SLUG")
    if not cost_center_id:
        missing.append("GITHUB_COST_CENTER_ID")
    if not token:
        missing.append("GITHUB_TOKEN")
    
    if missing:
        logging.error(f"Missing required environment variables: {', '.join(missing)}")
        logging.error("Please set these as GitHub Secrets or environment variables")
        raise SystemExit(1)

    # Run the sync
    try:
        sync_cost_center_with_team(base, enterprise, team_slug, cost_center_id, token)
    except Exception as e:
        logging.error(f"Sync failed with error: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
