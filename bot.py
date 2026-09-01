import os
import json
import asyncio
from datetime import datetime, timezone, timedelta

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands
from dotenv import load_dotenv


# =========================================================
# THE WITNESS — DIVINE SISTER COURT
# Efficient Edition
#
# Main optimization:
# Instead of downloading 71 Nannies' friend lists for a team scan,
# The Witness downloads each LEADER'S friend list once and builds a
# reverse connection index. Team scans then become almost instant.
#
# Commands:
# /connections        -> one user
# /connectionsmany    -> several users
# /teamconnections    -> whole rank/team
# /checkfriendslist   -> check who has an unavailable/private friends list
# /witnessrefresh     -> refresh staff + leadership + friend graph
# /witnessstatus      -> cache/API health
#
# Automatic:
# - refreshes staff roles every 5 minutes
# - detects new staff/promotions (2-scan confirmation)
# - automatically checks promoted users against cached leadership graph
# - refreshes leadership friend graph every 30 minutes
#
# Public Roblox friendship = connection only, NOT proof of favoritism.
# =========================================================


load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")

GROUP_ID = 5008654
STAFF_MIN_RANK = 50

CONNECTIONS_CHANNEL = "connections"
PRESENCE_CHANNEL = "leadership-presence"
CURRENT_SERVER_CHANNEL = "current-server"

# De Pride Isle Sanatorium
DPI_PLACE_ID = 3522803956
DPI_UNIVERSE_ID = 1246853548
PRESENCE_SCAN_MINUTES = 5
CURRENT_SERVER_REFRESH_MINUTES = 5
PRESENCE_STATE_FILE = "witness_presence_state.json"
CURRENT_SERVER_STATE_FILE = "witness_current_server_state.json"
PRESENCE_CACHE_SECONDS = 240
PRESENCE_BATCH_SIZE = 25

STAFF_SCAN_MINUTES = 5
LEADER_GRAPH_MINUTES = 30
PRIVACY_CACHE_MINUTES = 30
CONFIRM_SCANS = 2

STATE_FILE = "witness_state.json"
STAFF_CACHE_FILE = "witness_staff_cache.json"
PENDING_FILE = "witness_pending.json"
GRAPH_FILE = "witness_leader_graph.json"
PRIVACY_FILE = "witness_privacy_cache.json"

GROUPS_API = "https://groups.roblox.com"
FRIENDS_API = "https://friends.roblox.com"
USERS_API = "https://users.roblox.com"
PRESENCE_API = "https://presence.roblox.com"

TEAM_CONCURRENCY = 4
MAX_MULTI_USERS = 25
MAX_TEAM_USERS = 250

intents = discord.Intents.default()
bot = commands.Bot(command_prefix="!", intents=intents)

scan_lock = asyncio.Lock()
graph_lock = asyncio.Lock()
presence_lock = asyncio.Lock()
presence_cache = {}
presence_cache_at = None


# =========================================================
# FILE / TIME HELPERS
# =========================================================

def load_json(path, default):
    if not os.path.exists(path):
        return default

    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path, data):
    temp = path + ".tmp"

    with open(temp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    os.replace(temp, path)


def utc_now():
    return datetime.now(timezone.utc)


def utc_iso():
    return utc_now().isoformat()


def parse_iso(value):
    if not value:
        return None

    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def is_fresh(value, minutes):
    dt = parse_iso(value)

    if dt is None:
        return False

    return utc_now() - dt < timedelta(minutes=minutes)


def discord_relative_time(value):
    dt = parse_iso(value)

    if dt is None:
        return "Never"

    return f"<t:{int(dt.timestamp())}:R>"


def find_channel(guild, name):
    return discord.utils.get(guild.text_channels, name=name)


def profile_url(user_id):
    return f"https://www.roblox.com/users/{user_id}/profile"


# =========================================================
# ROBLOX HTTP
# =========================================================

async def request_json(
    session,
    method,
    url,
    *,
    payload=None,
    attempts=4,
):
    last_error = None

    for attempt in range(attempts):
        try:
            async with session.request(
                method,
                url,
                json=payload,
            ) as response:

                if response.status == 200:
                    return await response.json()

                if response.status == 429:
                    retry_after = response.headers.get(
                        "Retry-After",
                        "2",
                    )

                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = 2.0

                    await asyncio.sleep(
                        min(max(wait, 1), 10)
                    )
                    continue

                text = await response.text()

                last_error = RuntimeError(
                    f"Roblox API {response.status}: "
                    f"{text[:250]}"
                )

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ) as exc:
            last_error = exc

        await asyncio.sleep(1.5 * (attempt + 1))

    raise last_error or RuntimeError(
        "Roblox request failed."
    )


async def get_group_info(session):
    return await request_json(
        session,
        "GET",
        f"{GROUPS_API}/v1/groups/{GROUP_ID}",
    )


async def get_group_roles(session):
    data = await request_json(
        session,
        "GET",
        f"{GROUPS_API}/v1/groups/{GROUP_ID}/roles",
    )

    return data.get("roles", [])


async def get_users_in_role(session, role_id):
    users = {}
    cursor = None

    while True:
        url = (
            f"{GROUPS_API}/v1/groups/{GROUP_ID}"
            f"/roles/{role_id}/users"
            f"?limit=100&sortOrder=Asc"
        )

        if cursor:
            url += f"&cursor={cursor}"

        data = await request_json(
            session,
            "GET",
            url,
        )

        for entry in data.get("data", []):
            user = entry.get("user", entry)

            user_id = (
                user.get("userId")
                or user.get("id")
            )

            username = (
                user.get("username")
                or user.get("name")
                or "Unknown"
            )

            display_name = (
                user.get("displayName")
                or ""
            )

            if user_id is None:
                continue

            users[str(user_id)] = {
                "username": username,
                "display_name": display_name,
            }

        cursor = data.get("nextPageCursor")

        if not cursor:
            break

    return users


async def resolve_username(session, username):
    data = await request_json(
        session,
        "POST",
        f"{USERS_API}/v1/usernames/users",
        payload={
            "usernames": [username],
            "excludeBannedUsers": False,
        },
    )

    matches = data.get("data", [])

    if not matches:
        return None

    user = matches[0]

    return {
        "user_id": str(user.get("id")),
        "username": user.get("name", username),
        "display_name": user.get("displayName", ""),
    }


async def get_friends(session, user_id):
    data = await request_json(
        session,
        "GET",
        f"{FRIENDS_API}/v1/users/{user_id}/friends",
    )

    friends = {}

    for friend in data.get("data", []):
        friend_id = (
            friend.get("id")
            or friend.get("userId")
        )

        if friend_id is None:
            continue

        friends[str(friend_id)] = {
            "username": (
                friend.get("name")
                or friend.get("username")
                or "Unknown"
            ),
            "display_name": (
                friend.get("displayName")
                or ""
            ),
        }

    return friends


async def probe_friends_visibility(session, user_id):
    """
    Returns:
    public      -> endpoint readable
    unavailable -> privacy/restriction/unsupported response
    error       -> temporary network/API problem
    """

    url = f"{FRIENDS_API}/v1/users/{user_id}/friends"

    for attempt in range(3):
        try:
            async with session.get(url) as response:
                if response.status == 200:
                    data = await response.json()

                    return {
                        "status": "public",
                        "friend_count": len(data.get("data", [])),
                        "http_status": 200,
                    }

                if response.status == 429:
                    retry_after = response.headers.get(
                        "Retry-After",
                        "2",
                    )

                    try:
                        wait = float(retry_after)
                    except ValueError:
                        wait = 2.0

                    await asyncio.sleep(
                        min(max(wait, 1), 10)
                    )
                    continue

                if response.status in (
                    401,
                    403,
                    404,
                ):
                    return {
                        "status": "unavailable",
                        "friend_count": None,
                        "http_status": response.status,
                    }

                return {
                    "status": "error",
                    "friend_count": None,
                    "http_status": response.status,
                }

        except (
            aiohttp.ClientError,
            asyncio.TimeoutError,
        ):
            await asyncio.sleep(
                1.2 * (attempt + 1)
            )

    return {
        "status": "error",
        "friend_count": None,
        "http_status": None,
    }



# =========================================================
# ROBLOX PRESENCE
# =========================================================

async def get_user_presences(session, user_ids):
    """
    Rate-safe Roblox public presence fetch.

    Uses small batches, automatic split fallback, and exponential backoff
    for HTTP 429 so one Roblox throttle does not kill the whole watcher.
    """
    if not user_ids:
        return {}

    ids = [int(user_id) for user_id in user_ids]
    results = {}

    async def fetch_batch(batch):
        if not batch:
            return

        for attempt in range(7):
            try:
                async with session.post(
                    f"{PRESENCE_API}/v1/presence/users",
                    json={"userIds": batch},
                ) as response:
                    if response.status == 200:
                        data = await response.json()

                        for item in data.get("userPresences", []):
                            user_id = item.get("userId")
                            if user_id is not None:
                                results[str(user_id)] = item

                        return

                    body = await response.text()

                    if (
                        response.status == 400
                        and "too many user ids" in body.lower()
                        and len(batch) > 1
                    ):
                        middle = max(1, len(batch) // 2)

                        await fetch_batch(batch[:middle])
                        await asyncio.sleep(0.35)
                        await fetch_batch(batch[middle:])
                        return

                    if response.status == 429:
                        retry_after = response.headers.get("Retry-After")

                        try:
                            wait = (
                                float(retry_after)
                                if retry_after
                                else 0.0
                            )
                        except (TypeError, ValueError):
                            wait = 0.0

                        if wait <= 0:
                            wait = min(
                                4.0 * (2 ** attempt),
                                60.0,
                            )

                        print(
                            "Witness presence: rate limited; "
                            f"waiting {wait:.1f}s before retry."
                        )

                        await asyncio.sleep(
                            min(max(wait, 2.0), 60.0)
                        )
                        continue

                    if 500 <= response.status < 600:
                        wait = min(
                            2.5 * (2 ** attempt),
                            30.0,
                        )

                        await asyncio.sleep(wait)
                        continue

                    raise RuntimeError(
                        f"Roblox Presence API "
                        f"{response.status}: "
                        f"{body[:250]}"
                    )

            except (
                aiohttp.ClientError,
                asyncio.TimeoutError,
            ) as exc:
                if attempt >= 6:
                    raise

                wait = min(
                    2.0 * (2 ** attempt),
                    30.0,
                )

                print(
                    "Witness presence network error:",
                    repr(exc),
                    f"retrying in {wait:.1f}s",
                )

                await asyncio.sleep(wait)

        raise RuntimeError(
            "Roblox Presence API remained unavailable "
            "after retries."
        )

    for offset in range(
        0,
        len(ids),
        PRESENCE_BATCH_SIZE,
    ):
        batch = ids[
            offset:
            offset + PRESENCE_BATCH_SIZE
        ]

        await fetch_batch(batch)

        if offset + PRESENCE_BATCH_SIZE < len(ids):
            await asyncio.sleep(0.60)

    return results

def presence_is_dpi(presence):
    """
    Detect the De Pride Isle Sanatorium experience.
    universeId is preferred; rootPlaceId/placeId are fallbacks.
    """
    if not presence:
        return False

    universe_id = presence.get("universeId")
    root_place_id = presence.get("rootPlaceId")
    place_id = presence.get("placeId")

    return (
        str(universe_id) == str(DPI_UNIVERSE_ID)
        or str(root_place_id) == str(DPI_PLACE_ID)
        or str(place_id) == str(DPI_PLACE_ID)
    )


def presence_location_text(presence):
    if not presence:
        return "Unknown"

    return (
        presence.get("lastLocation")
        or "De Pride Isle Sanatorium"
    )

# =========================================================
# STAFF + LEADERSHIP SNAPSHOT
# =========================================================

async def build_snapshot():
    staff = {}
    role_snapshot = {}
    leaders = {}

    headers = {
        "User-Agent":
        "The-Witness-Divine-Sister-Court/2.0"
    }

    timeout = aiohttp.ClientTimeout(total=120)

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
    ) as session:

        group_info = await get_group_info(session)
        roles = await get_group_roles(session)

        staff_roles = sorted(
            [
                role
                for role in roles
                if int(role.get("rank", 0))
                >= STAFF_MIN_RANK
            ],
            key=lambda role:
            int(role.get("rank", 0)),
        )

        print(
            f"Witness: scanning "
            f"{len(staff_roles)} staff roles..."
        )

        for role in staff_roles:
            role_id = role.get("id")
            role_name = role.get(
                "name",
                "Unknown Role",
            )
            role_rank = int(
                role.get("rank", 0)
            )

            users = await get_users_in_role(
                session,
                role_id,
            )

            role_snapshot[str(role_id)] = {
                "name": role_name,
                "rank": role_rank,
                "count": len(users),
            }

            for user_id, info in users.items():
                staff[user_id] = {
                    "username":
                    info["username"],

                    "display_name":
                    info["display_name"],

                    "role_id":
                    role_id,

                    "role_name":
                    role_name,

                    "role_rank":
                    role_rank,
                }

                lower = role_name.lower()

                if (
                    "matrona" in lower
                    or "reverend" in lower
                ):
                    leaders[user_id] = {
                        "username":
                        info["username"],

                        "display_name":
                        info["display_name"],

                        "role_name":
                        role_name,

                        "category": (
                            "Matrona"
                            if "matrona" in lower
                            else "Reverend"
                        ),
                    }

        # Divine Sister / group owner automatically.
        owner = group_info.get("owner")

        if owner:
            owner_id = (
                owner.get("userId")
                or owner.get("id")
            )

            owner_name = (
                owner.get("username")
                or owner.get("name")
                or "Divine Sister"
            )

            if owner_id is not None:
                leaders[str(owner_id)] = {
                    "username": owner_name,
                    "display_name":
                    owner.get("displayName", ""),
                    "role_name":
                    "Divine Sister / Group Owner",
                    "category": "Owner",
                }

    snapshot = {
        "staff": staff,
        "roles": role_snapshot,
        "leaders": leaders,
        "updated_at": utc_iso(),
    }

    save_json(
        STATE_FILE,
        snapshot,
    )

    return snapshot


async def ensure_snapshot():
    snapshot = load_json(
        STATE_FILE,
        {},
    )

    if (
        not snapshot
        or not snapshot.get("staff")
        or not snapshot.get("leaders")
    ):
        return await build_snapshot()

    return snapshot


# =========================================================
# LEADERSHIP FRIEND GRAPH
# =========================================================

def filter_leaders(leaders, scope):
    scope = scope.lower()

    if scope == "matronas":
        wanted = "Matrona"
    elif scope == "reverends":
        wanted = "Reverend"
    elif scope == "owner":
        wanted = "Owner"
    else:
        return leaders

    return {
        user_id: info
        for user_id, info in leaders.items()
        if info.get("category") == wanted
    }


async def rebuild_leader_graph(snapshot=None):
    """
    Efficient core:
    Fetch each leadership account's friend list ONCE.
    Build reverse index:
       staff_user_id -> [leader_ids]
    """

    async with graph_lock:
        if snapshot is None:
            snapshot = await ensure_snapshot()

        leaders = snapshot.get(
            "leaders",
            {},
        )

        reverse_index = {}
        visibility = {}

        headers = {
            "User-Agent":
            "The-Witness-Divine-Sister-Court/2.0"
        }

        timeout = aiohttp.ClientTimeout(total=60)

        async with aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
        ) as session:

            for leader_id, leader in leaders.items():
                try:
                    friends = await get_friends(
                        session,
                        leader_id,
                    )

                    visibility[leader_id] = {
                        "status": "public",
                        "friend_count": len(friends),
                    }

                    for friend_id in friends:
                        reverse_index.setdefault(
                            str(friend_id),
                            [],
                        ).append(
                            str(leader_id)
                        )

                except Exception as exc:
                    visibility[leader_id] = {
                        "status": "unavailable",
                        "friend_count": None,
                        "error": type(exc).__name__,
                    }

                await asyncio.sleep(0.10)

        graph = {
            "built_at": utc_iso(),
            "leaders": leaders,
            "visibility": visibility,
            "reverse_index": reverse_index,
        }

        save_json(
            GRAPH_FILE,
            graph,
        )

        print(
            f"Witness graph: "
            f"{len(leaders)} leaders, "
            f"{len(reverse_index)} connected user IDs indexed."
        )

        return graph


async def ensure_leader_graph(
    snapshot=None,
    force=False,
):
    graph = load_json(
        GRAPH_FILE,
        {},
    )

    if (
        force
        or not graph
        or not is_fresh(
            graph.get("built_at"),
            LEADER_GRAPH_MINUTES,
        )
    ):
        return await rebuild_leader_graph(
            snapshot
        )

    return graph


def graph_coverage(graph, scope):
    leaders = filter_leaders(
        graph.get("leaders", {}),
        scope,
    )

    visibility = graph.get(
        "visibility",
        {},
    )

    visible = sum(
        1
        for leader_id in leaders
        if visibility.get(
            leader_id,
            {},
        ).get(
            "status"
        ) == "public"
    )

    return visible, len(leaders)


def connections_from_graph(
    subject,
    graph,
    scope,
):
    leaders = filter_leaders(
        graph.get("leaders", {}),
        scope,
    )

    leader_ids = graph.get(
        "reverse_index",
        {},
    ).get(
        str(subject["user_id"]),
        [],
    )

    results = []

    for leader_id in leader_ids:
        leader = leaders.get(
            str(leader_id)
        )

        if not leader:
            continue

        results.append({
            "user_id": str(leader_id),
            **leader,
        })

    results.sort(
        key=lambda item: (
            item.get("category", ""),
            item.get("username", "").lower(),
        )
    )

    return results


# =========================================================
# SUBJECT / RESULTS
# =========================================================

def subject_from_cached_user(
    user_id,
    info,
):
    return {
        "user_id": str(user_id),
        "username":
        info.get("username", "Unknown"),
        "display_name":
        info.get("display_name", ""),
        "role_name":
        info.get("role_name", "Unknown Role"),
    }


def build_single_result_embed(
    subject,
    connections,
    scope,
    graph,
):
    visible, total = graph_coverage(
        graph,
        scope,
    )

    if connections:
        colour = discord.Colour.gold()
        status = (
            f"Found **{len(connections)}** "
            f"public leadership friend connection(s)."
        )
    else:
        colour = discord.Colour.dark_grey()
        status = (
            "No indexed public leadership "
            "friend connections were found."
        )

    embed = discord.Embed(
        title="🔗 PUBLIC CONNECTION CHECK",
        description=(
            f"### {subject['username']}\n"
            f"**Current role:** "
            f"{subject.get('role_name', 'Unknown')}\n\n"
            f"{status}"
        ),
        colour=colour,
        timestamp=utc_now(),
    )

    embed.add_field(
        name="Scope",
        value=scope.title(),
        inline=True,
    )

    embed.add_field(
        name="Leadership Coverage",
        value=f"**{visible}/{total}** public lists",
        inline=True,
    )

    embed.add_field(
        name="Roblox Profile",
        value=(
            f"[Open profile]"
            f"({profile_url(subject['user_id'])})"
        ),
        inline=False,
    )

    for connection in connections[:20]:
        embed.add_field(
            name=(
                connection.get(
                    "username",
                    "Unknown",
                )
            ),
            value=(
                f"{connection.get('role_name', 'Unknown')}\n"
                f"`{connection.get('category', 'Leader')}`"
            ),
            inline=True,
        )

    if len(connections) > 20:
        embed.add_field(
            name="Additional Connections",
            value=(
                f"+{len(connections) - 20} more"
            ),
            inline=False,
        )

    embed.set_footer(
        text=(
            "The Witness • Cached leadership graph • "
            "Connection ≠ proof of favoritism"
        )
    )

    return embed


def build_bulk_result_embeds(
    title,
    results,
    scope,
    checked_count,
    graph,
):
    connected = [
        result
        for result in results
        if result["connections"]
    ]

    visible, total = graph_coverage(
        graph,
        scope,
    )

    pages = []

    if not connected:
        embed = discord.Embed(
            title=title,
            description=(
                f"Checked **{checked_count}** account(s).\n"
                f"No indexed leadership connections found.\n"
                f"Scope: **{scope.title()}**\n"
                f"Leadership coverage: "
                f"**{visible}/{total}** public friend lists."
            ),
            colour=discord.Colour.dark_grey(),
            timestamp=utc_now(),
        )

        embed.set_footer(
            text=(
                "The Witness • Connection ≠ proof of favoritism"
            )
        )

        return [embed]

    for page_start in range(
        0,
        min(len(connected), 200),
        20,
    ):
        page_results = connected[
            page_start:page_start + 20
        ]

        embed = discord.Embed(
            title=title,
            description=(
                f"Checked **{checked_count}** account(s).\n"
                f"**{len(connected)}** had at least one "
                f"indexed leadership connection.\n"
                f"Scope: **{scope.title()}**\n"
                f"Leadership coverage: "
                f"**{visible}/{total}** public friend lists."
            ),
            colour=discord.Colour.gold(),
            timestamp=utc_now(),
        )

        for result in page_results:
            subject = result["subject"]
            connections = result["connections"]

            lines = [
                (
                    f"• **{connection['username']}** "
                    f"— {connection['role_name']}"
                )
                for connection in connections[:8]
            ]

            if len(connections) > 8:
                lines.append(
                    f"• +{len(connections) - 8} more"
                )

            embed.add_field(
                name=(
                    f"🔗 {subject['username']} "
                    f"— {subject.get('role_name', 'Unknown')}"
                ),
                value="\n".join(lines),
                inline=False,
            )

        embed.set_footer(
            text=(
                "The Witness • Cached leadership graph • "
                "Connection ≠ proof of favoritism"
            )
        )

        pages.append(embed)

    return pages[:10]


async def post_single_result(
    guild,
    subject,
    connections,
    scope,
    graph,
):
    channel = find_channel(
        guild,
        CONNECTIONS_CHANNEL,
    )

    if not channel:
        raise RuntimeError(
            f"#{CONNECTIONS_CHANNEL} does not exist."
        )

    embed = build_single_result_embed(
        subject,
        connections,
        scope,
        graph,
    )

    return await channel.send(
        embed=embed
    )


async def post_bulk_results(
    guild,
    title,
    results,
    scope,
    checked_count,
    graph,
):
    channel = find_channel(
        guild,
        CONNECTIONS_CHANNEL,
    )

    if not channel:
        raise RuntimeError(
            f"#{CONNECTIONS_CHANNEL} does not exist."
        )

    embeds = build_bulk_result_embeds(
        title,
        results,
        scope,
        checked_count,
        graph,
    )

    await channel.send(
        embeds=embeds[:10]
    )


# =========================================================
# FAST LOOKUPS
# =========================================================

def cached_username_map(snapshot):
    return {
        info.get("username", "").lower():
        subject_from_cached_user(
            user_id,
            info,
        )
        for user_id, info
        in snapshot.get("staff", {}).items()
    }


async def resolve_subject(
    username,
    snapshot,
):
    cached = cached_username_map(
        snapshot
    ).get(
        username.lower()
    )

    if cached:
        return cached

    headers = {
        "User-Agent":
        "The-Witness-Divine-Sister-Court/2.0"
    }

    timeout = aiohttp.ClientTimeout(total=30)

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
    ) as session:

        resolved = await resolve_username(
            session,
            username,
        )

    if resolved is None:
        return None

    return {
        **resolved,
        "role_name":
        "Outside cached staff roster",
    }


# =========================================================
# AUTOCOMPLETE
# =========================================================

async def username_choices(current):
    snapshot = await ensure_snapshot()
    staff = snapshot.get("staff", {})

    current_lower = current.lower()

    matches = []

    for user_id, info in staff.items():
        username = info.get(
            "username",
            "Unknown",
        )

        display_name = info.get(
            "display_name",
            "",
        )

        searchable = (
            username + " " + display_name
        ).lower()

        if current_lower in searchable:
            label = username

            if display_name:
                label += f" ({display_name})"

            matches.append(
                app_commands.Choice(
                    name=label[:100],
                    value=username,
                )
            )

        if len(matches) >= 25:
            break

    return matches


async def role_choices(current):
    snapshot = await ensure_snapshot()
    roles = snapshot.get("roles", {})

    current_lower = current.lower()

    matches = []

    for role in sorted(
        roles.values(),
        key=lambda item:
        int(item.get("rank", 0)),
        reverse=True,
    ):
        name = role.get(
            "name",
            "Unknown Role",
        )

        if current_lower in name.lower():
            matches.append(
                app_commands.Choice(
                    name=(
                        f"{name} "
                        f"({role.get('count', 0)})"
                    )[:100],
                    value=name,
                )
            )

        if len(matches) >= 25:
            break

    return matches


SCOPE_CHOICES = [
    app_commands.Choice(
        name="All leadership",
        value="all",
    ),
    app_commands.Choice(
        name="Matronas only",
        value="matronas",
    ),
    app_commands.Choice(
        name="Reverends only",
        value="reverends",
    ),
    app_commands.Choice(
        name="Divine Sister / owner only",
        value="owner",
    ),
]


# =========================================================
# /connections — ONE USER, NEAR-INSTANT
# =========================================================

@bot.tree.command(
    name="connections",
    description=(
        "Check one Roblox user against "
        "cached DPI leadership friendships."
    ),
)
@app_commands.describe(
    username="Roblox username",
    scope="Leadership group to compare against",
)
@app_commands.choices(scope=SCOPE_CHOICES)
async def connections(
    interaction: discord.Interaction,
    username: str,
    scope: app_commands.Choice[str],
):
    await interaction.response.defer(
        ephemeral=True,
        thinking=True,
    )

    snapshot = await ensure_snapshot()
    graph = await ensure_leader_graph(
        snapshot
    )

    subject = await resolve_subject(
        username,
        snapshot,
    )

    if subject is None:
        await interaction.followup.send(
            "I couldn't find that Roblox username.",
            ephemeral=True,
        )
        return

    found = connections_from_graph(
        subject,
        graph,
        scope.value,
    )

    message = await post_single_result(
        interaction.guild,
        subject,
        found,
        scope.value,
        graph,
    )

    await interaction.followup.send(
        (
            f"🔗 Done — **{len(found)}** "
            f"connection(s). Posted in "
            f"{message.channel.mention}."
        ),
        ephemeral=True,
    )


@connections.autocomplete("username")
async def connections_autocomplete(
    interaction: discord.Interaction,
    current: str,
):
    return await username_choices(
        current
    )


# =========================================================
# /connectionsmany — NO PER-USER FRIEND API CALLS
# =========================================================

@bot.tree.command(
    name="connectionsmany",
    description=(
        "Check several users against "
        "cached DPI leadership friendships."
    ),
)
@app_commands.describe(
    usernames=(
        "Comma-separated usernames "
        f"(max {MAX_MULTI_USERS})"
    ),
    scope="Leadership group to compare against",
)
@app_commands.choices(scope=SCOPE_CHOICES)
async def connectionsmany(
    interaction: discord.Interaction,
    usernames: str,
    scope: app_commands.Choice[str],
):
    await interaction.response.defer(
        ephemeral=True,
        thinking=True,
    )

    names = list(
        dict.fromkeys(
            [
                piece.strip()
                for piece
                in usernames.replace(
                    "\n",
                    ",",
                ).split(",")
                if piece.strip()
            ]
        )
    )[:MAX_MULTI_USERS]

    if not names:
        await interaction.followup.send(
            "Give me at least one username.",
            ephemeral=True,
        )
        return

    snapshot = await ensure_snapshot()
    graph = await ensure_leader_graph(
        snapshot
    )

    subjects = []

    for name in names:
        subject = await resolve_subject(
            name,
            snapshot,
        )

        if subject:
            subjects.append(subject)

    results = [
        {
            "subject": subject,
            "connections":
            connections_from_graph(
                subject,
                graph,
                scope.value,
            ),
        }
        for subject in subjects
    ]

    await post_bulk_results(
        interaction.guild,
        "🔗 MULTI-USER CONNECTION CHECK",
        results,
        scope.value,
        len(subjects),
        graph,
    )

    connected_count = sum(
        bool(result["connections"])
        for result in results
    )

    channel = find_channel(
        interaction.guild,
        CONNECTIONS_CHANNEL,
    )

    await interaction.followup.send(
        (
            f"🔗 Checked **{len(subjects)}**. "
            f"**{connected_count}** had connections. "
            f"Results: {channel.mention}"
        ),
        ephemeral=True,
    )


# =========================================================
# /teamconnections — VERY FAST
# =========================================================

@bot.tree.command(
    name="teamconnections",
    description=(
        "Check an entire DPI rank against "
        "cached leadership friendships."
    ),
)
@app_commands.describe(
    rank="Staff rank/team",
    scope="Leadership group to compare against",
)
@app_commands.choices(scope=SCOPE_CHOICES)
async def teamconnections(
    interaction: discord.Interaction,
    rank: str,
    scope: app_commands.Choice[str],
):
    await interaction.response.defer(
        ephemeral=True,
        thinking=True,
    )

    snapshot = await ensure_snapshot()
    graph = await ensure_leader_graph(
        snapshot
    )

    staff = snapshot.get(
        "staff",
        {},
    )

    role_names = {
        info.get("role_name", "")
        for info in staff.values()
    }

    exact = next(
        (
            role_name
            for role_name in role_names
            if role_name.lower()
            == rank.lower()
        ),
        None,
    )

    chosen = exact

    if chosen is None:
        matches = [
            role_name
            for role_name in role_names
            if rank.lower()
            in role_name.lower()
        ]

        if len(matches) == 1:
            chosen = matches[0]

    if chosen is None:
        await interaction.followup.send(
            "I couldn't identify that rank. "
            "Use autocomplete.",
            ephemeral=True,
        )
        return

    subjects = [
        subject_from_cached_user(
            user_id,
            info,
        )
        for user_id, info
        in staff.items()
        if info.get("role_name")
        == chosen
    ][:MAX_TEAM_USERS]

    results = [
        {
            "subject": subject,
            "connections":
            connections_from_graph(
                subject,
                graph,
                scope.value,
            ),
        }
        for subject in subjects
    ]

    await post_bulk_results(
        interaction.guild,
        f"🔗 TEAM CONNECTION CHECK — {chosen}",
        results,
        scope.value,
        len(subjects),
        graph,
    )

    connected_count = sum(
        bool(result["connections"])
        for result in results
    )

    visible, total = graph_coverage(
        graph,
        scope.value,
    )

    channel = find_channel(
        interaction.guild,
        CONNECTIONS_CHANNEL,
    )

    await interaction.followup.send(
        (
            f"⚡ Scanned **{len(subjects)}** "
            f"member(s) of **{chosen}**. "
            f"**{connected_count}** had connections. "
            f"Leadership coverage: **{visible}/{total}**. "
            f"Results: {channel.mention}"
        ),
        ephemeral=True,
    )


@teamconnections.autocomplete("rank")
async def teamconnections_autocomplete(
    interaction: discord.Interaction,
    current: str,
):
    return await role_choices(
        current
    )


# =========================================================
# /checkfriendslist
# username OR entire rank/team
# =========================================================

async def privacy_probe_cached(
    subject,
    session,
):
    cache = load_json(
        PRIVACY_FILE,
        {},
    )

    user_id = str(
        subject["user_id"]
    )

    prior = cache.get(
        user_id,
        {},
    )

    if is_fresh(
        prior.get("checked_at"),
        PRIVACY_CACHE_MINUTES,
    ):
        return {
            "subject": subject,
            **prior,
            "cached": True,
        }

    result = await probe_friends_visibility(
        session,
        user_id,
    )

    record = {
        **result,
        "checked_at": utc_iso(),
    }

    cache[user_id] = record

    save_json(
        PRIVACY_FILE,
        cache,
    )

    return {
        "subject": subject,
        **record,
        "cached": False,
    }


@bot.tree.command(
    name="checkfriendslist",
    description=(
        "Check whether a user's or team's "
        "public Roblox friends list is accessible."
    ),
)
@app_commands.describe(
    username=(
        "One Roblox username "
        "(leave blank when using rank)"
    ),
    rank=(
        "Whole staff rank/team "
        "(leave blank when using username)"
    ),
)
async def checkfriendslist(
    interaction: discord.Interaction,
    username: str | None = None,
    rank: str | None = None,
):
    await interaction.response.defer(
        ephemeral=True,
        thinking=True,
    )

    if bool(username) == bool(rank):
        await interaction.followup.send(
            (
                "Choose **one**: either a username "
                "or a rank/team."
            ),
            ephemeral=True,
        )
        return

    snapshot = await ensure_snapshot()
    subjects = []
    label = ""

    if username:
        subject = await resolve_subject(
            username,
            snapshot,
        )

        if subject is None:
            await interaction.followup.send(
                "I couldn't find that Roblox username.",
                ephemeral=True,
            )
            return

        subjects = [subject]
        label = subject["username"]

    else:
        staff = snapshot.get(
            "staff",
            {},
        )

        role_names = {
            info.get("role_name", "")
            for info in staff.values()
        }

        exact = next(
            (
                name
                for name in role_names
                if name.lower()
                == rank.lower()
            ),
            None,
        )

        chosen = exact

        if chosen is None:
            matches = [
                name
                for name in role_names
                if rank.lower()
                in name.lower()
            ]

            if len(matches) == 1:
                chosen = matches[0]

        if chosen is None:
            await interaction.followup.send(
                "I couldn't identify that rank. "
                "Use autocomplete.",
                ephemeral=True,
            )
            return

        subjects = [
            subject_from_cached_user(
                user_id,
                info,
            )
            for user_id, info
            in staff.items()
            if info.get("role_name")
            == chosen
        ][:MAX_TEAM_USERS]

        label = chosen

    semaphore = asyncio.Semaphore(
        TEAM_CONCURRENCY
    )

    headers = {
        "User-Agent":
        "The-Witness-Divine-Sister-Court/2.0"
    }

    timeout = aiohttp.ClientTimeout(total=90)

    async with aiohttp.ClientSession(
        headers=headers,
        timeout=timeout,
    ) as session:

        async def worker(subject):
            async with semaphore:
                result = await privacy_probe_cached(
                    subject,
                    session,
                )

                await asyncio.sleep(0.10)

                return result

        results = await asyncio.gather(
            *[
                worker(subject)
                for subject in subjects
            ]
        )

    unavailable = [
        result
        for result in results
        if result.get("status")
        == "unavailable"
    ]

    errors = [
        result
        for result in results
        if result.get("status")
        == "error"
    ]

    public = [
        result
        for result in results
        if result.get("status")
        == "public"
    ]

    channel = find_channel(
        interaction.guild,
        CONNECTIONS_CHANNEL,
    )

    if not channel:
        await interaction.followup.send(
            f"I cannot find #{CONNECTIONS_CHANNEL}.",
            ephemeral=True,
        )
        return

    embed = discord.Embed(
        title="🔒 FRIENDS LIST VISIBILITY CHECK",
        description=(
            f"**Target:** {label}\n"
            f"Checked: **{len(results)}**\n"
            f"Public/readable: **{len(public)}**\n"
            f"Unavailable/private: **{len(unavailable)}**\n"
            f"Temporary errors: **{len(errors)}**"
        ),
        colour=(
            discord.Colour.orange()
            if unavailable
            else discord.Colour.green()
        ),
        timestamp=utc_now(),
    )

    if unavailable:
        lines = [
            (
                f"• **{result['subject']['username']}** "
                f"— HTTP "
                f"{result.get('http_status', '?')}"
            )
            for result in unavailable[:40]
        ]

        embed.add_field(
            name="Unavailable / Private",
            value="\n".join(lines),
            inline=False,
        )

        if len(unavailable) > 40:
            embed.add_field(
                name="More",
                value=(
                    f"+{len(unavailable) - 40} more "
                    f"unavailable account(s)"
                ),
                inline=False,
            )

    if errors:
        embed.add_field(
            name="Temporary Errors",
            value=(
                f"{len(errors)} account(s) could not "
                f"be confirmed this time. Re-run later."
            ),
            inline=False,
        )

    embed.set_footer(
        text=(
            f"The Witness • Results cached "
            f"{PRIVACY_CACHE_MINUTES} min • "
            "Unavailable does not explain why"
        )
    )

    await channel.send(
        embed=embed
    )

    await interaction.followup.send(
        (
            f"🔒 Friends-list check complete. "
            f"**{len(unavailable)}** unavailable/private. "
            f"Posted in {channel.mention}."
        ),
        ephemeral=True,
    )


@checkfriendslist.autocomplete("username")
async def friends_username_autocomplete(
    interaction: discord.Interaction,
    current: str,
):
    return await username_choices(
        current
    )


@checkfriendslist.autocomplete("rank")
async def friends_rank_autocomplete(
    interaction: discord.Interaction,
    current: str,
):
    return await role_choices(
        current
    )


# =========================================================
# /witnessrefresh / /witnessstatus
# =========================================================

@bot.tree.command(
    name="witnessrefresh",
    description=(
        "Refresh staff, leadership, "
        "and the optimized leadership friend graph."
    ),
)
async def witnessrefresh(
    interaction: discord.Interaction,
):
    await interaction.response.defer(
        ephemeral=True,
        thinking=True,
    )

    try:
        snapshot = await build_snapshot()
        graph = await ensure_leader_graph(
            snapshot,
            force=True,
        )

        visible, total = graph_coverage(
            graph,
            "all",
        )

        await interaction.followup.send(
            (
                f"👁️ Refreshed **{len(snapshot['staff'])}** staff, "
                f"**{len(snapshot['leaders'])}** leadership targets, "
                f"and the connection graph. "
                f"Friend-list coverage: **{visible}/{total}**."
            ),
            ephemeral=True,
        )

    except Exception as exc:
        print(
            "Witness refresh error:",
            repr(exc),
        )

        await interaction.followup.send(
            f"❌ Refresh failed: `{type(exc).__name__}`",
            ephemeral=True,
        )


@bot.tree.command(
    name="witnessstatus",
    description="Show The Witness cache and watcher status.",
)
async def witnessstatus(
    interaction: discord.Interaction,
):
    snapshot = load_json(
        STATE_FILE,
        {},
    )

    graph = load_json(
        GRAPH_FILE,
        {},
    )

    leaders = snapshot.get(
        "leaders",
        {},
    )

    matronas = sum(
        1
        for info in leaders.values()
        if info.get("category")
        == "Matrona"
    )

    reverends = sum(
        1
        for info in leaders.values()
        if info.get("category")
        == "Reverend"
    )

    owners = sum(
        1
        for info in leaders.values()
        if info.get("category")
        == "Owner"
    )

    visible, total = graph_coverage(
        graph,
        "all",
    )

    embed = discord.Embed(
        title="👁️ The Witness — Efficient Status",
        colour=discord.Colour.gold(),
        timestamp=utc_now(),
    )

    embed.add_field(
        name="Staff Cached",
        value=str(
            len(snapshot.get("staff", {}))
        ),
        inline=True,
    )

    embed.add_field(
        name="Matronas",
        value=str(matronas),
        inline=True,
    )

    embed.add_field(
        name="Reverends",
        value=str(reverends),
        inline=True,
    )

    embed.add_field(
        name="Owner",
        value=str(owners),
        inline=True,
    )

    embed.add_field(
        name="Leader Friend Lists",
        value=f"{visible}/{total} public",
        inline=True,
    )

    embed.add_field(
        name="Friend Graph Age",
        value=discord_relative_time(
            graph.get("built_at")
        ),
        inline=True,
    )

    embed.add_field(
        name="Staff Scan",
        value=(
            f"Every {STAFF_SCAN_MINUTES} min"
        ),
        inline=True,
    )

    embed.add_field(
        name="Graph Refresh",
        value=(
            f"Every {LEADER_GRAPH_MINUTES} min"
        ),
        inline=True,
    )

    embed.add_field(
        name="Output",
        value=f"#{CONNECTIONS_CHANNEL}",
        inline=True,
    )

    presence_state = load_json(
        PRESENCE_STATE_FILE,
        {},
    )

    visible_in_dpi = sum(
        1
        for item in presence_state.values()
        if item.get("in_dpi")
    )

    embed.add_field(
        name="DPI Presence Watch",
        value=(
            f"Every {PRESENCE_SCAN_MINUTES} min • "
            f"{visible_in_dpi} visible in-game"
        ),
        inline=False,
    )

    embed.add_field(
        name="Presence Output",
        value=f"#{PRESENCE_CHANNEL}",
        inline=True,
    )

    embed.add_field(
        name="Current Server Roster",
        value=(
            f"Every {CURRENT_SERVER_REFRESH_MINUTES} min • "
            f"#{CURRENT_SERVER_CHANNEL}"
        ),
        inline=False,
    )

    embed.set_footer(
        text=(
            "Team scans use cached leader graph: "
            "fast + far fewer Roblox friend API calls"
        )
    )

    await interaction.response.send_message(
        embed=embed,
        ephemeral=True,
    )


# =========================================================
# AUTOMATIC PROMOTION CONNECTION WATCH
# =========================================================

def rank_change_key(
    user_id,
    old_role_id,
    new_role_id,
):
    return (
        f"{user_id}:"
        f"{old_role_id}:"
        f"{new_role_id}"
    )


def detect_promotion_candidates(
    old_staff,
    new_staff,
):
    events = []

    for user_id, new in new_staff.items():
        old = old_staff.get(
            user_id
        )

        if old is None:
            events.append({
                "type": "new_staff",
                "user_id": user_id,
                "old_role_id": None,
                "new_role_id":
                new.get("role_id"),
                "subject":
                subject_from_cached_user(
                    user_id,
                    new,
                ),
            })

            continue

        if (
            old.get("role_id")
            == new.get("role_id")
        ):
            continue

        if int(
            new.get("role_rank", 0)
        ) > int(
            old.get("role_rank", 0)
        ):
            events.append({
                "type": "promotion",
                "user_id": user_id,
                "old_role_id":
                old.get("role_id"),
                "new_role_id":
                new.get("role_id"),
                "subject":
                subject_from_cached_user(
                    user_id,
                    new,
                ),
            })

    return events


def confirm_promotion_events(
    candidates,
):
    pending = load_json(
        PENDING_FILE,
        {},
    )

    next_pending = {}
    confirmed = []

    for event in candidates:
        key = rank_change_key(
            event["user_id"],
            event.get("old_role_id"),
            event.get("new_role_id"),
        )

        count = int(
            pending.get(
                key,
                {},
            ).get(
                "count",
                0,
            )
        ) + 1

        if count >= CONFIRM_SCANS:
            confirmed.append(
                event
            )
        else:
            next_pending[key] = {
                "count": count,
                "event": event,
            }

    save_json(
        PENDING_FILE,
        next_pending,
    )

    return confirmed


@tasks.loop(
    minutes=STAFF_SCAN_MINUTES
)
async def promotion_connection_watch():
    async with scan_lock:
        try:
            previous = load_json(
                STAFF_CACHE_FILE,
                {},
            )

            snapshot = await build_snapshot()
            current = snapshot["staff"]

            if not previous:
                save_json(
                    STAFF_CACHE_FILE,
                    current,
                )

                print(
                    f"Witness baseline: "
                    f"{len(current)} staff."
                )

                return

            candidates = (
                detect_promotion_candidates(
                    previous,
                    current,
                )
            )

            confirmed = (
                confirm_promotion_events(
                    candidates
                )
            )

            baseline_to_save = dict(
                current
            )

            confirmed_ids = {
                event["user_id"]
                for event in confirmed
            }

            for event in candidates:
                user_id = event["user_id"]

                if user_id in confirmed_ids:
                    continue

                if user_id in previous:
                    baseline_to_save[
                        user_id
                    ] = previous[user_id]
                else:
                    baseline_to_save.pop(
                        user_id,
                        None,
                    )

            save_json(
                STAFF_CACHE_FILE,
                baseline_to_save,
            )

            if not confirmed:
                return

            graph = await ensure_leader_graph(
                snapshot
            )

            results = []

            for event in confirmed:
                subject = event["subject"]

                found = connections_from_graph(
                    subject,
                    graph,
                    "all",
                )

                if found:
                    results.append({
                        "subject": subject,
                        "connections": found,
                    })

            if results:
                for guild in bot.guilds:
                    await post_bulk_results(
                        guild,
                        "👁️ PROMOTION CONNECTION WATCH",
                        results,
                        "all",
                        len(confirmed),
                        graph,
                    )

            print(
                f"Witness promotion scan: "
                f"{len(confirmed)} confirmed, "
                f"{len(results)} with indexed connections."
            )

        except Exception as exc:
            print(
                "Promotion watcher error:",
                repr(exc),
            )


@promotion_connection_watch.before_loop
async def before_promotion_connection_watch():
    await bot.wait_until_ready()


@tasks.loop(
    minutes=LEADER_GRAPH_MINUTES
)
async def leader_graph_refresh():
    try:
        snapshot = await ensure_snapshot()

        await rebuild_leader_graph(
            snapshot
        )

    except Exception as exc:
        print(
            "Leader graph refresh error:",
            repr(exc),
        )


@leader_graph_refresh.before_loop
async def before_graph_refresh():
    await bot.wait_until_ready()




# =========================================================
# SHARED PRESENCE CACHE
# =========================================================

def _presence_cache_fresh():
    if presence_cache_at is None:
        return False

    age = (
        utc_now() - presence_cache_at
    ).total_seconds()

    return age < PRESENCE_CACHE_SECONDS


async def get_shared_staff_presences(
    snapshot=None,
):
    """
    Shared automatic presence scan.

    IMPORTANT:
    Cache validity is based on scan AGE, not on whether Roblox returned
    every requested user. Hidden/offline users may be absent from a public
    presence response, and treating those absences as a broken cache caused
    manual commands to repeatedly rescan hundreds of accounts.

    The automatic 5-minute watchers still perform the real Roblox scan.
    """
    global presence_cache
    global presence_cache_at

    if snapshot is None:
        snapshot = await ensure_snapshot()

    people = all_tracked_people(snapshot)

    if _presence_cache_fresh():
        return snapshot, dict(presence_cache)

    async with presence_lock:
        # Another automatic task may have refreshed it while we waited.
        if _presence_cache_fresh():
            return snapshot, dict(presence_cache)

        headers = {
            "User-Agent":
            "The-Witness-Divine-Sister-Court/5.1"
        }

        timeout = aiohttp.ClientTimeout(
            total=180
        )

        async with aiohttp.ClientSession(
            headers=headers,
            timeout=timeout,
        ) as session:
            fresh = await get_user_presences(
                session,
                list(people.keys()),
            )

        # Only replace the cache after a complete successful scan.
        presence_cache = fresh
        presence_cache_at = utc_now()

        print(
            f"Witness presence cache: "
            f"{len(fresh)} public presence records returned "
            f"for {len(people)} tracked accounts."
        )

        return snapshot, dict(fresh)


def _latest_presence_state_time(state):
    latest = None

    for item in state.values():
        value = item.get("last_checked")
        if not value:
            continue

        try:
            parsed = datetime.fromisoformat(
                value.replace("Z", "+00:00")
            )
        except Exception:
            continue

        if latest is None or parsed > latest:
            latest = parsed

    return latest


def _discord_relative_time(dt):
    if dt is None:
        return "unknown"

    try:
        return f"<t:{int(dt.timestamp())}:R>"
    except Exception:
        return "unknown"


def _presence_cache_relative_time():
    if presence_cache_at is None:
        return "waiting for first scan"

    return _discord_relative_time(
        presence_cache_at
    )


# =========================================================
# LEADERSHIP PRESENCE WATCH
# =========================================================

def tracked_leadership(snapshot):
    """
    Uses the same automatically-discovered leadership set as The Witness:
    - every current Matrona
    - every current Reverend rank
    - Divine Sister / Roblox group owner
    """
    return snapshot.get("leaders", {})


async def post_presence_joins(guild, joined):
    if not joined:
        return

    channel = find_channel(
        guild,
        PRESENCE_CHANNEL,
    )

    if not channel:
        print(
            f"Witness presence: #{PRESENCE_CHANNEL} "
            f"does not exist in {guild.name}."
        )
        return

    lines = []

    for event in joined:
        leader = event["leader"]
        presence = event["presence"]

        username = leader.get(
            "username",
            "Unknown",
        )

        role_name = leader.get(
            "role_name",
            leader.get(
                "category",
                "Leadership",
            ),
        )

        location = presence_location_text(
            presence
        )

        lines.append(
            f"👁️ **{username}** — {role_name}\n"
            f"↳ Joined **De Pride Isle Sanatorium**\n"
            f"↳ `{location}`\n"
            f"↳ [Roblox profile]"
            f"({profile_url(event['user_id'])})"
        )

    chunks = []
    current = ""

    for block in lines:
        piece = block + "\n\n"

        if (
            len(current) + len(piece) > 3800
            and current
        ):
            chunks.append(
                current.rstrip()
            )
            current = ""

        current += piece

    if current:
        chunks.append(
            current.rstrip()
        )

    for index, description in enumerate(
        chunks
    ):
        suffix = (
            f" • {index + 1}/{len(chunks)}"
            if len(chunks) > 1
            else ""
        )

        embed = discord.Embed(
            title=(
                f"👁️ DPI LEADERSHIP JOIN DETECTED "
                f"— {len(joined)}{suffix}"
            ),
            description=description,
            colour=discord.Colour.purple(),
            timestamp=utc_now(),
        )

        embed.set_footer(
            text=(
                f"The Witness • checks every "
                f"{PRESENCE_SCAN_MINUTES} min • "
                "public Roblox presence only"
            )
        )

        await channel.send(
            embed=embed
        )


async def perform_presence_scan(
    *,
    post_joins=True,
):
    snapshot = await ensure_snapshot()
    leaders = tracked_leadership(snapshot)

    if not leaders:
        return {
            "tracked": 0,
            "in_dpi": [],
            "joined": [],
            "hidden_or_other": 0,
        }

    # IMPORTANT: no second Roblox scan here.
    # Reuse the same cached staff-presence result as #current-server.
    snapshot, presences = (
        await get_shared_staff_presences(
            snapshot
        )
    )

    previous = load_json(
        PRESENCE_STATE_FILE,
        {},
    )

    new_state = {}
    joined = []
    in_dpi = []

    for user_id, leader in leaders.items():
        presence = presences.get(
            str(user_id),
            {},
        )

        is_dpi = presence_is_dpi(
            presence
        )

        was_dpi = bool(
            previous.get(
                str(user_id),
                {},
            ).get(
                "in_dpi",
                False,
            )
        )

        new_state[str(user_id)] = {
            "in_dpi": is_dpi,
            "last_checked": utc_iso(),
            "presence_type":
                presence.get(
                    "userPresenceType"
                ),
            "place_id":
                presence.get("placeId"),
            "universe_id":
                presence.get(
                    "universeId"
                ),
            "game_id":
                presence.get("gameId"),
            "last_location":
                presence.get(
                    "lastLocation"
                ),
        }

        if is_dpi:
            in_dpi.append({
                "user_id":
                    str(user_id),
                "leader": leader,
                "presence":
                    presence,
            })

        if (
            is_dpi
            and not was_dpi
            and previous
        ):
            joined.append({
                "user_id":
                    str(user_id),
                "leader": leader,
                "presence":
                    presence,
            })

    save_json(
        PRESENCE_STATE_FILE,
        new_state,
    )

    if (
        post_joins
        and joined
    ):
        for guild in bot.guilds:
            await post_presence_joins(
                guild,
                joined,
            )

    return {
        "tracked": len(leaders),
        "in_dpi": in_dpi,
        "joined": joined,
        "hidden_or_other":
            len(leaders) - len(in_dpi),
    }

@tasks.loop(
    minutes=PRESENCE_SCAN_MINUTES
)
async def leadership_presence_watch():
    try:
        result = await perform_presence_scan(
            post_joins=True
        )

        if result["joined"]:
            print(
                f"Witness presence: "
                f"{len(result['joined'])} "
                f"DPI leadership join(s) detected."
            )

    except Exception as exc:
        print(
            "Leadership presence watcher error:",
            repr(exc),
        )


@leadership_presence_watch.before_loop
async def before_leadership_presence_watch():
    await bot.wait_until_ready()


@bot.tree.command(
    name="presencecheck",
    description=(
        "Instantly show the latest automatic DPI leadership presence scan."
    ),
)
async def presencecheck(
    interaction: discord.Interaction,
):
    try:
        snapshot = load_json(
            STATE_FILE,
            {},
        )

        leaders = snapshot.get(
            "leaders",
            {},
        )

        state = load_json(
            PRESENCE_STATE_FILE,
            {},
        )

        if not leaders or not state:
            await interaction.response.send_message(
                (
                    "👁️ The Witness is still warming up its "
                    "automatic presence cache. Try again after "
                    "the first 5-minute scan."
                ),
                ephemeral=True,
            )
            return

        current = []

        for user_id, leader in leaders.items():
            item = state.get(
                str(user_id),
                {},
            )

            if item.get("in_dpi"):
                current.append(
                    (
                        int(
                            snapshot.get(
                                "staff",
                                {},
                            ).get(
                                str(user_id),
                                {},
                            ).get(
                                "role_rank",
                                10000
                                if leader.get("category") == "Owner"
                                else 0,
                            )
                            or 0
                        ),
                        leader,
                        item,
                    )
                )

        current.sort(
            key=lambda row: (
                -row[0],
                row[1].get(
                    "username",
                    "",
                ).lower(),
            )
        )

        checked_at = _latest_presence_state_time(
            state
        )

        if not current:
            await interaction.response.send_message(
                (
                    f"👁️ **Latest DPI leadership check**\n"
                    f"No Matrona, Reverend, or Divine Sister/"
                    f"owner was publicly visible in DPI.\n\n"
                    f"Last automatic scan: "
                    f"{_discord_relative_time(checked_at)}\n"
                    f"Updates automatically every "
                    f"**{PRESENCE_SCAN_MINUTES} minutes**."
                ),
                ephemeral=True,
            )
            return

        lines = []

        for _, leader, item in current:
            game_id = item.get(
                "game_id"
            )

            server_bit = (
                f" • server `{game_id}`"
                if game_id
                else ""
            )

            lines.append(
                f"• **{leader.get('username', 'Unknown')}** "
                f"— {leader.get('role_name', leader.get('category', 'Leadership'))}"
                f"{server_bit}"
            )

        await interaction.response.send_message(
            (
                f"👁️ **Leadership publicly visible in DPI — "
                f"{len(current)}**\n"
                + "\n".join(lines)
                + f"\n\nLast automatic scan: "
                + _discord_relative_time(checked_at)
                + f" • updates every **{PRESENCE_SCAN_MINUTES} min**"
                + "\nPresence privacy can hide some users."
            ),
            ephemeral=True,
        )

    except Exception as exc:
        print(
            "Instant presence check error:",
            repr(exc),
        )

        if interaction.response.is_done():
            await interaction.followup.send(
                f"❌ Presence check failed: "
                f"`{type(exc).__name__}`",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"❌ Presence check failed: "
                f"`{type(exc).__name__}`",
                ephemeral=True,
            )



# =========================================================
# CURRENT DPI SERVERS — LIVE STAFF ROSTER
# =========================================================

def _parse_iso(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except Exception:
        return None


def _duration_text(start_iso):
    start = _parse_iso(start_iso)
    if not start:
        return None

    seconds = max(0, int((utc_now() - start).total_seconds()))
    minutes = seconds // 60

    if minutes < 1:
        return "<1m"
    if minutes < 60:
        return f"{minutes}m"

    hours, mins = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {mins}m"

    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"


def all_tracked_people(snapshot):
    people = {
        str(user_id): dict(info)
        for user_id, info in snapshot.get("staff", {}).items()
    }

    for user_id, leader in snapshot.get("leaders", {}).items():
        uid = str(user_id)

        if uid not in people:
            people[uid] = {
                "username": leader.get("username", "Unknown"),
                "display_name": leader.get("display_name", ""),
                "role_name": leader.get("role_name", "Leadership"),
                "role_rank": 10000 if leader.get("category") == "Owner" else 0,
            }

        people[uid]["leader_category"] = leader.get("category")

    return people


def leadership_badge(person):
    category = person.get("leader_category")
    if category == "Owner":
        return " 👑"
    if category == "Reverend":
        return " ✝️"
    if category == "Matrona":
        return " 🟣"
    return ""


def group_staff_by_server(snapshot, presences):
    people = all_tracked_people(snapshot)
    grouped = {}
    unknown_server = []

    for user_id, person in people.items():
        presence = presences.get(str(user_id), {})
        if not presence_is_dpi(presence):
            continue

        record = {
            "user_id": str(user_id),
            "person": person,
            "presence": presence,
        }

        server_id = presence.get("gameId")
        if server_id:
            grouped.setdefault(str(server_id), []).append(record)
        else:
            unknown_server.append(record)

    return grouped, unknown_server


def update_session_tracker(grouped, unknown_server):
    old = load_json(CURRENT_SERVER_STATE_FILE, {})
    old_sessions = old.get("sessions", {})
    now_iso = utc_iso()
    sessions = {}

    def remember(record, server_key):
        uid = str(record["user_id"])
        old_item = old_sessions.get(uid, {})
        first_seen = (
            old_item.get("first_seen")
            if old_item.get("server_id") == server_key
            else now_iso
        ) or now_iso

        sessions[uid] = {
            "server_id": server_key,
            "first_seen": first_seen,
            "last_seen": now_iso,
        }
        record["first_seen"] = first_seen

    for server_id, members in grouped.items():
        for record in members:
            remember(record, str(server_id))

    for record in unknown_server:
        remember(record, "dpi-server-hidden")

    return sessions


def _sorted_members(members):
    return sorted(
        members,
        key=lambda item: (
            -int(item["person"].get("role_rank", 0) or 0),
            item["person"].get("username", "").lower(),
        ),
    )


def build_current_server_embeds(snapshot, presences):
    grouped, unknown_server = group_staff_by_server(snapshot, presences)
    sessions = update_session_tracker(grouped, unknown_server)

    total_visible = sum(len(v) for v in grouped.values()) + len(unknown_server)

    server_groups = sorted(
        grouped.items(),
        key=lambda item: (
            -max(int(r["person"].get("role_rank", 0) or 0) for r in item[1]),
            -len(item[1]),
            item[0],
        ),
    )

    blocks = []

    for index, (server_id, members) in enumerate(server_groups, start=1):
        lines = []
        for record in _sorted_members(members):
            person = record["person"]
            badge = leadership_badge(person)
            duration = _duration_text(record.get("first_seen"))
            duration_bit = f" • seen {duration}" if duration else ""

            lines.append(
                f"• **{person.get('username', 'Unknown')}**{badge} "
                f"— {person.get('role_name', 'Unknown Rank')}{duration_bit}"
            )

        blocks.append(
            f"### 🛰️ Server {index} — {len(members)} staff visible\n"
            f"`gameId: {server_id}`\n" + "\n".join(lines)
        )

    if unknown_server:
        lines = []
        for record in _sorted_members(unknown_server):
            person = record["person"]
            badge = leadership_badge(person)
            duration = _duration_text(record.get("first_seen"))
            duration_bit = f" • seen {duration}" if duration else ""
            lines.append(
                f"• **{person.get('username', 'Unknown')}**{badge} "
                f"— {person.get('role_name', 'Unknown Rank')}{duration_bit}"
            )

        blocks.append(
            "### 👁️ In DPI — server instance hidden\n" + "\n".join(lines)
        )

    if not blocks:
        blocks = [
            "No tracked rank-50+ staff are currently **publicly visible** "
            "inside De Pride Isle Sanatorium."
        ]

    descriptions = []
    current = ""

    for block in blocks:
        piece = block + "\n\n"
        if current and len(current) + len(piece) > 3800:
            descriptions.append(current.rstrip())
            current = ""

        if len(piece) > 3800:
            for line in piece.splitlines():
                line_piece = line + "\n"
                if current and len(current) + len(line_piece) > 3800:
                    descriptions.append(current.rstrip())
                    current = ""
                current += line_piece
        else:
            current += piece

    if current:
        descriptions.append(current.rstrip())

    embeds = []

    for index, description in enumerate(descriptions, start=1):
        suffix = f" • {index}/{len(descriptions)}" if len(descriptions) > 1 else ""
        embed = discord.Embed(
            title=f"🛰️ CURRENT DPI SERVERS{suffix}",
            description=(
                f"Visible staff in DPI: **{total_visible}**\n"
                f"Sorted **highest rank → lowest rank** inside each server.\n"
                f"👑 Owner • ✝️ Reverend • 🟣 Matrona\n\n"
                f"{description}"
            ),
            colour=discord.Colour.blurple(),
            timestamp=utc_now(),
        )
        embed.set_footer(
            text=(
                f"The Witness • refreshes every "
                f"{CURRENT_SERVER_REFRESH_MINUTES} min • "
                "public Roblox presence only • "
                "'seen' time is approximate from first detection"
            )
        )
        embeds.append(embed)

    return embeds, sessions


async def publish_current_server_roster(guild, snapshot, presences):
    channel = find_channel(guild, CURRENT_SERVER_CHANNEL)
    if not channel:
        print(f"Witness current-server: #{CURRENT_SERVER_CHANNEL} does not exist in {guild.name}.")
        return

    embeds, sessions = build_current_server_embeds(snapshot, presences)
    state = load_json(CURRENT_SERVER_STATE_FILE, {})
    all_message_ids = state.get("message_ids", {})
    old_ids = all_message_ids.get(str(guild.id), [])

    if isinstance(old_ids, (str, int)):
        old_ids = [str(old_ids)]

    messages = []
    for raw_id in old_ids:
        try:
            messages.append(await channel.fetch_message(int(raw_id)))
        except (discord.NotFound, discord.Forbidden, discord.HTTPException, ValueError):
            pass

    if not messages:
        try:
            recovered = []
            async for old in channel.history(limit=50):
                if (
                    old.author.id == bot.user.id
                    and old.embeds
                    and old.embeds[0].title
                    and old.embeds[0].title.startswith("🛰️ CURRENT DPI SERVERS")
                ):
                    recovered.append(old)
            messages = list(reversed(recovered[:10]))
        except discord.HTTPException:
            messages = []

    final_messages = []

    for index, embed in enumerate(embeds):
        if index < len(messages):
            await messages[index].edit(embed=embed)
            final_messages.append(messages[index])
        else:
            final_messages.append(await channel.send(embed=embed))

    for message in messages[len(embeds):]:
        try:
            await message.delete()
        except (discord.NotFound, discord.Forbidden, discord.HTTPException):
            pass

    all_message_ids[str(guild.id)] = [str(message.id) for message in final_messages]

    save_json(
        CURRENT_SERVER_STATE_FILE,
        {
            "message_ids": all_message_ids,
            "sessions": sessions,
            "updated_at": utc_iso(),
        },
    )


async def get_current_staff_presences(
    snapshot=None,
):
    return await get_shared_staff_presences(
        snapshot
    )

async def refresh_current_servers_once():
    snapshot = await ensure_snapshot()
    snapshot, presences = await get_current_staff_presences(snapshot)

    for guild in bot.guilds:
        await publish_current_server_roster(guild, snapshot, presences)

    return snapshot, presences


@tasks.loop(minutes=CURRENT_SERVER_REFRESH_MINUTES)
async def current_server_roster_watch():
    try:
        await refresh_current_servers_once()
    except Exception as exc:
        print("Current server roster watcher error:", repr(exc))


@current_server_roster_watch.before_loop
async def before_current_server_roster_watch():
    await bot.wait_until_ready()


@bot.tree.command(
    name="currentservers",
    description=(
        "Instantly open the latest automatic DPI server roster."
    ),
)
async def currentservers(
    interaction: discord.Interaction,
):
    try:
        channel = find_channel(
            interaction.guild,
            CURRENT_SERVER_CHANNEL,
        )

        if not channel:
            await interaction.response.send_message(
                (
                    f"❌ I cannot find "
                    f"`#{CURRENT_SERVER_CHANNEL}`."
                ),
                ephemeral=True,
            )
            return

        # Never trigger a fresh Roblox scan from a slash command.
        # The automatic watcher owns API traffic and refreshes every 5 min.
        # If an in-memory cache exists, re-render it in the background using
        # LOCAL cached data only. The user gets an instant response.
        snapshot = load_json(
            STATE_FILE,
            {},
        )

        if snapshot and presence_cache:
            asyncio.create_task(
                publish_current_server_roster(
                    interaction.guild,
                    snapshot,
                    dict(presence_cache),
                )
            )

        await interaction.response.send_message(
            (
                f"🛰️ Latest server roster: "
                f"{channel.mention}\n"
                f"Last presence cache: "
                f"**{_presence_cache_relative_time()}**\n"
                f"It refreshes automatically every "
                f"**{CURRENT_SERVER_REFRESH_MINUTES} minutes** — "
                f"this command does **not** hit Roblox again, "
                f"so it responds instantly and avoids rate limits."
            ),
            ephemeral=True,
        )

    except Exception as exc:
        print(
            "Instant current-server command error:",
            repr(exc),
        )

        if interaction.response.is_done():
            await interaction.followup.send(
                f"❌ Current server command failed: "
                f"`{type(exc).__name__}`",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                f"❌ Current server command failed: "
                f"`{type(exc).__name__}`",
                ephemeral=True,
            )


# =========================================================
# READY / COMMAND SYNC
# =========================================================

synced_once = False


@bot.event
async def on_ready():
    global synced_once

    print(
        f"The Witness is online as {bot.user}"
    )

    if not synced_once:
        # Remove old guild duplicates.
        for guild in bot.guilds:
            try:
                bot.tree.clear_commands(
                    guild=guild
                )

                await bot.tree.sync(
                    guild=guild
                )

            except Exception as exc:
                print(
                    f"Guild command cleanup error "
                    f"in {guild.name}:",
                    repr(exc),
                )

        try:
            synced = await bot.tree.sync()

            print(
                f"Synced {len(synced)} "
                f"global slash commands."
            )

        except Exception as exc:
            print(
                "Global command sync error:",
                repr(exc),
            )

        synced_once = True

    try:
        snapshot = await ensure_snapshot()

        # Build efficient graph immediately if missing/stale.
        await ensure_leader_graph(
            snapshot
        )

    except Exception as exc:
        print(
            "Initial Witness cache error:",
            repr(exc),
        )

    if not promotion_connection_watch.is_running():
        promotion_connection_watch.start()

    if not leader_graph_refresh.is_running():
        leader_graph_refresh.start()

    if not leadership_presence_watch.is_running():
        leadership_presence_watch.start()

    if not current_server_roster_watch.is_running():
        try:
            await refresh_current_servers_once()
        except Exception as exc:
            print("Initial current-server refresh error:", repr(exc))

        current_server_roster_watch.start()


# =========================================================
# START
# =========================================================

if not TOKEN:
    raise RuntimeError(
        "DISCORD_TOKEN was not found. "
        "Check your Railway variable."
    )

bot.run(TOKEN)
