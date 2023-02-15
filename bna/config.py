from dataclasses import dataclass, field


@dataclass
class IdenaConfig:
    rpc_interval: int = 2
    identities_cache_interval: int = 300

@dataclass
class CexConfig:
    interval: int = 6

@dataclass
class BscConfig:
    # After this many seconds a transfer is considerd finalized and sent to `event_chan`
    transfer_delay: int = 5
    # Expect a message at least this often
    ws_event_timeout: int = 60

@dataclass
class DatabaseConfig:
    # Transfers and trades older than this many seconds will be evicted from cache every hour
    cached_record_age_limit: int = 86400

@dataclass
class TrackerConfig:
    recent_transfers_period: int = 60 * 60  # seconds
    recent_transfers_threshold: int = 1000  # USD
    recent_dex_volume_threshold: int = 500  # USD
    cex_volume_period: int = 10 * 60        # seconds
    cex_volume_threshold: int = 500         # USD
    killtx_stake_threshold: int = 500       # usd
    killtx_age_threshold: int = 40          # epochs
    pool_identities_moved_threshold: int = 10    # number
    pool_identities_moved_period = 24 * 60 * 60  # seconds, bounded by transfers_period if state is reset
    top_events_max_lines = 10               # lines
    majority_volume_fraction: float = 0.75  # float 0-1
    stats_interval: int = 8 * 60 * 60       # seconds

@dataclass
class DiscordBotConfig:
    # Rate limit per user - sleep for `sleep` seconds if user ran `number` of commands in `time`
    user_ratelimit_sleep: int = 5
    user_ratelimit_command_number: int = 5
    user_ratelimit_command_time: int = 120
    notif_channel: int = 634497771457609759
    admin_users: list[int] = field(default_factory=list)
    admin_roles: list[int] = field(default_factory=list)
    command_roles: list[int] = field(default_factory=list)
    pool_event_replace_period: int = 4 * 60 * 60  # seconds
    event_replace_period: int = 1 * 60 * 60  # seconds
    rank_addresses: dict = field(default_factory=dict)

@dataclass
class Config:
    tracker: TrackerConfig = field(default_factory=TrackerConfig)
    bsc: BscConfig = field(default_factory=BscConfig)
    idena: IdenaConfig = field(default_factory=IdenaConfig)
    cex: CexConfig = field(default_factory=CexConfig)
    discord: DiscordBotConfig = field(default_factory=DiscordBotConfig)
    db: DatabaseConfig = field(default_factory=DatabaseConfig)
