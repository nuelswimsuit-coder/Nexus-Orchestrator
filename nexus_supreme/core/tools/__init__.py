# Nexus Supreme — Utility Tools (27-tool suite)
# Modules:
#   monitor          — DevOps / system metrics (tools 1–6)
#   intelligence     — AI analytics & SEO      (tools 7–12)
#   media_tools      — Branding Studio         (tools 13–18)
#   marketing        — Marketing & AHU Portal  (tools 19–27)
#   telegram_handlers — aiogram registration (register_all_tools)

from .monitor import (
    get_system_metrics, format_sysmon,
    tail_logs,
    check_sessions_health, format_sessions,
    watchdog_status, watchdog_enable, watchdog_disable,
)
from .intelligence import (
    check_seo_visibility, format_seo,
    scan_channel_trends, format_trends,
    build_stats_report,
    calculate_roi,
    detect_hostile_bots, format_hostile,
    analyze_archive,
)
from .media_tools import (
    generate_emoji_set, format_emoji_gen,
    convert_to_webm_sticker, format_sticker,
    apply_watermark,
    compress_media, format_compress,
    split_instagram_grid, format_grid,
    bulk_resize,
)
from .marketing import (
    mass_broadcast, format_broadcast,
    export_panel_links,
    warmup_account, format_warmup,
    update_bot_menu, format_menu_update,
    check_fragment_username, format_fragment,
    run_ab_test, format_ab_test,
    schedule_broadcast, format_schedule, list_scheduled_jobs,
    tick_scheduled_broadcasts,
    cleanup_inactive_users, format_cleanup,
)

__all__ = [
    # monitor
    "get_system_metrics", "format_sysmon",
    "tail_logs",
    "check_sessions_health", "format_sessions",
    "watchdog_status", "watchdog_enable", "watchdog_disable",
    # intelligence
    "check_seo_visibility", "format_seo",
    "scan_channel_trends", "format_trends",
    "build_stats_report",
    "calculate_roi",
    "detect_hostile_bots", "format_hostile",
    "analyze_archive",
    # media
    "generate_emoji_set", "format_emoji_gen",
    "convert_to_webm_sticker", "format_sticker",
    "apply_watermark",
    "compress_media", "format_compress",
    "split_instagram_grid", "format_grid",
    "bulk_resize",
    # marketing
    "mass_broadcast", "format_broadcast",
    "export_panel_links",
    "warmup_account", "format_warmup",
    "update_bot_menu", "format_menu_update",
    "check_fragment_username", "format_fragment",
    "run_ab_test", "format_ab_test",
    "schedule_broadcast", "format_schedule", "list_scheduled_jobs",
    "tick_scheduled_broadcasts",
    "cleanup_inactive_users", "format_cleanup",
]
