"""Replay the Profile wizard's SBT outage without starting Discord or chain jobs."""
import ast
import asyncio
from datetime import datetime
from decimal import Decimal, InvalidOperation
import html
import os
from pathlib import Path
import sys
import threading
import time
from types import SimpleNamespace
import unittest
from unittest.mock import AsyncMock, Mock, patch

from runtime.profile_sbt import ProfileSbtSnapshot, fetch_profile_sbt_snapshot, sbt_unavailable_text


CORE_PATH = Path(__file__).resolve().parents[1] / "runtime" / "core.py"
WALLET = "0x" + "1" * 40
PROVIDER_ERROR = (
    "bscscan token1155tx request failed: bscscan token1155tx error: "
    "Unexpected error, timeout or server too busy. Please try again later"
)


def load_core_definitions(names, namespace):
    # Execute the actual legacy functions, excluding module-level bot, HTTP
    # server and scheduled-job initialization. Only external I/O is replaced.
    tree = ast.parse(CORE_PATH.read_text())
    nodes = [node for node in tree.body if getattr(node, "name", None) in names]
    assert len(nodes) == len(names), names
    exec(compile(ast.Module(body=nodes, type_ignores=[]), str(CORE_PATH), "exec"), namespace)
    return namespace


class ProfileSbtAvailabilityTests(unittest.TestCase):
    def setUp(self):
        self.analyze = Mock(side_effect=RuntimeError(PROVIDER_ERROR))
        self.collection = [{"name": "A collectible", "fmvPriceInUSD": 12300, "setName": "Gym"}]
        self.ns = load_core_definitions(
            {"_fetch_wallet_sbt_badges_onchain", "_fetch_user_sbt_badges",
             "_build_wallet_profile_picker_data_unlocked"},
            {
                "time": time,
                "PROFILE_DATA_MODE": "chain_official",
                "PROFILE_SBT_BADGE_SOURCE": "onchain",
                "_PROFILE_SBT_BADGE_CACHE": {},
                "PROFILE_SBT_BADGE_CACHE_TTL_SEC": 600,
                "ONCHAIN_SBT_CONTRACT": "0x" + "2" * 40,
                "_ONCHAIN_ANALYZE_SBT_WALLET_FN": self.analyze,
                "_build_profile_onchain_cfg": Mock(return_value=object()),
                "_normalize_wallet_address": lambda value: value.lower(),
                "_username_from_rankings_wallet": lambda wallet: "Suen",
                "_fetch_wallet_collection_chain": Mock(return_value=self.collection),
                "_fetch_sbt_metadata_map_onchain": Mock(return_value={"7": {"name": "Badge"}}),
                "_parse_int": lambda value: int(value) if value is not None else None,
                "_format_fmv_usd": lambda value: f"${value / 100:.2f}",
                "ProfileSbtSnapshot": ProfileSbtSnapshot,
                "fetch_profile_sbt_snapshot": fetch_profile_sbt_snapshot,
                "sbt_unavailable_text": sbt_unavailable_text,
            },
        )

    def test_provider_outage_keeps_picker_usable_and_count_unknown(self):
        result = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        self.assertEqual(result["collection_count"], 1)
        self.assertEqual(len(result["card_options"]), 1)
        self.assertFalse(result["sbt_available"])
        self.assertIsNone(result["owned_sbt_count"])
        self.assertEqual(result["sbt_error"], "sbt_onchain_lookup_failed")
        self.analyze.assert_called_once()

    def test_outage_with_no_collectibles_is_still_unknown(self):
        self.ns["_fetch_wallet_collection_chain"].return_value = []
        result = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        self.assertFalse(result["sbt_available"])
        self.assertIsNone(result["owned_sbt_count"])
        self.assertEqual(result["card_options"], [])

    def test_healthy_badges_keep_actual_balances_and_options(self):
        self.analyze.side_effect = None
        self.analyze.return_value = {"7": 3}
        result = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        self.assertEqual(result["owned_sbt_count"], 1)
        self.assertEqual(result["sbt_options"][0]["description"], "Balance 3")

    def test_verified_empty_balances_are_zero(self):
        self.analyze.side_effect = None
        self.analyze.return_value = {}
        result = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        self.assertTrue(result["sbt_available"])
        self.assertEqual(result["owned_sbt_count"], 0)
        self.assertEqual(result["sbt_snapshot"].total, 0)

    def test_unrelated_errors_are_not_hidden(self):
        self.ns["_fetch_user_sbt_badges"] = Mock(side_effect=RuntimeError("programming_error"))
        with self.assertRaisesRegex(RuntimeError, "programming_error"):
            self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)

    def load_generation(self):
        self.ns.update({
            "Decimal": Decimal, "InvalidOperation": InvalidOperation,
            "datetime": datetime, "threading": threading, "html_lib": html, "sys": sys,
            "_PROFILE_POSTER_TYPE_ORDER": ("history", "collection", "extremes"),
            "LEGACY_LEADERBOARDS_ENABLED": False,
            "PROFILE_REALTIME_RECALC_ON_PROFILE": False,
            "PROFILE_ENABLE_HOLDINGS_POSTER": False,
            "PROFILE_ENABLE_SBT_RANK_POSTER": True,
            "_TRANSPARENT_CARD_IMAGE": "data:image/png;base64,",
            "_normalize_profile_background_key": lambda key: "classic",
            "_profile_background_data_uri": Mock(return_value=""),
            "_profile_sbt_rank_background_data_uri": Mock(return_value=""),
            "_load_rankings_wallet_map": Mock(return_value={}),
            "_prepare_sbt_badge_image_for_poster": Mock(return_value=""),
            "_build_wallet_activity_history_for_profile": Mock(return_value={
                "opened_packs_count": 10, "pack_spent_total": Decimal("2480"),
                "total_spent": Decimal("2480"), "total_earned": Decimal("3000"),
                "net_total": Decimal("520"),
            }),
        })
        load_core_definitions({
            "_build_wallet_profile_context", "_profile_lang_from_locale", "_profile_ui_labels",
            "_profile_history_labels", "_normalize_profile_poster_types", "_clamp_profile_card_count",
            "_select_profile_items", "_rank_chip_payload", "_compact_sbt_label", "_t",
            "_format_number", "_format_fmv_usd", "_format_fmv_display", "_format_usd",
            "_to_decimal", "_format_usdt_decimal", "_format_usdt_currency", "_short_hex",
            "_profile_top_value_label", "_build_wallet_features_html",
        }, self.ns)
        return self.ns["_build_wallet_profile_context"]

    def test_generation_reuses_failed_snapshot_without_another_chain_lookup(self):
        picker = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        generate = self.load_generation()
        result = generate(WALLET, poster_types=["collection", "history"], sbt_snapshot=picker["sbt_snapshot"])
        self.analyze.assert_called_once()
        self.assertFalse(result["sbt_available"])
        self.assertTrue(result["profile_poster_enabled"])
        self.assertEqual(len(result["template_context"]["items"]), 1)
        self.assertEqual(result["template_context"]["sbt_total"], "—")
        self.assertEqual(result["history_template_context"]["metric_sbt_value"], "—")
        self.assertEqual(result["history_summary"]["opened_packs"], 10)
        self.assertEqual(result["history_summary"]["total_spent"], Decimal("2480"))
        self.assertEqual(result["history_summary"]["net_total"], Decimal("643"))

    def test_direct_generation_outage_is_also_nonfatal(self):
        result = self.load_generation()(WALLET, poster_types=["history"])
        self.assertFalse(result["sbt_available"])
        self.assertEqual(result["history_template_context"]["metric_sbt_value"], "—")
        self.analyze.assert_called_once()

    def test_generation_preserves_healthy_sbt_counts(self):
        self.analyze.side_effect = None
        self.analyze.return_value = {"7": 3}
        picker = self.ns["_build_wallet_profile_picker_data_unlocked"](WALLET)
        result = self.load_generation()(WALLET, poster_types=["collection", "history"], sbt_snapshot=picker["sbt_snapshot"])
        self.assertTrue(result["sbt_available"])
        self.assertEqual(result["template_context"]["sbt_total"], 3)
        self.assertEqual(result["history_template_context"]["metric_sbt_value"], "3")
        self.assertEqual(result["template_context"]["sbt_badges_display"][0]["balance"], 3)
        self.analyze.assert_called_once()

    def test_generation_rejects_a_different_wallet_snapshot(self):
        generate = self.load_generation()
        snapshot = ProfileSbtSnapshot(wallet="0x" + "3" * 40, badges=())
        with self.assertRaisesRegex(ValueError, "profile_sbt_snapshot_wallet_mismatch"):
            generate(WALLET, sbt_snapshot=snapshot)
        self.analyze.assert_not_called()
        self.ns["_build_wallet_activity_history_for_profile"].assert_not_called()

    def test_required_collection_or_history_failure_is_not_converted_to_zero(self):
        generate = self.load_generation()
        self.ns["_fetch_wallet_collection_chain"].side_effect = RuntimeError("collection_source_unavailable")
        with self.assertRaisesRegex(RuntimeError, "collection_source_unavailable"):
            generate(WALLET)
        self.ns["_fetch_wallet_collection_chain"].side_effect = None
        self.ns["_build_wallet_activity_history_for_profile"].side_effect = RuntimeError("history_source_unavailable")
        with self.assertRaisesRegex(RuntimeError, "profile_chain_history_failed"):
            generate(WALLET)

    def test_discord_command_panel_and_generate_button_continue_after_sbt_outage(self):
        import discord
        from commands.profile_flexpack import handle_profile

        async def exercise():
            self.load_generation()
            lease = Mock()
            renderer = AsyncMock(return_value={"history": "test-history.png"})
            self.ns.update({
                "discord": discord, "asyncio": asyncio,
                "LANG_FLAGS": {}, "LANG_LABELS": {},
                "os": SimpleNamespace(path=SimpleNamespace(exists=lambda path: True)),
                "tempfile": SimpleNamespace(mkdtemp=Mock(return_value="test-output-not-created")),
                "shutil": SimpleNamespace(rmtree=Mock()),
                "acquire_profile_job_lock_async": AsyncMock(return_value=lease),
                "POSTER_SEMAPHORE": asyncio.Semaphore(1),
                "_render_wallet_profile_posters_bundle": renderer,
            })
            load_core_definitions({
                "_profile_wizard_texts", "ProfileConfigView", "ProfileTemplateSelect",
                "ProfilePosterSelect", "ProfileSBTSelect", "ProfileCardSelect",
            }, self.ns)
            thread = SimpleNamespace(send=AsyncMock(return_value=Mock()), add_user=AsyncMock())
            interaction = SimpleNamespace(
                id=123, user=SimpleNamespace(id=456),
                response=SimpleNamespace(send_message=AsyncMock(), edit_message=AsyncMock()),
                original_response=AsyncMock(return_value=SimpleNamespace(create_thread=AsyncMock(return_value=thread))),
                followup=SimpleNamespace(send=AsyncMock()),
            )
            language_view = SimpleNamespace(wait_for_choice=AsyncMock(return_value=("zhs", True)))
            await handle_profile(interaction, WALLET, deps={
                "get_user_default_wallet": Mock(),
                "normalize_wallet_address": self.ns["_normalize_wallet_address"],
                "profile_wizard_texts": self.ns["_profile_wizard_texts"],
                "LanguageSelectView": Mock(return_value=language_view),
                "build_wallet_profile_picker_data": self.ns["_build_wallet_profile_picker_data_unlocked"],
                "ProfileConfigView": self.ns["ProfileConfigView"],
            })
            panel = thread.send.await_args_list[-1]
            view = panel.kwargs["view"]
            self.assertIn("暂时无法获取", panel.args[0])
            self.assertNotIn("可选 SBT: **0**", panel.args[0])
            self.assertNotIn("sbt_onchain_lookup_failed", panel.args[0])
            self.assertTrue(next(child for child in view.children if child.row == 2).disabled)
            self.assertFalse(view.generate.disabled)
            for language in ("zh", "zhs", "ko", "en"):
                localized = self.ns["ProfileConfigView"](456, WALLET, view.picker_data, language)
                self.assertIn(sbt_unavailable_text(language, short=True), localized.render_message())
                localized.stop()
            with patch.object(discord, "File", return_value=object()):
                await view.generate.callback(interaction)
            summary = interaction.followup.send.await_args.args[0]
            self.assertIn("SBT 数量显示为未知", summary)
            self.assertIn("✅", summary)
            self.assertNotIn("❌", summary)
            self.assertEqual(len(interaction.followup.send.await_args.kwargs["files"]), 1)
            self.assertFalse(renderer.await_args.args[0]["sbt_available"])
            self.analyze.assert_called_once()
            lease.release.assert_called_once()

        asyncio.run(exercise())

    def test_render_bundle_omits_unavailable_sbt_rank_poster(self):
        async def exercise():
            page = SimpleNamespace(set_content=AsyncMock(), close=AsyncMock())
            context = SimpleNamespace(new_page=AsyncMock(return_value=page), close=AsyncMock())
            render_html = Mock(return_value="<html></html>")
            ns = {
                "os": SimpleNamespace(makedirs=Mock(), path=SimpleNamespace(join=os.path.join, exists=lambda path: True)),
                "re": __import__("re"),
                "PROFILE_ENABLE_SBT_RANK_POSTER": True, "PROFILE_POSTER_DEVICE_SCALE": 1,
                "PROFILE_TEMPLATE_PATH": "collection-template", "PROFILE_HISTORY_TEMPLATE_PATH": "history-template",
                "PROFILE_EXTREMES_TEMPLATE_PATH": "extremes-template", "PROFILE_SBT_RANK_TEMPLATE_PATH": "sbt-template",
                "_render_wallet_template_html": render_html,
                "image_generator": SimpleNamespace(RENDER_SEMAPHORE=asyncio.Semaphore(1),
                    _new_browser_context=AsyncMock(return_value=context), _screenshot_poster_root=AsyncMock()),
            }
            load_core_definitions({"_render_wallet_profile_posters_bundle"}, ns)
            result = await ns["_render_wallet_profile_posters_bundle"](
                {"sbt_available": False}, "test-output-not-created", render_extremes=False,
            )
            self.assertIsNone(result["sbt_rank"])
            self.assertEqual([call.args[0] for call in render_html.call_args_list], ["collection-template", "history-template"])
            self.assertEqual(context.new_page.await_count, 2)
            context.close.assert_awaited_once()

        asyncio.run(exercise())


if __name__ == "__main__":
    unittest.main()
