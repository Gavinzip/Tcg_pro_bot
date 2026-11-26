"""Optional SBT data for the Discord Profile wizard.

An unavailable source is distinct from a successful empty balance. The wizard
passes this snapshot to generation so a failed lookup is not immediately retried.
"""
from dataclasses import dataclass
import logging
from typing import Callable


_LOG = logging.getLogger(__name__)
_SOURCE_ERRORS = frozenset({
    "sbt_onchain_lookup_failed", "sbt_onchain_invalid_response",
    "sbt_onchain_module_unavailable", "sbt_source_unavailable",
    "sbt_official_lookup_failed", "sbt_official_username_unavailable",
})


@dataclass(frozen=True)
class ProfileSbtSnapshot:
    wallet: str
    badges: tuple[dict, ...] | None
    error_code: str | None = None

    @property
    def available(self) -> bool:
        return self.badges is not None

    @property
    def owned_badges(self) -> list[dict]:
        return [dict(b) for b in (self.badges or ()) if b.get("is_owned") and (b.get("balance") or 0) > 0]

    @property
    def total(self) -> int | None:
        return sum(int(b.get("balance") or 0) for b in self.badges) if self.available else None

    def picker_fields(self) -> dict:
        owned = self.owned_badges
        options = []
        for badge in owned[:25]:
            name = str(badge.get("name") or "").strip()
            if not name:
                continue
            sbt_id = str(badge.get("sbt_id") or "").strip()
            options.append({
                "value": (f"id:{sbt_id}" if sbt_id else f"name:{name.lower()}")[:100],
                "label": name[:100],
                "full_label": name[:160],
                "description": f"Balance {int(badge.get('balance') or 0)}"[:100],
            })
        return {
            "sbt_available": self.available,
            "sbt_error": self.error_code,
            "sbt_snapshot": self,
            "owned_sbt_count": len(owned) if self.available else None,
            "sbt_options": options,
        }


def fetch_profile_sbt_snapshot(
    fetch_badges: Callable[[str | None, str], list[dict]],
    username: str | None,
    wallet: str,
) -> ProfileSbtSnapshot:
    try:
        badges = fetch_badges(username, wallet)
    except RuntimeError as exc:
        code = str(exc)
        if code not in _SOURCE_ERRORS:
            raise
        # Provider exceptions can contain authenticated URLs. Log the wallet,
        # safe error code and cause type, never raw exception text or credentials.
        _LOG.warning("profile_sbt_unavailable wallet=%s code=%s cause_type=%s",
                     wallet, code, type(exc.__cause__ or exc).__name__)
        return ProfileSbtSnapshot(wallet=wallet, badges=None, error_code=code)
    return ProfileSbtSnapshot(wallet=wallet, badges=tuple(dict(b) for b in badges))


def sbt_unavailable_text(lang: str, *, short: bool = False) -> str:
    short_text, notice = {
        "zh": ("暫時無法取得", "⚠️ SBT 來源暫時無法查詢；本次海報的 SBT 數量顯示為未知，徽章暫不顯示。其他資料可繼續生成，稍後可重新執行 /profile 查詢 SBT。"),
        "zhs": ("暂时无法获取", "⚠️ SBT 来源暂时无法查询；本次海报的 SBT 数量显示为未知，徽章暂不显示。其他数据可继续生成，稍后可重新执行 /profile 查询 SBT。"),
        "ko": ("일시적으로 조회 불가", "⚠️ SBT 조회를 사용할 수 없습니다. 이번 포스터에는 SBT 수량을 알 수 없음으로 표시하고 배지는 생략합니다. 다른 데이터로 계속 생성할 수 있습니다. 나중에 /profile을 다시 실행해 주세요."),
        "en": ("Temporarily unavailable", "⚠️ The SBT source is unavailable. SBT counts are unknown and badges are omitted for this poster. Other data can still be generated. Run /profile again later to retry SBT."),
    }.get(lang, ("Temporarily unavailable", "⚠️ SBT is unavailable; counts are unknown and badges are omitted. Run /profile again later to retry."))
    return short_text if short else notice
