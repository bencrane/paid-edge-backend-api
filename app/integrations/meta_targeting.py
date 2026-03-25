"""Meta Ad Set targeting builder (BJC-153)."""

import logging

logger = logging.getLogger(__name__)


class MetaTargetingBuilder:
    """Construct Meta targeting spec from PaidEdge audience parameters.

    Targeting spec structure supports geo_locations, demographics,
    flexible_spec (interests/behaviors with OR/AND logic), custom audiences,
    placements, and targeting expansion.
    """

    def __init__(self):
        self._spec: dict = {}

    def set_locations(
        self,
        countries: list[str] | None = None,
        regions: list[dict] | None = None,
        cities: list[dict] | None = None,
    ) -> "MetaTargetingBuilder":
        """Set geographic targeting."""
        geo = {}
        if countries:
            geo["countries"] = countries
        if regions:
            geo["regions"] = regions
        if cities:
            geo["cities"] = cities
        if geo:
            self._spec["geo_locations"] = geo
        return self

    def set_demographics(
        self,
        age_min: int = 18,
        age_max: int = 65,
        genders: list[int] | None = None,
    ) -> "MetaTargetingBuilder":
        """Set age and gender targeting.

        genders: 0=all, 1=male, 2=female
        """
        self._spec["age_min"] = age_min
        self._spec["age_max"] = age_max
        if genders:
            self._spec["genders"] = genders
        return self

    def add_interests(self, interests: list[dict]) -> "MetaTargetingBuilder":
        """Add interests to flexible_spec. OR within group, AND between groups."""
        if not interests:
            return self
        if "flexible_spec" not in self._spec:
            self._spec["flexible_spec"] = []
        self._spec["flexible_spec"].append({"interests": interests})
        return self

    def add_behaviors(self, behaviors: list[dict]) -> "MetaTargetingBuilder":
        """Add behaviors to flexible_spec."""
        if not behaviors:
            return self
        if "flexible_spec" not in self._spec:
            self._spec["flexible_spec"] = []
        self._spec["flexible_spec"].append({"behaviors": behaviors})
        return self

    def set_custom_audiences(
        self,
        audience_ids: list[str],
        excluded_ids: list[str] | None = None,
    ) -> "MetaTargetingBuilder":
        """Set custom audience inclusion/exclusion."""
        if audience_ids:
            self._spec["custom_audiences"] = [{"id": aid} for aid in audience_ids]
        if excluded_ids:
            self._spec["excluded_custom_audiences"] = [
                {"id": aid} for aid in excluded_ids
            ]
        return self

    def set_placements(
        self,
        platforms: list[str] | None = None,
        positions: dict | None = None,
    ) -> "MetaTargetingBuilder":
        """Set specific placements, or omit entirely for Advantage+ (auto-optimized)."""
        if platforms:
            self._spec["publisher_platforms"] = platforms
        if positions:
            for key, values in positions.items():
                self._spec[key] = values
        return self

    def enable_targeting_expansion(
        self, enabled: bool = True
    ) -> "MetaTargetingBuilder":
        """Advantage Detailed Targeting — lets Meta expand beyond specified interests."""
        self._spec["targeting_expansion"] = {"expansion": enabled}
        return self

    def set_exclusions(self, interests: list[dict] | None = None) -> "MetaTargetingBuilder":
        """Set interest exclusions."""
        if interests:
            self._spec["exclusions"] = {"interests": interests}
        return self

    def set_locales(self, locale_ids: list[int]) -> "MetaTargetingBuilder":
        """Set language targeting."""
        if locale_ids:
            self._spec["locales"] = locale_ids
        return self

    def build(self) -> dict:
        """Return the complete targeting spec dict."""
        return dict(self._spec)


def enforce_special_ad_category_restrictions(
    targeting: dict, categories: list[str]
) -> dict:
    """If campaign has HOUSING/CREDIT/EMPLOYMENT, enforce restrictions.

    No age, gender, zip, or interest exclusions allowed.
    """
    if not categories:
        return targeting

    restricted = any(
        c in categories for c in ["HOUSING", "CREDIT", "EMPLOYMENT"]
    )
    if not restricted:
        return targeting

    targeting.pop("age_min", None)
    targeting.pop("age_max", None)
    targeting.pop("genders", None)
    targeting.pop("exclusions", None)
    geo = targeting.get("geo_locations", {})
    geo.pop("zips", None)

    return targeting


def build_schedule(
    start_time: str,
    end_time: str | None = None,
    dayparting: list[dict] | None = None,
) -> dict:
    """Build ad set schedule config."""
    schedule = {"start_time": start_time}
    if end_time:
        schedule["end_time"] = end_time
    if dayparting:
        schedule["pacing_type"] = ["day_parting"]
        schedule["adset_schedule"] = dayparting
    return schedule
