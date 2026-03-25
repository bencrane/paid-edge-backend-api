"""LinkedIn Conversions API (CAPI) bridge — maps PaidEdge events to LinkedIn (BJC-138)."""

import logging

from app.integrations.linkedin import LinkedInAdsClient

logger = logging.getLogger(__name__)


class LinkedInConversionBridge:
    """Sends PaidEdge behavioral events and CRM events to LinkedIn CAPI."""

    PAIDEDGE_TO_LINKEDIN_TYPE: dict[str, str] = {
        "form_submitted": "LEAD",
        "content_downloaded": "DOWNLOAD",
        "demo_requested": "LEAD",
        "meeting_booked": "LEAD",
        "opportunity_created": "OTHER",
        "closed_won": "PURCHASE",
    }

    def __init__(self, client: LinkedInAdsClient, supabase):
        self.client = client
        self.supabase = supabase

    async def setup_conversion_rules(
        self,
        tenant_id: str,
        account_id: int,
    ) -> dict[str, str]:
        """Create standard conversion rules on LinkedIn for all PaidEdge event types.

        Returns mapping: {paidedge_event_type: linkedin_conversion_urn}
        Stores mapping in provider_configs.config.conversion_rules.
        """
        mapping: dict[str, str] = {}
        for event_type, linkedin_type in self.PAIDEDGE_TO_LINKEDIN_TYPE.items():
            rule_name = f"PaidEdge: {event_type}"
            result = await self.client.create_conversion_rule(
                account_id=account_id,
                name=rule_name,
                conversion_type=linkedin_type,
            )
            conversion_urn = result.get(
                "id", result.get("urn", "")
            )
            mapping[event_type] = conversion_urn

        # Store mapping in provider_configs
        res = (
            self.supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", tenant_id)
            .eq("provider", "linkedin_ads")
            .maybe_single()
            .execute()
        )
        config = res.data["config"] if res.data else {}
        config["conversion_rules"] = mapping
        self.supabase.table("provider_configs").update(
            {"config": config}
        ).eq("organization_id", tenant_id).eq(
            "provider", "linkedin_ads"
        ).execute()

        logger.info(
            "Created %d conversion rules for tenant %s",
            len(mapping),
            tenant_id,
        )
        return mapping

    async def send_paidedge_event(
        self,
        tenant_id: str,
        event_type: str,
        email: str,
        event_id: str,
        value_usd: str | None = None,
        user_info: dict | None = None,
    ) -> None:
        """Map PaidEdge event to LinkedIn conversion and send.

        Looks up conversion_urn from stored mapping in provider_configs.
        """
        res = (
            self.supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", tenant_id)
            .eq("provider", "linkedin_ads")
            .maybe_single()
            .execute()
        )
        if not res.data:
            logger.warning(
                "No LinkedIn config for tenant %s, skipping CAPI event",
                tenant_id,
            )
            return

        config = res.data["config"]
        rules = config.get("conversion_rules", {})
        conversion_urn = rules.get(event_type)
        if not conversion_urn:
            logger.warning(
                "No conversion rule for event type %s (tenant %s)",
                event_type,
                tenant_id,
            )
            return

        await self.client.send_conversion_event(
            conversion_urn=conversion_urn,
            email=email,
            event_id=event_id,
            value_usd=value_usd,
            user_info=user_info,
        )
        logger.info(
            "Sent CAPI event: tenant=%s type=%s event_id=%s",
            tenant_id,
            event_type,
            event_id,
        )
