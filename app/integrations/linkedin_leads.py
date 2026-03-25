"""LinkedIn Lead Gen Forms — processing and form templates (BJC-139)."""

import logging
from datetime import UTC, datetime

from app.integrations.linkedin import LinkedInAdsClient

logger = logging.getLogger(__name__)

# Standard field type to snake_case key mapping
_FIELD_TYPE_MAP: dict[str, str] = {
    "FIRST_NAME": "first_name",
    "LAST_NAME": "last_name",
    "EMAIL": "email",
    "PHONE_NUMBER": "phone_number",
    "COMPANY_NAME": "company_name",
    "JOB_TITLE": "job_title",
    "JOB_FUNCTION": "job_function",
    "SENIORITY": "seniority",
    "INDUSTRY": "industry",
    "COMPANY_SIZE": "company_size",
    "CITY": "city",
    "STATE": "state",
    "COUNTRY": "country",
    "WORK_PHONE": "work_phone",
    "GENDER": "gender",
}


class LinkedInLeadProcessor:
    """Processes LinkedIn lead gen form submissions into PaidEdge."""

    def __init__(self, client: LinkedInAdsClient, supabase):
        self.client = client
        self.supabase = supabase

    async def parse_lead_answers(
        self,
        answers: list[dict],
    ) -> dict:
        """Parse LinkedIn field answers into a flat dict.

        Returns: {first_name, last_name, email, company_name, job_title, ...}
        Custom questions are keyed by their customQuestionText (snake_cased).
        """
        result: dict[str, str] = {}
        for answer in answers:
            field_type = answer.get("fieldType", "")
            value = answer.get("value", "")
            if field_type == "CUSTOM":
                # Use custom question text as key
                question = answer.get("customQuestionText", "custom")
                key = question.lower().replace(" ", "_").replace("?", "")
                result[key] = value
            else:
                key = _FIELD_TYPE_MAP.get(field_type, field_type.lower())
                result[key] = value
        return result

    async def sync_leads(
        self,
        tenant_id: str,
        account_id: int,
        form_id: int,
        since: datetime | None = None,
    ) -> dict:
        """Pull new leads and process them.

        Steps:
        1. Get submissions since last sync (or since param)
        2. Parse answers into structured lead records
        3. Update last_synced_at timestamp
        4. Return: {leads_processed, new_leads, errors}
        """
        submitted_after = None
        if since:
            submitted_after = int(since.timestamp() * 1000)

        submissions = await self.client.get_lead_submissions(
            account_id=account_id,
            form_id=form_id,
            submitted_after=submitted_after,
        )

        leads_processed = 0
        new_leads = []
        errors: list[str] = []

        for submission in submissions:
            try:
                answers = submission.get("answers", [])
                parsed = await self.parse_lead_answers(answers)
                parsed["submitted_at"] = submission.get("submittedAt")
                parsed["associated_entity"] = submission.get(
                    "associatedEntity", ""
                )
                parsed["form_id"] = form_id
                new_leads.append(parsed)
                leads_processed += 1
            except Exception as e:
                errors.append(str(e))

        # Update last_synced_at
        now_ms = int(datetime.now(UTC).timestamp() * 1000)
        res = (
            self.supabase.table("provider_configs")
            .select("config")
            .eq("organization_id", tenant_id)
            .eq("provider", "linkedin_ads")
            .maybe_single()
            .execute()
        )
        if res.data:
            config = res.data["config"]
            lead_sync = config.get("lead_sync", {})
            lead_sync[str(form_id)] = {
                "last_synced_at": now_ms,
            }
            config["lead_sync"] = lead_sync
            self.supabase.table("provider_configs").update(
                {"config": config}
            ).eq("organization_id", tenant_id).eq(
                "provider", "linkedin_ads"
            ).execute()

        logger.info(
            "Lead sync for tenant %s form %d: %d processed, %d errors",
            tenant_id,
            form_id,
            leads_processed,
            len(errors),
        )

        return {
            "leads_processed": leads_processed,
            "new_leads": new_leads,
            "errors": errors,
        }


def build_lead_magnet_form(config: dict) -> dict:
    """Standard lead magnet download form: name, email, company, title."""
    return {
        "name": f"LM: {config['asset_title']}",
        "headline": config.get("headline", "Get Your Free Guide"),
        "description": config.get("description", "Download now"),
        "privacyPolicyUrl": config["privacy_policy_url"],
        "thankYouMessage": config.get(
            "thank_you_message",
            "Thanks! Check your email for the download link.",
        ),
        "thankYouLandingPageUrl": config.get(
            "thank_you_landing_page_url"
        ),
        "questions": [
            {"fieldType": "FIRST_NAME", "required": True},
            {"fieldType": "LAST_NAME", "required": True},
            {"fieldType": "EMAIL", "required": True},
            {"fieldType": "COMPANY_NAME", "required": True},
            {"fieldType": "JOB_TITLE", "required": False},
        ],
    }


def build_demo_request_form(config: dict) -> dict:
    """Demo request form: name, email, company, phone, custom question."""
    questions = [
        {"fieldType": "FIRST_NAME", "required": True},
        {"fieldType": "LAST_NAME", "required": True},
        {"fieldType": "EMAIL", "required": True},
        {"fieldType": "COMPANY_NAME", "required": True},
        {"fieldType": "PHONE_NUMBER", "required": False},
    ]
    if config.get("custom_question"):
        questions.append(
            {
                "fieldType": "CUSTOM",
                "customQuestionText": config["custom_question"],
                "required": False,
                "answerType": "FREE_TEXT",
            }
        )

    return {
        "name": f"Demo: {config.get('product_name', 'Product Demo')}",
        "headline": config.get(
            "headline", "Request a Demo"
        ),
        "description": config.get(
            "description",
            "See how we can help your business",
        ),
        "privacyPolicyUrl": config["privacy_policy_url"],
        "thankYouMessage": config.get(
            "thank_you_message",
            "Thanks! Our team will be in touch shortly.",
        ),
        "thankYouLandingPageUrl": config.get(
            "thank_you_landing_page_url"
        ),
        "questions": questions,
    }
