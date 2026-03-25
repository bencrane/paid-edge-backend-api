"""Meta ad creative and ad CRUD (BJC-155)."""

import logging

from pydantic import BaseModel

logger = logging.getLogger(__name__)

# --- CTA types ---

META_CTA_TYPES = [
    "APPLY_NOW", "BOOK_TRAVEL", "CALL_NOW", "CONTACT_US", "DOWNLOAD",
    "GET_OFFER", "GET_QUOTE", "LEARN_MORE", "LISTEN_MUSIC", "MESSAGE_PAGE",
    "NO_BUTTON", "OPEN_LINK", "ORDER_NOW", "SHOP_NOW", "SIGN_UP",
    "SUBSCRIBE", "WATCH_MORE", "WHATSAPP_MESSAGE",
]

# --- Ad format specs ---

IMAGE_SPECS = {
    "recommended": (1080, 1080),
    "landscape": (1200, 628),
    "min_width": 600,
    "max_file_size_mb": 30,
    "formats": ["jpg", "png"],
    "aspect_ratio_tolerance": 0.03,
}

VIDEO_SPECS = {
    "min_resolution": (1080, 1080),
    "max_duration_feed": 241 * 60,
    "max_duration_stories": 120,
    "max_file_size_gb": 4,
    "formats": ["mp4", "mov"],
}


# --- Pydantic models ---


class MetaCreative(BaseModel):
    id: str
    name: str = ""
    status: str = ""
    object_story_spec: dict = {}


class MetaAd(BaseModel):
    id: str
    name: str = ""
    adset_id: str = ""
    creative_id: str = ""
    status: str = ""
    effective_status: str = ""


# --- Creative + Ad CRUD methods ---


class MetaCreativesMixin:
    """Creative and Ad CRUD methods for MetaAdsClient."""

    async def create_image_ad_creative(
        self,
        name: str,
        page_id: str,
        image_hash: str,
        link: str,
        message: str,
        headline: str,
        description: str = "",
        cta_type: str = "LEARN_MORE",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/adcreatives — single image creative."""
        import json

        object_story_spec = {
            "page_id": page_id,
            "link_data": {
                "image_hash": image_hash,
                "link": link,
                "message": message,
                "name": headline,
                "description": description,
                "call_to_action": {"type": cta_type, "value": {"link": link}},
            },
        }
        return await self._request(
            "POST",
            f"{self.ad_account_id}/adcreatives",
            data={
                "name": name,
                "object_story_spec": json.dumps(object_story_spec),
            },
        )

    async def create_video_ad_creative(
        self,
        name: str,
        page_id: str,
        video_id: str,
        thumbnail_hash: str | None = None,
        title: str = "",
        message: str = "",
        cta_type: str = "SIGN_UP",
        cta_link: str = "",
    ) -> dict:
        """Video creative with object_story_spec.video_data."""
        import json

        video_data = {
            "video_id": video_id,
            "message": message,
            "title": title,
            "call_to_action": {"type": cta_type, "value": {"link": cta_link}},
        }
        if thumbnail_hash:
            video_data["image_hash"] = thumbnail_hash

        object_story_spec = {"page_id": page_id, "video_data": video_data}
        return await self._request(
            "POST",
            f"{self.ad_account_id}/adcreatives",
            data={
                "name": name,
                "object_story_spec": json.dumps(object_story_spec),
            },
        )

    async def create_carousel_creative(
        self,
        name: str,
        page_id: str,
        message: str,
        cards: list[dict],
        link: str,
    ) -> dict:
        """Carousel creative with 2-10 child_attachments."""
        import json

        child_attachments = []
        for card in cards:
            attachment = {
                "link": card.get("link", link),
                "image_hash": card.get("image_hash", ""),
                "name": card.get("name", ""),
                "description": card.get("description", ""),
                "call_to_action": {
                    "type": card.get("cta_type", "LEARN_MORE"),
                    "value": {"link": card.get("link", link)},
                },
            }
            child_attachments.append(attachment)

        object_story_spec = {
            "page_id": page_id,
            "link_data": {
                "message": message,
                "link": link,
                "child_attachments": child_attachments,
            },
        }
        return await self._request(
            "POST",
            f"{self.ad_account_id}/adcreatives",
            data={
                "name": name,
                "object_story_spec": json.dumps(object_story_spec),
            },
        )

    async def create_lead_ad_creative(
        self,
        name: str,
        page_id: str,
        message: str,
        form_id: str,
        image_hash: str | None = None,
        cta_type: str = "SIGN_UP",
    ) -> dict:
        """Lead ad creative — link must be 'https://fb.me/'."""
        import json

        link_data = {
            "message": message,
            "link": "https://fb.me/",
            "call_to_action": {
                "type": cta_type,
                "value": {"lead_gen_form_id": form_id, "link": "https://fb.me/"},
            },
        }
        if image_hash:
            link_data["image_hash"] = image_hash

        object_story_spec = {"page_id": page_id, "link_data": link_data}
        return await self._request(
            "POST",
            f"{self.ad_account_id}/adcreatives",
            data={
                "name": name,
                "object_story_spec": json.dumps(object_story_spec),
            },
        )

    # --- Ad CRUD ---

    async def create_ad(
        self,
        name: str,
        adset_id: str,
        creative_id: str,
        status: str = "PAUSED",
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/ads — links creative to ad set."""
        import json

        return await self._request(
            "POST",
            f"{self.ad_account_id}/ads",
            data={
                "name": name,
                "adset_id": adset_id,
                "creative": json.dumps({"creative_id": creative_id}),
                "status": status,
            },
        )

    async def get_ad(self, ad_id: str) -> dict:
        """GET /{AD_ID} with fields."""
        return await self._request(
            "GET",
            ad_id,
            params={
                "fields": "name,adset_id,creative,status,effective_status,created_time"
            },
        )

    async def update_ad(self, ad_id: str, **fields) -> dict:
        """POST /{AD_ID}. Common: status, name, creative."""
        return await self._request("POST", ad_id, data=fields)

    async def list_ads(
        self, adset_id: str | None = None, limit: int = 25
    ) -> list[dict]:
        """GET /act_{AD_ACCOUNT_ID}/ads or GET /{ADSET_ID}/ads"""
        path = f"{adset_id}/ads" if adset_id else f"{self.ad_account_id}/ads"
        params = {
            "fields": "name,adset_id,creative,status,effective_status,created_time",
            "limit": limit,
        }
        return await self._paginate(path, params=params, limit=limit)

    async def delete_ad(self, ad_id: str) -> None:
        """DELETE /{AD_ID}"""
        await self._request("DELETE", ad_id)
