"""Meta media upload (BJC-155)."""

import asyncio
import base64
import logging

logger = logging.getLogger(__name__)


class MetaMediaMixin:
    """Media upload methods for MetaAdsClient."""

    async def upload_image(
        self,
        image_bytes: bytes | None = None,
        image_path: str | None = None,
    ) -> dict:
        """POST /act_{AD_ACCOUNT_ID}/adimages

        Upload via base64 bytes or file path.
        Returns: {hash: str, url: str}
        """
        data = {}
        if image_bytes:
            data["bytes"] = base64.b64encode(image_bytes).decode("utf-8")
        elif image_path:
            with open(image_path, "rb") as f:
                data["bytes"] = base64.b64encode(f.read()).decode("utf-8")

        resp = await self._request(
            "POST", f"{self.ad_account_id}/adimages", data=data
        )

        # Response: {"images": {"bytes": {"hash": "...", "url": "..."}}}
        images = resp.get("images", {})
        img_data = next(iter(images.values()), {})
        return {
            "hash": img_data.get("hash", ""),
            "url": img_data.get("url", ""),
        }

    async def upload_video(
        self,
        video_path: str,
        title: str = "",
    ) -> dict:
        """Upload video. Auto-selects simple vs chunked based on file size.

        Returns: {video_id: str, title: str}
        """
        import os

        file_size = os.path.getsize(video_path)
        if file_size >= 1_073_741_824:  # 1GB
            return await self._chunked_video_upload(video_path, title)

        with open(video_path, "rb") as f:
            video_data = f.read()

        data = {"title": title or "PaidEdge Video"}
        resp = await self._request(
            "POST",
            f"{self.ad_account_id}/advideos",
            data={**data, "source": base64.b64encode(video_data).decode("utf-8")},
        )
        return {"video_id": resp.get("id", ""), "title": title}

    async def _chunked_video_upload(
        self, video_path: str, title: str
    ) -> dict:
        """Three-step chunked upload for large videos.

        1. POST upload_phase=start → get upload_session_id, video_id
        2. POST upload_phase=transfer → send chunks
        3. POST upload_phase=finish → finalize
        """
        import os

        file_size = os.path.getsize(video_path)

        # Step 1: Start
        start_resp = await self._request(
            "POST",
            f"{self.ad_account_id}/advideos",
            data={
                "upload_phase": "start",
                "file_size": file_size,
            },
        )
        upload_session_id = start_resp.get("upload_session_id", "")
        video_id = start_resp.get("video_id", "")

        # Step 2: Transfer chunks
        chunk_size = 4 * 1024 * 1024  # 4MB chunks
        with open(video_path, "rb") as f:
            start_offset = 0
            while start_offset < file_size:
                chunk = f.read(chunk_size)
                transfer_resp = await self._request(
                    "POST",
                    f"{self.ad_account_id}/advideos",
                    data={
                        "upload_phase": "transfer",
                        "upload_session_id": upload_session_id,
                        "start_offset": start_offset,
                        "video_file_chunk": base64.b64encode(chunk).decode("utf-8"),
                    },
                )
                start_offset = int(
                    transfer_resp.get("start_offset", start_offset + len(chunk))
                )

        # Step 3: Finish
        await self._request(
            "POST",
            f"{self.ad_account_id}/advideos",
            data={
                "upload_phase": "finish",
                "upload_session_id": upload_session_id,
                "title": title or "PaidEdge Video",
            },
        )

        return {"video_id": video_id, "title": title}

    async def wait_for_video_ready(
        self,
        video_id: str,
        max_wait_seconds: int = 300,
        poll_interval: int = 10,
    ) -> dict:
        """Poll until video processing complete."""
        elapsed = 0
        while elapsed < max_wait_seconds:
            resp = await self._request(
                "GET", video_id, params={"fields": "status"}
            )
            status = resp.get("status", {})
            video_status = status.get("video_status", "")
            if video_status == "ready":
                return resp
            if video_status == "error":
                raise Exception(f"Video processing failed: {status}")
            await asyncio.sleep(poll_interval)
            elapsed += poll_interval

        raise Exception(f"Video {video_id} not ready after {max_wait_seconds}s")
