import logging
from dataclasses import dataclass
from typing import Any

from fastapi import Depends, HTTPException, Query, status
from fastapi.responses import StreamingResponse

from xcore.sdk import TrustedBase, AutoDispatchMixin, RoutedPlugin, RouterRegistry, action
from xcore.kernel.api.rbac import get_current_user, require_role, AuthPayload, require_permission
from xcore.kernel.events import Event

from .client import RedisPubSubManager, StreamLimitExceeded, InvalidChannel, validate_channels, RedisConfiguration

logger = logging.getLogger("xpulse.plugin")

router = RouterRegistry()

# Channels internes réservés au système
SYSTEM_CHANNELS = {"system_notification", "broadcast"}



# ─────────────────────────────────────────────
# PLUGIN PRINCIPAL
# ─────────────────────────────────────────────

class Plugin(AutoDispatchMixin, RoutedPlugin, TrustedBase):

    # ── Lifecycle ─────────────────────────────────────────────────────────

    async def on_load(self) -> None:
        self.event = self.ctx.events
        self.redis_server: RedisPubSubManager | None = None
        health_cheker = self.ctx.health

        @health_cheker.register("xpulse.redis")
        async def redis_health_check():
            if not self.redis_server:
                return False, "Redis non configuré."
            return await self.redis_server.health_check(), "Redis répond." if await self.redis_server.health_check() else "Redis ne répond pas."
        try:
            self.redis_server = RedisPubSubManager(RedisConfiguration.from_dict(self.ctx.env))
            await self.redis_server.connect()
            logger.info("xpulse démarré — Redis prêt.")
        except Exception as exc:
            logger.error("xpulse : impossible d'initialiser Redis : %s", exc)
            logger.warning("xpulse démarré en mode dégradé (pas de Redis).")

        await self._register_event_handlers()

    async def on_unload(self) -> None:
        if self.redis_server:
            logger.info("xpulse : fermeture du pool Redis…")
            await self.redis_server.close()

    # ── Helpers internes ──────────────────────────────────────────────────

    def _require_redis(self) -> RedisPubSubManager:
        """Lève une 503 si Redis n'est pas disponible."""
        if not self.redis_server:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Service Redis indisponible.",
            )
        return self.redis_server

    def _parse_channels(self, raw: list[str]) -> list[str]:
        """
        Valide la liste de channels fournie par le client.
        Lève une 400 si invalide.
        """
        try:
            return validate_channels(raw)
        except InvalidChannel as exc:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc))

    # ── Event handlers ────────────────────────────────────────────────────

    async def _register_event_handlers(self) -> None:

        @self.event.on("ext.notification.publish")
        async def stream_response(event: Event):
            """
            Publie sur un ou plusieurs channels.
            Payload attendu : { "channels": ["chan1", "chan2"], ...data }
            ou               { "channel": "chan1", ...data }
            """
            if not self.redis_server:
                logger.warning("ext.notification.publish ignoré : Redis non disponible.")
                return ["error:redis_unavailable"]

            data: dict = dict(event.data)
            raw_channels = data.pop("channels", None) or [data.pop("channel", "notification")]

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning("ext.notification.publish : channels invalides : %s", exc)
                return [f"error:{exc}"]

            results = await self.redis_server.publish_many(channels, data)
            ok = [ch for ch, s in results.items() if s]
            fail = [ch for ch, s in results.items() if not s]
            if fail:
                logger.warning("Channels en échec : %s", fail)
            return [{"ok": ok, "failed": fail}]

        @self.event.on("ext.notification.broadcast")
        async def send_broadcast(event: Event):
            """
            Broadcast vers tous les users sur un ou plusieurs channels.
            Payload : { "channels": [...], "text": "..." }
            """
            if not self.redis_server:
                return

            data: dict = event.data
            raw_channels = data.get("channels", ["notification"])
            text = data.get("text", "")

            try:
                channels = validate_channels(
                    raw_channels if isinstance(raw_channels, list) else [raw_channels]
                )
            except InvalidChannel as exc:
                logger.warning("broadcast : channels invalides : %s", exc)
                return

            try:
                response = await self.event.emit("auth.get.user.ids", {})
                user_ids = list(response[0]) if response and response[0] else []
            except Exception as exc:
                logger.error("broadcast : impossible de récupérer les user IDs : %s", exc)
                return

            for uid in user_ids:
                await self.redis_server.publish_many(
                    channels,
                    {"user_id": uid, "text": text},
                )

    # ── Routes HTTP ───────────────────────────────────────────────────────

    @router.get("/stream/{user_id}", tags=["xpulse"])
    async def get_stream(
        self,
        current_user: AuthPayload = Depends(get_current_user),
        channels: list[str] = Query(
            default=["notification"],
            description=(
                "Un ou plusieurs channels à écouter. "
                "Ex: ?channels=notification&channels=alerts "
                "ou ?channels=notification,alerts"
            ),
        ),
    ):
        """
        SSE multi-channel : ouvre un stream pour un utilisateur sur N channels.

        Le client reçoit des events typés par channel :
            event: notification
            data: {"channel": "notification", "user_id": "...", "text": "..."}

        Usage JS :
            const src = new EventSource('/stream/user123?channels=notification&channels=alerts');
            src.addEventListener('notification', e => ...);
            src.addEventListener('alerts',       e => ...);
        """
        redis = self._require_redis()

        if not user_id or not current_user.sub.strip():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="user_id invalide.")

        # Support des channels séparés par virgule : ?channels=notification,alerts
        flat = []
        for c in channels:
            flat.extend(c.split(","))

        parsed_channels = self._parse_channels(flat)

        try:
            generator = redis.stream(channels=parsed_channels, user_id=user_id.strip())
        except StreamLimitExceeded as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc))

        return StreamingResponse(
            generator,
            media_type="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @router.post("/publish", tags=["xpulse"],dependencies=[Depends(require_role("xpulse:broadcast"))])
    async def publish(
        self,
        user_id: str,
        text: str,
        channels: list[str] = Query(
            default=["notification"],
            description="Channel(s) cible(s). Ex: ?channels=notification&channels=alerts",
        ),
    ):
        """Publie un message ciblé (user_id) sur un ou plusieurs channels."""
        redis = self._require_redis()

        if not user_id or not text:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="user_id et text sont requis.",
            )

        flat = []
        for c in channels:
            flat.extend(c.split(","))
        parsed_channels = self._parse_channels(flat)

        results = await redis.publish_many(
            parsed_channels,
            {"user_id": user_id, "text": text},
        )

        failed = [ch for ch, ok in results.items() if not ok]
        if failed:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail=f"Publish échoué sur : {failed}",
            )
        return {"status": "ok", "channels": parsed_channels}

    @router.post("/broadcast", tags=["xpulse"], dependencies=[Depends(require_permission("broadcast_notification"))])
    async def broadcast(
        self,
        text: str,
        channels: list[str] = Query(
            default=["notification"],
            description="Channel(s) cible(s) pour le broadcast.",
        ),
    ):
        """Envoie un message à tous les utilisateurs sur un ou plusieurs channels."""
        redis = self._require_redis()

        if not text:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="text est requis.")

        flat = []
        for c in channels:
            flat.extend(c.split(","))
        parsed_channels = self._parse_channels(flat)

        try:
            response = await self.event.emit("auth.get.user.ids", {})
            user_ids = list(response[0]) if response and response[0] else []
        except Exception as exc:
            logger.error("broadcast HTTP : impossible de récupérer les user IDs : %s", exc)
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Erreur lors de la récupération des utilisateurs.",
            )

        if not user_ids:
            return {"status": "ok", "sent": 0, "channels": parsed_channels}

        total_errors = 0
        for uid in user_ids:
            results = await redis.publish_many(parsed_channels, {"user_id": uid, "text": text})
            total_errors += sum(1 for ok in results.values() if not ok)

        logger.info(
            "Broadcast : %d users × %d channels, %d erreurs.",
            len(user_ids), len(parsed_channels), total_errors,
        )
        return {
            "status": "ok",
            "sent": len(user_ids),
            "channels": parsed_channels,
            "errors": total_errors,
        }

    @router.get("/health", tags=["xpulse"])
    async def health(self):
        """Vérifie que Redis est accessible et retourne les métriques courantes."""
        if not self.redis_server:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis non configuré.",
            )
        alive = await self.redis_server.health_check()
        if not alive:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Redis ne répond pas.",
            )
        return {
            "status": "ok",
            "active_streams": self.redis_server.active_streams,
        }

    # ── Action interne ────────────────────────────────────────────────────

    @action("xpulse.stream")
    async def publish_message(self, event: dict):
        """
        Action interne : publie sur un ou plusieurs channels.
        Payload : { "channels": ["chan1"], "event": {...} }
        ou       { "channel": "chan1",   "event": {...} }
        """
        if not self.redis_server:
            logger.warning("xpulse.stream : Redis non disponible, message ignoré.")
            return

        raw_channels = event.get("channels") or [event.get("channel", "system_notification")]
        payload      = event.get("event", {"user": "default", "text": "Message par défaut"})

        try:
            channels = validate_channels(
                raw_channels if isinstance(raw_channels, list) else [raw_channels]
            )
        except InvalidChannel as exc:
            logger.warning("xpulse.stream : channels invalides ignorés : %s", exc)
            return

        await self.redis_server.publish_many(channels, payload)

    # ── Router ────────────────────────────────────────────────────────────

    def get_router(self) -> Any | None:
        return self.RouterIn()