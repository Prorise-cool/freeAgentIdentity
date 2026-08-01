"""供 TeamAuto 消费账号的生产者 API。"""
from __future__ import annotations

import hmac
import os

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field

from infrastructure.producer_queue import QueueConflict, ack_lease, inventory, lease_accounts, release_lease


router = APIRouter(prefix="/producer", tags=["producer"])


class LeaseRequest(BaseModel):
    """领取账号的租约参数。"""

    consumer_id: str = Field(min_length=1, max_length=100)
    limit: int = Field(default=20, ge=1, le=100)
    lease_seconds: int = Field(default=300, ge=30, le=3600)


class LeaseItemsRequest(BaseModel):
    """确认一批租约条目。"""

    delivery_ids: list[int] = Field(min_length=1, max_length=100)


class ReleaseItemsRequest(LeaseItemsRequest):
    """释放一批失败条目并记录脱敏原因。"""

    reason: str = Field(default="", max_length=500)


def _require_producer_key(x_producer_key: str = Header(default="", alias="X-Producer-Key")) -> None:
    """使用独立服务密钥保护生产接口，不复用 Web 管理密码。"""
    expected = os.environ.get("PRODUCER_API_KEY", "").strip()
    if not expected:
        raise HTTPException(status_code=503, detail="生产 API 未配置")
    if not hmac.compare_digest(expected, str(x_producer_key or "")):
        raise HTTPException(status_code=401, detail="生产 API 密钥错误")


@router.get("/inventory", dependencies=[])
def producer_inventory(x_producer_key: str = Header(default="", alias="X-Producer-Key")) -> dict:
    """返回可领取、租赁中、已确认和阻塞账号数量。"""
    _require_producer_key(x_producer_key)
    return inventory()


@router.post("/leases")
def create_lease(body: LeaseRequest, x_producer_key: str = Header(default="", alias="X-Producer-Key")) -> dict:
    """领取一批账号并返回租约。"""
    _require_producer_key(x_producer_key)
    return lease_accounts(body.consumer_id, body.limit, body.lease_seconds)


@router.post("/leases/{lease_id}/ack")
def acknowledge_lease(
    lease_id: str,
    body: LeaseItemsRequest,
    x_producer_key: str = Header(default="", alias="X-Producer-Key"),
) -> dict:
    """确认下游已经成功持久化的条目。"""
    _require_producer_key(x_producer_key)
    try:
        return ack_lease(lease_id, body.delivery_ids)
    except QueueConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc


@router.post("/leases/{lease_id}/release")
def return_lease(
    lease_id: str,
    body: ReleaseItemsRequest,
    x_producer_key: str = Header(default="", alias="X-Producer-Key"),
) -> dict:
    """释放导入失败条目供下一轮重试。"""
    _require_producer_key(x_producer_key)
    try:
        return release_lease(lease_id, body.delivery_ids, body.reason)
    except QueueConflict as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
