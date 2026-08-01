"""生产者租约 API 回归测试。"""
from __future__ import annotations

from core.base_platform import Account
from core.db import save_account
from infrastructure.producer_queue import enqueue_account, inventory


def _headers() -> dict[str, str]:
    """返回测试生产 API 鉴权头。"""
    return {"X-Producer-Key": "producer-test-key"}


def _create_account(*, email: str = "producer@test.com", access_token: str = "access-token") -> int:
    """创建并入队一个 ChatGPT 测试账号。"""
    model = save_account(
        Account(
            platform="chatgpt",
            email=email,
            password="Pass123!",
            user_id="acct-producer",
            extra={
                "access_token": access_token,
                "refresh_token": "refresh-token",
                "session_token": "session-token",
                "cookies": {"__Secure-next-auth.session-token": "session-token"},
            },
        )
    )
    enqueue_account(int(model.id))
    return int(model.id)


def test_producer_api_rejects_wrong_key(client, monkeypatch):
    """错误服务密钥不能查看或领取库存。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    response = client.get("/api/producer/inventory", headers={"X-Producer-Key": "wrong"})
    assert response.status_code == 401


def test_lease_ack_flow_is_exclusive(client, monkeypatch):
    """同一账号只能处于一个有效租约，确认后进入终态。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    account_id = _create_account()
    first = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 10, "lease_seconds": 300},
    )
    assert first.status_code == 200
    lease = first.json()
    assert [item["delivery_id"] for item in lease["items"]] == [account_id]
    assert lease["items"][0]["raw_session"]["accessToken"] == "access-token"

    second = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "other", "limit": 10, "lease_seconds": 300},
    )
    assert second.json()["items"] == []

    acknowledged = client.post(
        f"/api/producer/leases/{lease['lease_id']}/ack",
        headers=_headers(),
        json={"delivery_ids": [account_id]},
    )
    assert acknowledged.status_code == 200
    assert inventory()["acked"] == 1


def test_release_makes_account_available_again(client, monkeypatch):
    """消费失败释放后，账号可被下一轮重新领取。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    account_id = _create_account(email="release@test.com")
    leased = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    response = client.post(
        f"/api/producer/leases/{leased['lease_id']}/release",
        headers=_headers(),
        json={"delivery_ids": [account_id], "reason": "数据库暂时不可用"},
    )
    assert response.status_code == 200
    assert inventory()["available"] == 1


def test_missing_access_token_is_blocked(client, monkeypatch):
    """缺必要凭据的账号不会交付给消费者。"""
    monkeypatch.setenv("PRODUCER_API_KEY", "producer-test-key")
    _create_account(email="blocked@test.com", access_token="")
    leased = client.post(
        "/api/producer/leases",
        headers=_headers(),
        json={"consumer_id": "teamauto", "limit": 1, "lease_seconds": 300},
    ).json()
    assert leased["items"] == []
    assert inventory()["blocked"] == 1
