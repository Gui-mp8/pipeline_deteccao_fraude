from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict


class BaseTableContract(BaseModel):
    model_config = ConfigDict(extra="ignore")


class PlayerContract(BaseTableContract):
    player_id: Any
    email: Any
    city: Any
    created_at: Any


class SessionContract(BaseTableContract):
    session_id: Any
    player_id: Any
    ip: Any
    device: Any
    timestamp: Any


class TransactionContract(BaseTableContract):
    transaction_id: Any
    player_id: Any
    type: Any
    amount: Any
    timestamp: Any


class AffiliateCpaFtdContract(BaseTableContract):
    affiliate_id: Any
    player_id: Any
    country: Any
    clicks: Any
    registrations: Any
    ftd: Any
    cpa_value: Any
