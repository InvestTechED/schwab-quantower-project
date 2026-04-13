from datetime import datetime

from fastapi import APIRouter, HTTPException

from app.models import EquityOrderRequest, ModifyEquityOrderRequest
from app.services.broker import SchwabBrokerService

router = APIRouter(tags=["broker"])

broker_service = SchwabBrokerService()


@router.get("/broker/accounts")
def broker_accounts():
    return broker_service.get_accounts()


@router.get("/broker/positions")
def broker_positions():
    return broker_service.get_positions()


@router.get("/broker/orders")
def broker_orders(
    lookback_days: int = 7,
    from_entered_datetime: datetime | None = None,
    to_entered_datetime: datetime | None = None,
):
    return broker_service.get_orders(
        lookback_days=lookback_days,
        from_entered_datetime=from_entered_datetime,
        to_entered_datetime=to_entered_datetime,
    )


@router.get("/broker/executions")
def broker_executions(
    lookback_days: int = 7,
    from_entered_datetime: datetime | None = None,
    to_entered_datetime: datetime | None = None,
):
    return broker_service.get_executions(
        lookback_days=lookback_days,
        from_entered_datetime=from_entered_datetime,
        to_entered_datetime=to_entered_datetime,
    )


@router.post("/broker/orders/preview")
def broker_preview_order(request: EquityOrderRequest):
    try:
        return broker_service.preview_order(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        response = getattr(exc, "response", None)
        detail = response.text if response is not None else str(exc)
        status_code = response.status_code if response is not None else 502
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/broker/orders/place")
def broker_place_order(request: EquityOrderRequest):
    try:
        return broker_service.place_order(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        response = getattr(exc, "response", None)
        detail = response.text if response is not None else str(exc)
        status_code = response.status_code if response is not None else 502
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.post("/broker/orders/modify")
def broker_modify_order(request: ModifyEquityOrderRequest):
    try:
        return broker_service.modify_order(request)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        response = getattr(exc, "response", None)
        detail = response.text if response is not None else str(exc)
        status_code = response.status_code if response is not None else 502
        raise HTTPException(status_code=status_code, detail=detail) from exc


@router.delete("/broker/orders/{account_hash}/{order_id}")
def broker_cancel_order(account_hash: str, order_id: str):
    try:
        return broker_service.cancel_order(account_hash, order_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except Exception as exc:
        response = getattr(exc, "response", None)
        detail = response.text if response is not None else str(exc)
        status_code = response.status_code if response is not None else 502
        raise HTTPException(status_code=status_code, detail=detail) from exc
