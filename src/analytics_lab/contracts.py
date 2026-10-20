from datetime import UTC, date, datetime
from typing import Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, field_validator, model_validator

Source = Literal["google_ads", "meta_ads", "crm"]


class Record(BaseModel):
    model_config = ConfigDict(extra="forbid")
    record_id: str = Field(min_length=1, max_length=100)
    updated_at: AwareDatetime
    day: date
    campaign_id: str = Field(min_length=1, max_length=100)
    amount_minor: int = Field(ge=0, le=10**12, strict=True)
    currency: Literal["PLN"]

    @field_validator("updated_at")
    @classmethod
    def normalize_timestamp(cls, value):
        return value.astimezone(UTC)


class Batch(BaseModel):
    """An adapter promises a COMPLETE update-time window, not a partial API page."""

    model_config = ConfigDict(extra="forbid")
    source: Source
    window_start: AwareDatetime
    window_end: AwareDatetime
    complete: Literal[True]
    records: list[Record] = Field(max_length=100_000)

    @field_validator("window_start", "window_end")
    @classmethod
    def normalize_timestamp(cls, value):
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def valid_window(self):
        if self.window_end < self.window_start:
            raise ValueError("Window end precedes start")
        for record in self.records:
            if not self.window_start <= record.updated_at <= self.window_end:
                raise ValueError("Record update time is outside the declared extraction window")
        return self


def require_aware(value: datetime) -> None:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("Timestamp must include a timezone")
