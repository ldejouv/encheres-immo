from __future__ import annotations

from datetime import date, time
from typing import Optional

from pydantic import BaseModel


class TribunalInfo(BaseModel):
    name: str
    slug: str
    region: str
    auction_count: int
    url_path: str


class ListingSummary(BaseModel):
    licitor_id: int
    url_path: str
    property_type: Optional[str] = None
    department_code: Optional[str] = None
    city: Optional[str] = None
    mise_a_prix: Optional[int] = None
    description_short: Optional[str] = None
    publication_date: Optional[str] = None
    # Adjudication result (from results pages)
    final_price: Optional[int] = None
    result_status: Optional[str] = None  # "sold", "carence", "non_requise"
    result_date: Optional[str] = None


class ListingDetail(BaseModel):
    licitor_id: int
    url_path: str
    property_type: Optional[str] = None
    description: Optional[str] = None
    surface_m2: Optional[float] = None
    energy_rating: Optional[str] = None
    occupancy_status: Optional[str] = None

    department_code: Optional[str] = None
    city: Optional[str] = None
    full_address: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    cadastral_ref: Optional[str] = None

    tribunal_name: Optional[str] = None
    tribunal_slug: Optional[str] = None
    auction_date: Optional[date] = None
    auction_time: Optional[time] = None
    mise_a_prix: Optional[int] = None
    case_reference: Optional[str] = None
    has_price_reduction: Optional[str] = None

    lawyer_name: Optional[str] = None
    lawyer_phone: Optional[str] = None

    visit_date: Optional[str] = None

    price_per_m2_min: Optional[float] = None
    price_per_m2_avg: Optional[float] = None
    price_per_m2_max: Optional[float] = None

    view_count: Optional[int] = None
    favorites_count: Optional[int] = None
    publication_date: Optional[str] = None


class AlertCriteria(BaseModel):
    name: str
    min_price: Optional[int] = None
    max_price: Optional[int] = None
    department_codes: Optional[list[str]] = None
    regions: Optional[list[str]] = None
    property_types: Optional[list[str]] = None
    min_surface: Optional[float] = None
    max_surface: Optional[float] = None
    tribunal_slugs: Optional[list[str]] = None
