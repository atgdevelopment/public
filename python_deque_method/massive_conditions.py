from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


@dataclass(frozen=True, slots=True)
class TradeConditionRule:
    code: int
    name: str
    sip_mapping: str | None = None
    updates_high_low: bool = False
    updates_last: bool = False
    updates_volume: bool = False


@dataclass(frozen=True, slots=True)
class QuoteConditionRule:
    code: int
    name: str
    is_valid: bool = True
    is_firm: bool = False
    allows_spread: bool = False


@dataclass(frozen=True, slots=True)
class TradeConditionProfile:
    codes: tuple[int, ...]
    names: tuple[str, ...]
    updates_high_low: bool
    updates_last: bool
    updates_volume: bool


@dataclass(frozen=True, slots=True)
class QuoteConditionProfile:
    codes: tuple[int, ...]
    names: tuple[str, ...]
    is_valid: bool
    is_firm: bool
    allows_spread: bool


def _t(
    code: int,
    name: str,
    sip: str | None = None,
    *,
    hl: bool = False,
    last: bool = False,
    vol: bool = False,
) -> TradeConditionRule:
    return TradeConditionRule(
        code=code,
        name=name,
        sip_mapping=sip,
        updates_high_low=hl,
        updates_last=last,
        updates_volume=vol,
    )


def _q(
    code: int,
    name: str,
    *,
    valid: bool = True,
    firm: bool = False,
    spread: bool = False,
) -> QuoteConditionRule:
    return QuoteConditionRule(
        code=code,
        name=name,
        is_valid=valid,
        is_firm=firm,
        allows_spread=spread,
    )


# The quote source list duplicated code 23 and skipped 24. To preserve both labels,
# 24 is normalized to "DueToRelatedSecurityNewsDissemination".

TRADE_CONDITION_RULES: dict[int, TradeConditionRule] = {
    0: _t(0, "Regular Sale", "@", hl=True, last=True, vol=True),
    1: _t(1, "Acquisition", "A", hl=True, last=True, vol=True),
    2: _t(2, "Average Price Trade", "W", hl=False, last=False, vol=True),
    3: _t(3, "Automatic Execution", None, hl=True, last=True, vol=True),
    4: _t(4, "Bunched Trade", "B", hl=True, last=True, vol=True),
    5: _t(5, "Bunched Sold Trade", "G", hl=True, last=False, vol=True),
    6: _t(6, "CAP Election", None, hl=False, last=False, vol=True),
    7: _t(7, "Cash Sale", "C", hl=False, last=False, vol=True),
    8: _t(8, "Closing Prints", "6", hl=True, last=True, vol=True),
    9: _t(9, "Cross Trade", "X", hl=True, last=True, vol=True),
    10: _t(10, "Derivatively Priced", "4", hl=True, last=False, vol=True),
    11: _t(11, "Distribution", "D", hl=True, last=True, vol=True),
    12: _t(12, "Form T", "T", hl=False, last=False, vol=True),
    13: _t(13, "Extended Trading Hours (Sold Out of Sequence)", "U", hl=False, last=False, vol=True),
    14: _t(14, "Intermarket Sweep", "F", hl=True, last=True, vol=True),
    15: _t(15, "Market Center Official Close", "M", hl=False, last=False, vol=False),
    16: _t(16, "Market Center Official Open", "Q", hl=False, last=False, vol=False),
    17: _t(17, "Market Center Opening Trade", None, hl=False, last=False, vol=True),
    18: _t(18, "Market Center Reopening Trade", None, hl=False, last=False, vol=True),
    19: _t(19, "Market Center Closing Trade", None, hl=False, last=False, vol=True),
    20: _t(20, "Next Day", "N", hl=False, last=False, vol=True),
    21: _t(21, "Price Variation Trade", "H", hl=False, last=False, vol=True),
    22: _t(22, "Prior Reference Price", "P", hl=True, last=False, vol=True),
    23: _t(23, "Rule 155 Trade (AMEX)", "K", hl=True, last=True, vol=True),
    24: _t(24, "Rule 127 NYSE", None, hl=False, last=False, vol=True),
    25: _t(25, "Opening Prints", "O", hl=True, last=True, vol=True),
    26: _t(26, "Opened", None, hl=False, last=False, vol=True),
    27: _t(27, "Stopped Stock (Regular Trade)", "1", hl=True, last=True, vol=True),
    28: _t(28, "Re-Opening Prints", "5", hl=True, last=True, vol=True),
    29: _t(29, "Seller", "R", hl=True, last=False, vol=True),
    30: _t(30, "Sold Last", "L", hl=True, last=True, vol=True),
    31: _t(31, "EquipmentChangeover", None, hl=False, last=False, vol=True),
    32: _t(32, "Sold Out", None, hl=False, last=False, vol=True),
    33: _t(33, "Sold (out of Sequence)", "Z", hl=True, last=False, vol=True),
    34: _t(34, "Split Trade", "S", hl=True, last=True, vol=True),
    35: _t(35, "Stock Option", None, hl=False, last=False, vol=True),
    36: _t(36, "Yellow Flag Regular Trade", "Y", hl=True, last=True, vol=True),
    37: _t(37, "Odd Lot Trade", "I", hl=False, last=False, vol=True),
    38: _t(38, "Corrected Consolidated Close (per listing market)", "9", hl=True, last=True, vol=False),
    39: _t(39, "Unknown", None, hl=False, last=False, vol=True),
    40: _t(40, "Held", None, hl=False, last=False, vol=True),
    41: _t(41, "Trade Thru Exempt", None, hl=False, last=False, vol=True),
    42: _t(42, "NonEligible", None, hl=False, last=False, vol=True),
    43: _t(43, "NonEligible Extended", None, hl=False, last=False, vol=True),
    44: _t(44, "Cancelled", None, hl=False, last=False, vol=True),
    45: _t(45, "Recovery", None, hl=False, last=False, vol=True),
    46: _t(46, "Correction", None, hl=False, last=False, vol=True),
    47: _t(47, "As of", None, hl=False, last=False, vol=True),
    48: _t(48, "As of Correction", None, hl=False, last=False, vol=True),
    49: _t(49, "As of Cancel", None, hl=False, last=False, vol=True),
    50: _t(50, "OOB", None, hl=False, last=False, vol=True),
    51: _t(51, "Summary", None, hl=False, last=False, vol=True),
    52: _t(52, "Contingent Trade", "V", hl=False, last=False, vol=True),
    53: _t(53, 'Qualified Contingent Trade ("QCT")', "7", hl=False, last=False, vol=True),
    54: _t(54, "Errored", None, hl=False, last=False, vol=True),
    55: _t(55, "OPENING_REOPENING_TRADE_DETAIL", None, hl=False, last=False, vol=True),
    56: _t(56, "Placeholder", "E", hl=False, last=False, vol=False),
    59: _t(59, "Placeholder for 611 exempt", "8", hl=False, last=False, vol=False),
}

QUOTE_CONDITION_RULES: dict[int, QuoteConditionRule] = {
    -1: _q(-1, "Invalid", valid=False, firm=False, spread=False),
    0: _q(0, "Regular", valid=True, firm=True, spread=True),
    1: _q(1, "RegularTwoSidedOpen", valid=True, firm=True, spread=True),
    2: _q(2, "RegularOneSidedOpen", valid=True, firm=False, spread=False),
    3: _q(3, "SlowAsk", valid=True, firm=False, spread=False),
    4: _q(4, "SlowBid", valid=True, firm=False, spread=False),
    5: _q(5, "SlowBidAsk", valid=True, firm=False, spread=False),
    6: _q(6, "SlowDueLRPBid", valid=True, firm=False, spread=False),
    7: _q(7, "SlowDueLRPAsk", valid=True, firm=False, spread=False),
    8: _q(8, "SlowDueNYSELRP", valid=True, firm=False, spread=False),
    9: _q(9, "SlowDueSetSlowListBidAsk", valid=True, firm=False, spread=False),
    10: _q(10, "ManualAskAutomatedBid", valid=True, firm=False, spread=False),
    11: _q(11, "ManualBidAutomatedAsk", valid=True, firm=False, spread=False),
    12: _q(12, "ManualBidAndAsk", valid=True, firm=False, spread=False),
    13: _q(13, "Opening", valid=True, firm=False, spread=False),
    14: _q(14, "Closing", valid=True, firm=False, spread=False),
    15: _q(15, "Closed", valid=False, firm=False, spread=False),
    16: _q(16, "Resume", valid=True, firm=False, spread=False),
    17: _q(17, "FastTrading", valid=True, firm=True, spread=True),
    18: _q(18, "TradingRangeIndication", valid=True, firm=False, spread=False),
    19: _q(19, "MarketMakerQuotesClosed", valid=False, firm=False, spread=False),
    20: _q(20, "NonFirm", valid=False, firm=False, spread=False),
    21: _q(21, "NewsDissemination", valid=True, firm=False, spread=False),
    22: _q(22, "OrderInflux", valid=True, firm=False, spread=False),
    23: _q(23, "OrderImbalance", valid=True, firm=False, spread=False),
    24: _q(24, "DueToRelatedSecurityNewsDissemination", valid=True, firm=False, spread=False),
    25: _q(25, "DueToRelatedSecurityNewsPending", valid=True, firm=False, spread=False),
    26: _q(26, "AdditionalInformation", valid=True, firm=False, spread=False),
    27: _q(27, "NewsPending", valid=True, firm=False, spread=False),
    28: _q(28, "AdditionalInformationDueToRelatedSecurity", valid=True, firm=False, spread=False),
    29: _q(29, "DueToRelatedSecurity", valid=True, firm=False, spread=False),
    30: _q(30, "InViewOfCommon", valid=True, firm=False, spread=False),
    31: _q(31, "EquipmentChangeover", valid=True, firm=False, spread=False),
    32: _q(32, "NoOpenNoResponse", valid=False, firm=False, spread=False),
    33: _q(33, "SubPennyTrading", valid=True, firm=True, spread=True),
    34: _q(34, "AutomatedBidNoOfferNoBid", valid=False, firm=False, spread=False),
    35: _q(35, "LULDPriceBand", valid=True, firm=False, spread=False),
    36: _q(36, "MarketWideCircuitBreakerLevel1", valid=False, firm=False, spread=False),
    37: _q(37, "MarketWideCircuitBreakerLevel2", valid=False, firm=False, spread=False),
    38: _q(38, "MarketWideCircuitBreakerLevel3", valid=False, firm=False, spread=False),
    39: _q(39, "RepublishedLULDPriceBand", valid=True, firm=False, spread=False),
    40: _q(40, "OnDemandAuction", valid=True, firm=False, spread=False),
    41: _q(41, "CashOnlySettlement", valid=True, firm=False, spread=False),
    42: _q(42, "NextDaySettlement", valid=True, firm=False, spread=False),
    43: _q(43, "LULDTradingPause", valid=False, firm=False, spread=False),
    71: _q(71, "SlowDueLRPBidAsk", valid=True, firm=False, spread=False),
    80: _q(80, "Cancel", valid=False, firm=False, spread=False),
    81: _q(81, "Corrected_Price", valid=False, firm=False, spread=False),
    82: _q(82, "SIPGenerated", valid=False, firm=False, spread=False),
    83: _q(83, "Unknown", valid=False, firm=False, spread=False),
    84: _q(84, "Crossed_Market", valid=True, firm=False, spread=False),
    85: _q(85, "Locked_Market", valid=True, firm=False, spread=False),
    86: _q(86, "Depth_On_Offer_Side", valid=True, firm=False, spread=False),
    87: _q(87, "Depth_On_Bid_Side", valid=True, firm=False, spread=False),
    88: _q(88, "Depth_On_Bid_And_Offer", valid=True, firm=False, spread=False),
    89: _q(89, "Pre_Opening_Indication", valid=True, firm=False, spread=False),
    90: _q(90, "Syndicate_Bid", valid=True, firm=False, spread=False),
    91: _q(91, "Pre_Syndicate_Bid", valid=True, firm=False, spread=False),
    92: _q(92, "Penalty_Bid", valid=True, firm=False, spread=False),
}


def _coerce_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


def _unique_ordered(values: Iterable[int | None]) -> tuple[int, ...]:
    out: list[int] = []
    seen: set[int] = set()
    for value in values:
        if value is None:
            continue
        iv = int(value)
        if iv in seen:
            continue
        seen.add(iv)
        out.append(iv)
    return tuple(out)


def _extract_flat_codes(value: Any) -> tuple[int, ...]:
    if value is None:
        return ()

    if isinstance(value, (list, tuple, set)):
        return _unique_ordered(_coerce_int(v) for v in value)

    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return ()

        for sep in (",", "|", ";", " "):
            if sep in stripped:
                return _unique_ordered(
                    _coerce_int(part.strip())
                    for part in stripped.split(sep)
                    if part.strip()
                )

    return _unique_ordered((_coerce_int(value),))


def extract_trade_condition_codes(fields: Mapping[str, Any]) -> tuple[int, ...]:
    flat = _extract_flat_codes(fields.get("C"))
    if flat:
        return flat

    return _unique_ordered(
        (
            _coerce_int(fields.get("c1")),
            _coerce_int(fields.get("c2")),
        )
    )


def extract_quote_indicator_codes(fields: Mapping[str, Any]) -> tuple[int, ...]:
    flat = _extract_flat_codes(fields.get("i"))
    if flat:
        return flat

    numbered: list[tuple[int, int]] = []

    for key, value in fields.items():
        if not isinstance(key, str):
            continue
        if not key.startswith("i"):
            continue

        suffix = key[1:]
        if not suffix.isdigit():
            continue

        iv = _coerce_int(value)
        if iv is not None:
            numbered.append((int(suffix), iv))

    if numbered:
        numbered.sort(key=lambda x: x[0])
        return tuple(code for _, code in numbered)

    fallback = _coerce_int(fields.get("i1"))
    return (fallback,) if fallback is not None else ()


def _trade_rule_for(code: int) -> TradeConditionRule:
    rule = TRADE_CONDITION_RULES.get(code)
    if rule is not None:
        return rule
    return _t(code, f"Unknown({code})", None, hl=False, last=False, vol=False)


def _quote_rule_for(code: int) -> QuoteConditionRule:
    rule = QUOTE_CONDITION_RULES.get(code)
    if rule is not None:
        return rule
    return _q(code, f"Unknown({code})", valid=False, firm=False, spread=False)


def build_trade_condition_profile_from_codes(codes: Iterable[int] | None) -> TradeConditionProfile:
    normalized = _unique_ordered(codes or ())
    if not normalized:
        normalized = (0,)

    rules = tuple(_trade_rule_for(code) for code in normalized)

    updates_high_low = all(rule.updates_high_low for rule in rules)
    updates_last = all(rule.updates_last for rule in rules)
    updates_volume = all(rule.updates_volume for rule in rules)

    return TradeConditionProfile(
        codes=normalized,
        names=tuple(rule.name for rule in rules),
        updates_high_low=updates_high_low,
        updates_last=updates_last,
        updates_volume=updates_volume,
    )


def build_trade_condition_profile_from_fields(fields: Mapping[str, Any]) -> TradeConditionProfile:
    return build_trade_condition_profile_from_codes(extract_trade_condition_codes(fields))


def build_quote_condition_profile_from_codes(codes: Iterable[int] | None) -> QuoteConditionProfile:
    normalized = _unique_ordered(codes or ())
    if not normalized:
        normalized = (0,)

    rules = tuple(_quote_rule_for(code) for code in normalized)

    is_valid = all(rule.is_valid for rule in rules)
    is_firm = all(rule.is_firm for rule in rules)
    allows_spread = all(rule.allows_spread for rule in rules)

    return QuoteConditionProfile(
        codes=normalized,
        names=tuple(rule.name for rule in rules),
        is_valid=is_valid,
        is_firm=is_firm,
        allows_spread=allows_spread,
    )


def build_quote_condition_profile_from_fields(fields: Mapping[str, Any]) -> QuoteConditionProfile:
    return build_quote_condition_profile_from_codes(extract_quote_indicator_codes(fields))