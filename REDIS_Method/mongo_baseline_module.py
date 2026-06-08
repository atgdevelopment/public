import logging
from dataclasses import dataclass, field
from typing import Any, Protocol

import motor.motor_asyncio

from logging_utils import append_output_to_file
from time_utils import utc_now_ms


logger = logging.getLogger(__name__)


# Mongo baseline variables
# =========================
MONGO_HOST = "192.168.1.126"
MONGO_PORT = 27017
MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}"
MONGO_DATABASE = "trading_data"
MONGO_BASELINE_COLLECTION = "baselines_new"
MONGO_SERVER_SELECTION_TIMEOUT_MS = 5_000
MONGO_SOCKET_TIMEOUT_MS = 30_000
MONGO_BASELINE_BATCH_SIZE = 500
MONGO_BASELINE_REQUIRED = True

# These are the document_type values in the current Mongo documents.
# Keep these raw; do not translate them to the old *_symbol_summary names.
PRICE_MOVEMENT_DOCUMENT_TYPE = "px_move_symbol"
QUOTE_REVERSAL_DOCUMENT_TYPE = "quote_rev_symbol"
MONGO_BASELINE_DOCUMENT_TYPES = (
    PRICE_MOVEMENT_DOCUMENT_TYPE,
    QUOTE_REVERSAL_DOCUMENT_TYPE,
)
MONGO_BASELINE_QUERY: dict[str, Any] = {
    "document_type": {"$in": list(MONGO_BASELINE_DOCUMENT_TYPES)},
    "is_latest": True,
}

MAX_BASELINES_PER_SYMBOL = 3
MAX_REVERSAL_BASELINES_PER_SYMBOL = 3


def _required_symbol(document: dict[str, Any]) -> str:
    symbol = document.get("symbol")
    if symbol is None or symbol == "":
        raise ValueError(f"Mongo baseline document has no symbol: {document.get('_id')!r}")
    return str(symbol)


def _document_id(document: dict[str, Any]) -> Any:
    return document.get("_id")


def _document_type(document: dict[str, Any]) -> str:
    return str(document.get("document_type") or "")


def _raw_dict(document: dict[str, Any], key: str) -> dict[str, Any]:
    value = document.get(key)
    if isinstance(value, dict):
        return value
    return {}


def _raw_list(document: dict[str, Any], key: str) -> list[Any]:
    value = document.get(key)
    if isinstance(value, list):
        return value
    return []


# Mongo baseline models / helpers
# =========================
@dataclass(slots=True)
class BaselineSnapshot:
    """
    Raw price-movement Mongo baseline.

    The full Mongo document is kept in `document` and returned unchanged by
    `baseline_snapshot_to_dict`. The properties below expose raw top-level
    fields for compatibility with the existing app/state code; they do not
    rename, compact, map, or normalise Mongo fields.
    """

    symbol: str
    document_id: Any
    document_type: str
    loaded_at_ms: int
    document: dict[str, Any]

    @property
    def raw(self) -> dict[str, Any]:
        return self.document

    @property
    def run_id(self) -> Any:
        return self.document.get("run_id")

    @property
    def generated_at_utc(self) -> Any:
        return self.document.get("generated_at_utc")

    @property
    def input_dir(self) -> Any:
        return self.document.get("input_dir")

    @property
    def input_files(self) -> dict[str, Any]:
        return _raw_dict(self.document, "input_files")

    @property
    def input_row_counts(self) -> dict[str, Any]:
        return _raw_dict(self.document, "input_row_counts")

    @property
    def thresholds(self) -> dict[str, Any]:
        return _raw_dict(self.document, "thresholds")

    @property
    def trade_baseline(self) -> dict[str, Any]:
        return _raw_dict(self.document, "trade_baseline")

    @property
    def regimes(self) -> list[Any]:
        return _raw_list(self.document, "regime_summary")

    @property
    def quote_reprice_by_dir(self) -> list[Any]:
        return _raw_list(self.document, "quote_reprice_by_dir")

    @property
    def quote_confirmation(self) -> list[Any]:
        return self.quote_reprice_by_dir

    @property
    def conditions(self) -> list[Any]:
        return _raw_list(self.document, "top_cond_moves")

    @property
    def condition_combos(self) -> list[Any]:
        return _raw_list(self.document, "top_cond_combos")

    @property
    def lead_profiles(self) -> list[Any]:
        return _raw_list(self.document, "lead_profiles")

    @property
    def quality(self) -> dict[str, Any]:
        # px_move_symbol documents may not have a quality object. Return only
        # a raw quality object if Mongo provides one; do not derive one.
        return _raw_dict(self.document, "quality")

    @property
    def ratios(self) -> dict[str, Any]:
        # Kept for import/runtime compatibility. No derived ratios are built.
        return _raw_dict(self.document, "ratios")


@dataclass(slots=True)
class ReversalBaselineSnapshot:
    """
    Raw quote-reversal Mongo baseline.

    `thresholds`, `settings`, `short_to_long`, and `long_to_short` are exposed
    exactly as stored in Mongo. In particular threshold keys remain raw:
    min_mid_move_pct, max_spread_pct, and require_same_dir.
    """

    symbol: str
    document_id: Any
    document_type: str
    loaded_at_ms: int
    document: dict[str, Any]

    @property
    def raw(self) -> dict[str, Any]:
        return self.document

    @property
    def run_id(self) -> Any:
        return self.document.get("run_id")

    @property
    def generated_at_utc(self) -> Any:
        return self.document.get("generated_at_utc")

    @property
    def input_dir(self) -> Any:
        return self.document.get("input_dir")

    @property
    def input_files(self) -> dict[str, Any]:
        return _raw_dict(self.document, "input_files")

    @property
    def settings(self) -> dict[str, Any]:
        return _raw_dict(self.document, "settings")

    @property
    def input_row_counts(self) -> dict[str, Any]:
        return _raw_dict(self.document, "input_row_counts")

    @property
    def candidate_count(self) -> Any:
        return self.document.get("candidate_count")

    @property
    def quality(self) -> dict[str, Any]:
        return _raw_dict(self.document, "quality")

    @property
    def short_to_long(self) -> dict[str, Any]:
        return _raw_dict(self.document, "short_to_long")

    @property
    def long_to_short(self) -> dict[str, Any]:
        return _raw_dict(self.document, "long_to_short")

    @property
    def sides(self) -> dict[str, dict[str, Any]]:
        # Compatibility view only. It keeps the raw side names and raw side dicts.
        return {
            "short_to_long": self.short_to_long,
            "long_to_short": self.long_to_short,
        }

    @property
    def thresholds(self) -> dict[str, Any]:
        return _raw_dict(self.document, "thresholds")

    @property
    def candidate_examples(self) -> list[Any]:
        return _raw_list(self.document, "candidate_examples")


# Builders keep the raw Mongo document object. No field remapping, compaction,
# derived metrics, or normalisation is performed.
def build_baseline_snapshot(document: dict[str, Any]) -> BaselineSnapshot:
    return BaselineSnapshot(
        symbol=_required_symbol(document),
        document_id=_document_id(document),
        document_type=_document_type(document),
        loaded_at_ms=utc_now_ms(),
        document=document,
    )


def build_reversal_baseline_snapshot(document: dict[str, Any]) -> ReversalBaselineSnapshot:
    return ReversalBaselineSnapshot(
        symbol=_required_symbol(document),
        document_id=_document_id(document),
        document_type=_document_type(document),
        loaded_at_ms=utc_now_ms(),
        document=document,
    )


def baseline_snapshot_to_dict(snapshot: BaselineSnapshot) -> dict[str, Any]:
    # Return the raw Mongo document directly for speed and exact field fidelity.
    return snapshot.document


def reversal_baseline_snapshot_to_dict(snapshot: ReversalBaselineSnapshot) -> dict[str, Any]:
    # Return the raw Mongo document directly for speed and exact field fidelity.
    return snapshot.document


class BaselineStateStore(Protocol):
    def add_baseline(self, baseline: BaselineSnapshot) -> Any:
        ...

    def add_reversal_baseline(self, baseline: ReversalBaselineSnapshot) -> Any:
        ...

    def baseline_count(self) -> int:
        ...

    def reversal_baseline_count(self) -> int:
        ...


@dataclass(slots=True)
class MongoBaselineConfig:
    uri: str = MONGO_URI
    database: str = MONGO_DATABASE
    collection: str = MONGO_BASELINE_COLLECTION
    query: dict[str, Any] = field(default_factory=lambda: dict(MONGO_BASELINE_QUERY))
    server_selection_timeout_ms: int = MONGO_SERVER_SELECTION_TIMEOUT_MS
    socket_timeout_ms: int = MONGO_SOCKET_TIMEOUT_MS
    batch_size: int = MONGO_BASELINE_BATCH_SIZE


class MongoBaselineLoader:
    def __init__(self, config: MongoBaselineConfig | None = None) -> None:
        self.config = config or MongoBaselineConfig()
        self.client = motor.motor_asyncio.AsyncIOMotorClient(
            self.config.uri,
            serverSelectionTimeoutMS=self.config.server_selection_timeout_ms,
            socketTimeoutMS=self.config.socket_timeout_ms,
        )
        self.collection = self.client[self.config.database][self.config.collection]

    async def close(self) -> None:
        self.client.close()

    async def ping(self) -> None:
        await self.client.admin.command("ping")

    async def load_latest_symbol_baselines(
        self,
        store: BaselineStateStore,
    ) -> int:
        await self.ping()

        loaded_price_movement = 0
        loaded_reversal = 0
        skipped = 0

        cursor = self.collection.find(
            self.config.query,
            batch_size=self.config.batch_size,
        )

        async for document in cursor:
            document_type = _document_type(document)

            try:
                if document_type == PRICE_MOVEMENT_DOCUMENT_TYPE:
                    store.add_baseline(build_baseline_snapshot(document))
                    loaded_price_movement += 1
                    continue

                if document_type == QUOTE_REVERSAL_DOCUMENT_TYPE:
                    store.add_reversal_baseline(build_reversal_baseline_snapshot(document))
                    loaded_reversal += 1
                    continue

                skipped += 1
                logger.warning(
                    "Skipping unsupported raw Mongo baseline document_type=%r _id=%r",
                    document_type,
                    document.get("_id"),
                )

            except Exception:
                skipped += 1
                logger.exception(
                    "Skipping invalid raw Mongo baseline _id=%r document_type=%r",
                    document.get("_id"),
                    document_type,
                )

        loaded = loaded_price_movement + loaded_reversal
        logger.info(
            "Loaded %s raw Mongo baseline(s) into symbol state; "
            "px_move_symbol=%s quote_rev_symbol=%s skipped=%s",
            loaded,
            loaded_price_movement,
            loaded_reversal,
            skipped,
        )
        return loaded


async def load_mongo_baselines_into_market_state(
    store: BaselineStateStore,
    *,
    required: bool = MONGO_BASELINE_REQUIRED,
) -> int:
    loader = MongoBaselineLoader()

    try:
        loaded = await loader.load_latest_symbol_baselines(store)
    except Exception:
        logger.exception(
            "Failed loading raw Mongo baselines from %s/%s.%s",
            MONGO_URI,
            MONGO_DATABASE,
            MONGO_BASELINE_COLLECTION,
        )
        append_output_to_file(
            "\n========== RAW MONGO BASELINE LOAD FAILED =========="
            f"\nuri={MONGO_URI}"
            f"\ndatabase={MONGO_DATABASE}"
            f"\ncollection={MONGO_BASELINE_COLLECTION}"
            f"\nquery={MONGO_BASELINE_QUERY}"
            "\n==================================================\n"
        )
        if required:
            raise
        return 0
    finally:
        await loader.close()

    append_output_to_file(
        "\n========== RAW MONGO BASELINES LOADED =========="
        f"\nuri={MONGO_URI}"
        f"\ndatabase={MONGO_DATABASE}"
        f"\ncollection={MONGO_BASELINE_COLLECTION}"
        f"\nquery={MONGO_BASELINE_QUERY}"
        f"\nloaded={loaded}"
        f"\nprice_movement_baselines={store.baseline_count()}"
        f"\nreversal_baselines={store.reversal_baseline_count()}"
        "\n===============================================\n"
    )
    return loaded
