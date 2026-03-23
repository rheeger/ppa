"""Copilot finance CSV adapter."""

from __future__ import annotations

import csv
import os
from datetime import date, datetime
from time import perf_counter
from typing import Any, Iterable

from .base import BaseAdapter, FetchedBatch, deterministic_provenance
from hfa.schema import FinanceCard
from hfa.uid import generate_uid


class CopilotFinanceAdapter(BaseAdapter):
    source_id = "copilot-finance"
    preload_existing_uid_index = False
    enable_person_resolution = False

    @staticmethod
    def _parse_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        lowered = str(value or "").strip().lower()
        return lowered in {"1", "true", "yes", "y"}

    @staticmethod
    def _parse_tags(raw: str) -> list[str]:
        return [part.strip() for part in str(raw or "").split(",") if part and part.strip()]

    def _adapter_log(self, message: str, *, verbose: bool) -> None:
        if not verbose:
            return
        timestamp = datetime.now().isoformat(timespec="seconds")
        print(f"[{timestamp}] {self.source_id}: {message}", flush=True)

    @staticmethod
    def _resolve_csv_path(csv_path: str | None) -> str | None:
        path = csv_path or os.path.join(os.path.expanduser("~"), "Downloads", "copilot-transactions.csv")
        downloads = os.path.join(os.path.expanduser("~"), "Downloads")
        if not os.path.isfile(path) and os.path.isdir(downloads):
            for candidate in os.listdir(downloads):
                if "copilot" in candidate.lower() and candidate.lower().endswith(".csv"):
                    path = os.path.join(downloads, candidate)
                    break
        return path if os.path.isfile(path) else None

    def _iter_rows(
        self,
        path: str,
        *,
        threshold: float,
        verbose: bool = False,
        progress_every: int = 1000,
    ) -> Iterable[dict[str, Any]]:
        started_at = perf_counter()
        self._adapter_log(
            f"csv scan start: path={path} threshold={threshold:.2f} progress_every={progress_every}",
            verbose=verbose,
        )
        scanned = 0
        yielded = 0
        skipped_threshold = 0
        with open(path, encoding="utf-8", errors="ignore") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                scanned += 1
                lower = {str(key).lower(): (value or "").strip() for key, value in row.items()}
                amount_raw = lower.get("amount") or "0"
                try:
                    amount = float(amount_raw.replace("$", "").replace(",", ""))
                except ValueError:
                    amount = 0.0
                if abs(amount) < threshold:
                    skipped_threshold += 1
                    if progress_every and scanned % progress_every == 0:
                        self._adapter_log(
                            f"csv scan progress: scanned={scanned} yielded={yielded} skipped_threshold={skipped_threshold}",
                            verbose=verbose,
                        )
                    continue
                transaction_date = lower.get("date") or lower.get("transaction date") or date.today().isoformat()
                merchant = lower.get("merchant") or lower.get("name") or lower.get("description") or "transaction"
                yielded += 1
                if progress_every and scanned % progress_every == 0:
                    self._adapter_log(
                        f"csv scan progress: scanned={scanned} yielded={yielded} skipped_threshold={skipped_threshold}",
                        verbose=verbose,
                    )
                yield {
                    "date": transaction_date[:10],
                    "merchant": merchant.strip(),
                    "amount": amount,
                    "currency": (lower.get("currency") or "USD").upper(),
                    "category": (lower.get("category") or lower.get("account type") or "").strip(),
                    "parent_category": (lower.get("parent category") or "").strip(),
                    "account": (lower.get("account") or "").strip(),
                    "account_mask": (lower.get("account mask") or "").strip(),
                    "transaction_status": (lower.get("status") or "").strip(),
                    "transaction_type": (lower.get("type") or "").strip(),
                    "excluded": self._parse_bool(lower.get("excluded")),
                    "provider_tags": self._parse_tags(lower.get("tags") or ""),
                    "note": (lower.get("note") or "").strip(),
                    "recurring_label": (lower.get("recurring") or "").strip(),
                }
        self._adapter_log(
            f"csv scan done: scanned={scanned} yielded={yielded} skipped_threshold={skipped_threshold} "
            f"elapsed_s={perf_counter() - started_at:.2f}",
            verbose=verbose,
        )

    def fetch(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        csv_path: str | None = None,
        **kwargs,
    ) -> list[dict[str, Any]]:
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        self._adapter_log("fetch start", verbose=verbose)
        path = self._resolve_csv_path(csv_path)
        if path is None:
            self._adapter_log("fetch done: no csv found", verbose=verbose)
            return []
        threshold = float(config.finance_min_amount if config else 20.0)
        rows = list(self._iter_rows(path, threshold=threshold, verbose=verbose, progress_every=progress_every))
        self._adapter_log(f"fetch done: rows={len(rows)}", verbose=verbose)
        return rows

    def fetch_batches(
        self,
        vault_path: str,
        cursor: dict[str, Any],
        config=None,
        csv_path: str | None = None,
        **kwargs,
    ) -> Iterable[FetchedBatch]:
        verbose = self.ingest_verbose(**kwargs)
        progress_every = self.ingest_progress_every(**kwargs)
        fetch_started_at = perf_counter()
        self._adapter_log("fetch_batches start", verbose=verbose)
        path = self._resolve_csv_path(csv_path)
        if path is None:
            self._adapter_log("fetch_batches done: no csv found", verbose=verbose)
            return
        threshold = float(config.finance_min_amount if config else 20.0)
        requested_batch_size = kwargs.get("batch_size") or os.environ.get("HFA_COPILOT_BATCH_SIZE") or 500
        batch_size = max(1, int(requested_batch_size))
        self._adapter_log(
            f"fetch_batches config: path={path} batch_size={batch_size} threshold={threshold:.2f}",
            verbose=verbose,
        )
        batch_items: list[dict[str, Any]] = []
        sequence = 0
        yielded_items = 0
        for item in self._iter_rows(path, threshold=threshold, verbose=verbose, progress_every=progress_every):
            batch_items.append(item)
            if len(batch_items) >= batch_size:
                yielded_items += len(batch_items)
                self._adapter_log(
                    f"yield batch: sequence={sequence} items={len(batch_items)} cumulative_items={yielded_items}",
                    verbose=verbose,
                )
                yield FetchedBatch(items=batch_items, sequence=sequence)
                sequence += 1
                batch_items = []
        if batch_items:
            yielded_items += len(batch_items)
            self._adapter_log(
                f"yield batch: sequence={sequence} items={len(batch_items)} cumulative_items={yielded_items}",
                verbose=verbose,
            )
            yield FetchedBatch(items=batch_items, sequence=sequence)
        self._adapter_log(
            f"fetch_batches done: batches={sequence + (1 if batch_items else 0)} items={yielded_items} "
            f"elapsed_s={perf_counter() - fetch_started_at:.2f}",
            verbose=verbose,
        )

    def to_card(self, item: dict[str, Any]):
        transaction_date = str(item.get("date", "")).strip() or date.today().isoformat()
        source_id = f"{transaction_date}:{item.get('merchant', '')}:{item.get('amount', 0)}"
        category = str(item.get("category", "")).strip()
        parent_category = str(item.get("parent_category", "")).strip()
        transaction_status = str(item.get("transaction_status", "")).strip()
        transaction_type = str(item.get("transaction_type", "")).strip()
        provider_tags = [str(tag).strip() for tag in item.get("provider_tags", []) if str(tag).strip()]
        tag_candidates = ["copilot", "transaction"]
        if category:
            tag_candidates.append(category.lower())
        if parent_category:
            tag_candidates.append(parent_category.lower())
        if transaction_type:
            tag_candidates.append(transaction_type.lower())
        if transaction_status:
            tag_candidates.append(transaction_status.lower())
        tag_candidates.extend(provider_tags)
        card = FinanceCard(
            uid=generate_uid("finance", self.source_id, source_id),
            type="finance",
            source=["copilot"],
            source_id=source_id,
            created=transaction_date,
            updated=date.today().isoformat(),
            summary=f"{item.get('merchant', 'transaction')} {float(item.get('amount', 0) or 0):.2f}",
            tags=tag_candidates,
            amount=float(item.get("amount", 0) or 0),
            currency=str(item.get("currency", "USD")).upper(),
            counterparty=str(item.get("merchant", "")).strip(),
            category=category,
            parent_category=parent_category,
            account=str(item.get("account", "")).strip(),
            account_mask=str(item.get("account_mask", "")).strip(),
            transaction_status=transaction_status,
            transaction_type=transaction_type,
            excluded=self._parse_bool(item.get("excluded")),
            provider_tags=provider_tags,
            note=str(item.get("note", "")).strip(),
            recurring_label=str(item.get("recurring_label", "")).strip(),
        )
        provenance = deterministic_provenance(card, "copilot")
        return card, provenance, ""
