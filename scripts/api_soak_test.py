#!/usr/bin/env python3
import argparse
import json
import statistics
import sys
import time
import urllib.error
import urllib.request
from typing import Any


def _http_json(
    base_url: str,
    method: str,
    path: str,
    payload: dict[str, Any] | None = None,
    headers: dict[str, str] | None = None,
    timeout: int = 1200,
) -> tuple[int, dict[str, Any], float]:
    req_headers = {"accept": "application/json"}
    if headers:
        req_headers.update(headers)
    data = None
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        req_headers.setdefault("content-type", "application/json")

    request = urllib.request.Request(
        f"{base_url.rstrip('/')}{path}",
        method=method,
        data=data,
        headers=req_headers,
    )
    started = time.time()
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", "replace")
            elapsed_ms = (time.time() - started) * 1000
            try:
                body = json.loads(raw)
            except Exception:
                body = {"raw": raw[:2000]}
            return response.status, body, elapsed_ms
    except urllib.error.HTTPError as error:
        raw = error.read().decode("utf-8", "replace")
        elapsed_ms = (time.time() - started) * 1000
        try:
            body = json.loads(raw)
        except Exception:
            body = {"raw": raw[:2000]}
        return error.code, body, elapsed_ms
    except (urllib.error.URLError, TimeoutError, OSError) as error:
        elapsed_ms = (time.time() - started) * 1000
        return 0, {"error": str(error), "type": error.__class__.__name__}, elapsed_ms


def _p95(values: list[float]) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    sorted_values = sorted(values)
    idx = int(len(sorted_values) * 0.95) - 1
    if idx < 0:
        idx = 0
    return sorted_values[idx]


def _meta_duration_ms(body: dict[str, Any]) -> float | None:
    meta = body.get("meta")
    if not isinstance(meta, dict):
        return None
    value = meta.get("durationMs")
    if isinstance(value, (int, float)):
        return float(value)
    return None


def _error_code_from_body(body: Any) -> str | None:
    if not isinstance(body, dict):
        return None
    err = body.get("error")
    if isinstance(err, dict):
        code = err.get("code")
        return code if isinstance(code, str) and code else None
    if isinstance(err, str) and err:
        return err
    return None


def _run_single_matrix(
    base_url: str,
    http_timeout: int,
    excluded_ids: set[str] | None = None,
) -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]
]:
    status, opened, _ = _http_json(
        base_url, "GET", "/api/v1/opened", timeout=http_timeout
    )
    if status != 200:
        raise RuntimeError(f"GET /api/v1/opened failed: status={status} body={opened}")

    datasets = opened.get("data", {}).get("datasets", [])
    if not isinstance(datasets, list):
        datasets = []
    opened_record = {
        "phase": "opened",
        "count": len(datasets),
        "ids": [item.get("id") for item in datasets if isinstance(item, dict)],
    }

    single_specs: list[dict[str, Any]] = []
    batch_templates: list[dict[str, Any]] = []
    web_multi: list[dict[str, Any]] = []

    for item in datasets:
        if not isinstance(item, dict):
            continue
        dataset_id = item.get("id")
        mode = item.get("mode")
        if not isinstance(dataset_id, str) or not dataset_id:
            continue
        if excluded_ids and dataset_id in excluded_ids:
            continue
        detail_status, detail, detail_ms = _http_json(
            base_url, "GET", f"/api/v1/opened/{dataset_id}", timeout=http_timeout
        )
        if detail_status != 200:
            single_specs.append(
                {
                    "name": f"{mode}:detail",
                    "datasetId": dataset_id,
                    "request": None,
                    "status": detail_status,
                    "ok": False,
                    "errorCode": _error_code_from_body(detail),
                    "ms": round(detail_ms, 1),
                }
            )
            continue

        data = detail.get("data", {}) if isinstance(detail, dict) else {}
        details = data.get("details") if isinstance(data.get("details"), dict) else {}
        selection = (
            data.get("selection") if isinstance(data.get("selection"), dict) else {}
        )

        if mode == "localDirectory":
            request = {
                "datasetId": dataset_id,
                "shardName": ".",
                "traverse": True,
                "itemIndex": -1,
                "fieldIndex": 0,
                "traverseOffset": 0,
                "traverseLimit": 8,
            }
            single_specs.append(
                {"name": "localDirectory:traverse", "datasetId": dataset_id, "request": request}
            )
            batch_templates.append(request)
            continue

        if mode == "webdatasetDir":
            selected_shard = selection.get("selectedShardName")
            shards = details.get("shards") if isinstance(details.get("shards"), list) else []
            if not selected_shard and shards and isinstance(shards[0], dict):
                selected_shard = shards[0].get("filename")
            if isinstance(selected_shard, str) and selected_shard:
                request = {
                    "datasetId": dataset_id,
                    "shardName": selected_shard,
                    "itemIndex": 0,
                    "fieldIndex": 0,
                }
                single_specs.append(
                    {"name": "webdataset:direct", "datasetId": dataset_id, "request": request}
                )
                batch_templates.append(request)
            for shard in shards[:64]:
                if not isinstance(shard, dict):
                    continue
                filename = shard.get("filename")
                if not isinstance(filename, str) or not filename:
                    continue
                web_multi.append(
                    {
                        "datasetId": dataset_id,
                        "shardName": filename,
                        "itemIndex": 0,
                        "fieldIndex": 0,
                    }
                )
            continue

        if mode == "litdataIndex":
            chunks = details.get("chunks") if isinstance(details.get("chunks"), list) else []
            chunk_name = "chunk-0-0.bin"
            if chunks and isinstance(chunks[0], dict):
                candidate = chunks[0].get("filename")
                if isinstance(candidate, str) and candidate:
                    chunk_name = candidate
            direct = {
                "datasetId": dataset_id,
                "chunkName": chunk_name,
                "itemIndex": 0,
                "fieldIndex": 0,
            }
            traverse = {
                "datasetId": dataset_id,
                "chunkName": chunk_name,
                "traverse": True,
                "itemIndex": -1,
                "fieldIndex": 0,
                "traverseOffset": 0,
                "traverseLimit": 10,
            }
            single_specs.append({"name": "litdata:direct", "datasetId": dataset_id, "request": direct})
            single_specs.append(
                {"name": "litdata:traverse", "datasetId": dataset_id, "request": traverse}
            )
            batch_templates.append(direct)
            batch_templates.append(traverse)
            continue

        if mode == "huggingface":
            features = details.get("features") if isinstance(details.get("features"), list) else []
            field_name = "episode_id"
            if features and isinstance(features[0], dict):
                candidate = features[0].get("name")
                if isinstance(candidate, str) and candidate:
                    field_name = candidate
            config = details.get("config") or selection.get("selectedHfConfig") or "default"
            split = details.get("split") or selection.get("selectedHfSplit") or "train"
            request = {
                "datasetId": dataset_id,
                "config": config,
                "split": split,
                "rowIndex": 0,
                "fieldName": field_name,
            }
            single_specs.append({"name": "huggingface:direct", "datasetId": dataset_id, "request": request})
            batch_templates.append(request)

    single_results: list[dict[str, Any]] = []
    for spec in single_specs:
        request = spec.get("request")
        if not isinstance(request, dict):
            single_results.append(spec)
            continue
        dataset_id = spec["datasetId"]
        status, body, elapsed_ms = _http_json(
            base_url,
            "POST",
            f"/api/v1/opened/{dataset_id}/field",
            request,
            timeout=http_timeout,
        )
        ok = status == 200 and isinstance(body, dict) and body.get("ok") is True
        error_code = _error_code_from_body(body)
        item_count = (
            (body.get("data") or {}).get("itemCount") if isinstance(body, dict) else None
        )
        single_results.append(
            {
                "name": spec.get("name"),
                "datasetId": dataset_id,
                "status": status,
                "ok": ok,
                "errorCode": error_code,
                "itemCount": item_count,
                "ms": round(elapsed_ms, 1),
            }
        )

    return opened_record, single_results, batch_templates, web_multi


def _run_batch_soak(
    base_url: str,
    requests: list[dict[str, Any]],
    concurrency: int,
    rounds: int,
    http_timeout: int,
) -> dict[str, Any]:
    if rounds <= 0:
        return {
            "requestCount": len(requests),
            "rounds": rounds,
            "allRoundsFullSuccess": True,
            "totalFailed": 0,
            "successPerRound": [],
            "failPerRound": [],
            "errorHistogram": {},
            "errorByDataset": {},
            "errorSamples": [],
            "clientMs": {
                "min": 0.0,
                "p50": 0.0,
                "p95": 0.0,
                "max": 0.0,
                "mean": 0.0,
            },
            "skipped": "zero_rounds",
        }
    latencies: list[float] = []
    server_durations: list[float] = []
    success_by_round: list[int] = []
    fail_by_round: list[int] = []
    error_histogram: dict[str, int] = {}
    error_by_dataset: dict[str, int] = {}
    error_samples: list[dict[str, Any]] = []

    for _ in range(rounds):
        status, body, elapsed_ms = _http_json(
            base_url,
            "POST",
            "/api/v1/opened/field/batch",
            {"requests": requests, "concurrency": concurrency},
            timeout=http_timeout,
        )
        latencies.append(elapsed_ms)
        duration_ms = _meta_duration_ms(body)
        if duration_ms is not None:
            server_durations.append(duration_ms)

        entries = []
        if status == 200 and isinstance(body, dict):
            data = body.get("data")
            if isinstance(data, dict):
                value = data.get("requests")
                if isinstance(value, list):
                    entries = value

        ok_count = 0
        fail_count = 0

        if status != 200:
            fail_count = len(requests)
            key = f"HTTP_{status}"
            error_histogram[key] = error_histogram.get(key, 0) + 1
        else:
            for entry in entries:
                if isinstance(entry, dict) and entry.get("ok") is True:
                    ok_count += 1
                    continue
                fail_count += 1
                code = "UNKNOWN"
                if isinstance(entry, dict):
                    err = entry.get("error")
                    if isinstance(err, dict):
                        raw_code = err.get("code")
                        if isinstance(raw_code, str) and raw_code:
                            code = raw_code
                error_histogram[code] = error_histogram.get(code, 0) + 1
                dataset_id = "unknown"
                if isinstance(entry, dict):
                    raw_dataset_id = entry.get("datasetId")
                    if isinstance(raw_dataset_id, str) and raw_dataset_id:
                        dataset_id = raw_dataset_id
                key = f"{dataset_id}:{code}"
                error_by_dataset[key] = error_by_dataset.get(key, 0) + 1
                if len(error_samples) < 10 and isinstance(entry, dict):
                    err = entry.get("error")
                    if isinstance(err, dict):
                        error_samples.append(
                            {
                                "datasetId": dataset_id,
                                "code": code,
                                "message": err.get("message"),
                                "details": err.get("details"),
                            }
                        )

            if len(entries) < len(requests):
                missing = len(requests) - len(entries)
                fail_count += missing
                error_histogram["MISSING_RESULTS"] = (
                    error_histogram.get("MISSING_RESULTS", 0) + missing
                )
                error_by_dataset["unknown:MISSING_RESULTS"] = (
                    error_by_dataset.get("unknown:MISSING_RESULTS", 0) + missing
                )

        success_by_round.append(ok_count)
        fail_by_round.append(fail_count)

    result: dict[str, Any] = {
        "requestCount": len(requests),
        "rounds": rounds,
        "allRoundsFullSuccess": all(value == 0 for value in fail_by_round),
        "totalFailed": sum(fail_by_round),
        "successPerRound": success_by_round,
        "failPerRound": fail_by_round,
        "errorHistogram": error_histogram,
        "errorByDataset": error_by_dataset,
        "errorSamples": error_samples,
        "clientMs": {
            "min": round(min(latencies), 1),
            "p50": round(statistics.median(latencies), 1),
            "p95": round(_p95(latencies), 1),
            "max": round(max(latencies), 1),
            "mean": round(statistics.mean(latencies), 1),
        },
    }
    if server_durations:
        result["serverDurationMs"] = {
            "min": round(min(server_durations), 1),
            "p50": round(statistics.median(server_durations), 1),
            "p95": round(_p95(server_durations), 1),
            "max": round(max(server_durations), 1),
            "mean": round(statistics.mean(server_durations), 1),
        }
    return result


def _repeat_to_length(source: list[dict[str, Any]], target_len: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    while len(out) < target_len:
        out.extend(source)
    return out[:target_len]


def main() -> int:
    parser = argparse.ArgumentParser(description="Dataset Inspector API soak tester")
    parser.add_argument("--base-url", default="http://127.0.0.1:9292")
    parser.add_argument("--mixed-size", type=int, default=128)
    parser.add_argument("--mixed-concurrency", type=int, default=64)
    parser.add_argument("--mixed-rounds", type=int, default=20)
    parser.add_argument("--web-rounds", type=int, default=10)
    parser.add_argument(
        "--web-benchmark-concurrency",
        default="8,16,32,64",
        help="Comma-separated concurrency list for webdataset benchmark",
    )
    parser.add_argument(
        "--fail-on-any-failure",
        action="store_true",
        help="Return non-zero if any matrix/soak step has failure",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to write all emitted JSON records as an array",
    )
    parser.add_argument(
        "--http-timeout",
        type=int,
        default=1200,
        help="Per-request HTTP timeout in seconds",
    )
    parser.add_argument(
        "--exclude-dataset-id",
        default="",
        help="Comma-separated dataset IDs to exclude from test generation",
    )
    args = parser.parse_args()
    excluded_ids = {
        token.strip()
        for token in args.exclude_dataset_id.split(",")
        if token.strip()
    }

    records: list[dict[str, Any]] = []

    def emit(record: dict[str, Any]) -> None:
        print(json.dumps(record, ensure_ascii=False))
        records.append(record)

    opened_record, single_results, batch_templates, web_multi = _run_single_matrix(
        args.base_url, http_timeout=args.http_timeout, excluded_ids=excluded_ids
    )
    emit(opened_record)
    ok_count = sum(1 for row in single_results if row.get("ok") is True)
    emit(
        {
            "phase": "single_matrix",
            "total": len(single_results),
            "ok": ok_count,
            "fail": len(single_results) - ok_count,
        }
    )
    for row in single_results:
        emit({"single": row})

    failures = 0
    failures += len(single_results) - ok_count

    if batch_templates:
        mixed_requests = _repeat_to_length(batch_templates, args.mixed_size)
        mixed_summary = _run_batch_soak(
            args.base_url,
            mixed_requests,
            args.mixed_concurrency,
            args.mixed_rounds,
            args.http_timeout,
        )
        mixed_summary["phase"] = "mixed_soak"
        emit(mixed_summary)
        failures += int(mixed_summary.get("totalFailed", 0))
    else:
        emit({"phase": "mixed_soak", "skipped": "no_templates"})

    if web_multi:
        web_requests = web_multi[:64]
        web_soak = _run_batch_soak(
            args.base_url,
            web_requests,
            64,
            args.web_rounds,
            args.http_timeout,
        )
        web_soak["phase"] = "webdataset_64shards_soak"
        web_soak["requests"] = len(web_requests)
        emit(web_soak)
        failures += int(web_soak.get("totalFailed", 0))

        benchmark_values = []
        for token in args.web_benchmark_concurrency.split(","):
            token = token.strip()
            if not token:
                continue
            try:
                value = int(token)
            except ValueError:
                continue
            if value > 0:
                benchmark_values.append(value)
        if not benchmark_values:
            benchmark_values = [8, 16, 32, 64]

        benchmark_rows = []
        for concurrency in benchmark_values:
            row = _run_batch_soak(
                args.base_url,
                web_requests,
                concurrency,
                rounds=3,
                http_timeout=args.http_timeout,
            )
            row["concurrency"] = concurrency
            benchmark_rows.append(row)
            failures += int(row.get("totalFailed", 0))

        def _bench_key(item: dict[str, Any]) -> float:
            server = item.get("serverDurationMs")
            if isinstance(server, dict):
                value = server.get("p50")
                if isinstance(value, (int, float)):
                    return float(value)
            client = item.get("clientMs")
            if isinstance(client, dict):
                value = client.get("p50")
                if isinstance(value, (int, float)):
                    return float(value)
            return float("inf")

        best = min(benchmark_rows, key=_bench_key)
        emit(
            {
                "phase": "webdataset_concurrency_benchmark",
                "results": benchmark_rows,
                "best": best,
            }
        )
    else:
        emit({"phase": "webdataset_64shards_soak", "skipped": "no_webdataset_shards"})

    output_json_path = args.output_json.strip()
    if output_json_path:
        with open(output_json_path, "w", encoding="utf-8") as handle:
            json.dump(records, handle, ensure_ascii=False, indent=2)
            handle.write("\n")

    if args.fail_on_any_failure and failures > 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
