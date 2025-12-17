"use client";

import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  ArrowRight,
  BadgeInfo,
  ChevronLeft,
  ChevronRight,
  Copy,
  Database,
  FolderOpen,
  HardDrive,
  KeyRound,
  Loader2,
  Play,
  Sparkles,
  TriangleAlert,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  chooseIndexSource,
  chooseOpenerApp,
  clearHfToken,
  hfDatasetPreview,
  hfOpenField,
  isTauri,
  listChunkItems,
	  loadChunkList,
	  loadIndex,
	  openLeaf,
	  prepareAudioPreview,
	  openPathWithApp,
	  peekField,
	  readHfToken,
	  readPreferredOpenerForExt,
	  readLastIndex,
	  saveHfToken,
	  saveLastIndex,
	  savePreferredOpenerForExt,
	  toFileSrc,
	  type FieldPreview,
	  type HfDatasetPreview,
	  type HfFeature,
	  type IndexSummary,
	  type ItemMeta,
} from "@/lib/tauri-api";
import { cn } from "@/lib/utils";
import { useViewerStore } from "@/store/viewer";

const HF_PAGE_SIZE = 25;
const EMPTY_ROWS: unknown[] = [];
const EMPTY_HF_FEATURES: HfFeature[] = [];

const formatBytes = (value: number) => {
  if (!Number.isFinite(value)) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = value;
  let idx = 0;
  while (v >= 1024 && idx < units.length - 1) {
    v /= 1024;
    idx += 1;
  }
  return `${v.toFixed(v >= 10 || v < 1 ? 0 : 1)} ${units[idx]}`;
};

const looksLikeLocalPath = (value: string) => {
  const v = value.trim();
  if (!v) return false;
  if (v.startsWith("/") || v.startsWith("~") || v.startsWith("./") || v.startsWith("../")) return true;
  if (/^[A-Za-z]:[\\/]/.test(v)) return true;
  if (/\.(json|bin|zst)$/i.test(v)) return true;
  return false;
};

const looksLikeHfInput = (value: string) => {
  const v = value.trim();
  if (!v) return false;
  if (v.startsWith("hf://datasets/")) return true;
  if (v.startsWith("https://huggingface.co/datasets/") || v.startsWith("http://huggingface.co/datasets/")) return true;
  if (v.startsWith("https://hf.co/datasets/") || v.startsWith("http://hf.co/datasets/")) return true;
  return false;
};

const displayHfDatasetId = (value: string) => {
  const v = value.trim();
  if (!v) return null;
  if (v.startsWith("hf://datasets/")) {
    const rest = v.slice("hf://datasets/".length);
    const parts = rest.split("/").filter(Boolean);
    if (parts.length >= 2) return `${parts[0]}/${parts[1]}`;
    return null;
  }
  if (v.startsWith("https://huggingface.co/datasets/") || v.startsWith("http://huggingface.co/datasets/")) {
    try {
      const u = new URL(v);
      const segments = u.pathname.split("/").filter(Boolean);
      const idx = segments.indexOf("datasets");
      if (idx >= 0 && segments.length >= idx + 3) return `${segments[idx + 1]}/${segments[idx + 2]}`;
    } catch {
      // ignore
    }
    return null;
  }
  if (v.startsWith("https://hf.co/datasets/") || v.startsWith("http://hf.co/datasets/")) {
    try {
      const u = new URL(v);
      const segments = u.pathname.split("/").filter(Boolean);
      const idx = segments.indexOf("datasets");
      if (idx >= 0 && segments.length >= idx + 3) return `${segments[idx + 1]}/${segments[idx + 2]}`;
    } catch {
      // ignore
    }
    return null;
  }
  return null;
};

function safeJson(value: unknown) {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

function formatCell(value: unknown) {
  if (value === null || value === undefined) return "null";
  if (typeof value === "string") return value;
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  const json = safeJson(value);
  return json.length > 220 ? `${json.slice(0, 220)}…` : json;
}

function featureLabel(feature: HfFeature) {
  const dtype = (feature.dtype ?? "").trim();
  return dtype ? `${feature.name} (${dtype})` : feature.name;
}

function extFromUrl(input: string) {
  try {
    const url = new URL(input);
    const name = url.pathname.split("/").pop() ?? "";
    const ext = name.includes(".") ? name.split(".").pop() : "";
    const cleaned = String(ext ?? "").trim().toLowerCase();
    return cleaned || null;
  } catch {
    return null;
  }
}

function guessHfFieldExt(value: unknown) {
  if (value && typeof value === "object" && !Array.isArray(value)) {
    const obj = value as Record<string, unknown>;
    const src = typeof obj.src === "string" ? obj.src.trim() : "";
    const fromSrc = src ? extFromUrl(src) : null;
    if (fromSrc) return fromSrc;
    return "json";
  }
  if (typeof value === "string") return "txt";
  return "json";
}

export default function Page() {
  const {
    sourceInput,
    setSourceInput,
    chunkSelection,
    setChunkSelection,
    mode,
    triggerLoad,
    selectedChunkName,
    selectChunk,
    selectedItemIndex,
    selectItem,
    selectedFieldIndex,
    selectField,
    hfConfigOverride,
    hfSplitOverride,
    hfOffset,
    setHfOffset,
    setHfConfigSplit,
    hfSelectedRowIndex,
    selectHfRow,
    hfSelectedFieldName,
    selectHfField,
    statusMessage,
    setStatusMessage,
  } = useViewerStore();

  const [hfToken, setHfToken] = useState<string | null>(null);
  const hfTokenMasked = hfToken ? `…${hfToken.slice(-6)}` : null;
  const [hfTokenDraft, setHfTokenDraft] = useState("");
  const [hfTokenDialogOpen, setHfTokenDialogOpen] = useState(false);
  const [hfOffsetDraft, setHfOffsetDraft] = useState(String(hfOffset));

  const isLitdataMode = mode?.kind === "litdata-index" || mode?.kind === "litdata-chunks";
  const isHfMode = mode?.kind === "huggingface";
  const autodetectedHf = looksLikeHfInput(sourceInput) && chunkSelection.length === 0;

  const latestSourceInputRef = useRef(sourceInput);
  useEffect(() => {
    latestSourceInputRef.current = sourceInput;
  }, [sourceInput]);

  useEffect(() => {
    setHfOffsetDraft(String(hfOffset));
  }, [hfOffset]);

  useEffect(() => {
    if (!isTauri()) return;
    void readLastIndex().then((last) => {
      if (!last) return;
      if (!latestSourceInputRef.current.trim()) setSourceInput(last);
    });
  }, [setSourceInput]);

  useEffect(() => {
    if (!isTauri()) return;
    void readHfToken().then((token) => setHfToken(token));
  }, []);
  useEffect(() => {
    setHfTokenDraft("");
  }, [hfToken]);

  const indexQuery = useQuery<IndexSummary>({
    queryKey: ["index-summary", mode?.requestId ?? 0],
    enabled: Boolean(isLitdataMode),
    queryFn: () => {
      if (!mode) throw new Error("No source selected.");
      if (mode.kind === "litdata-index") return loadIndex(mode.indexPath);
      if (mode.kind === "litdata-chunks") return loadChunkList(mode.paths);
      throw new Error("Not a LitData mode.");
    },
  });

  const hfQuery = useQuery<HfDatasetPreview>({
    queryKey: [
      "hf-preview",
      isHfMode ? mode.input : null,
      hfConfigOverride,
      hfSplitOverride,
      hfOffset,
      HF_PAGE_SIZE,
      hfTokenMasked,
    ],
    enabled: Boolean(isHfMode && isTauri()),
    queryFn: () => {
      if (!mode || mode.kind !== "huggingface") throw new Error("No dataset selected.");
      return hfDatasetPreview({
        input: mode.input,
        config: hfConfigOverride ?? undefined,
        split: hfSplitOverride ?? undefined,
        offset: hfOffset,
        length: HF_PAGE_SIZE,
        token: hfToken,
      });
    },
    staleTime: 60 * 1000,
  });

  useEffect(() => {
    if (mode?.kind === "litdata-index" && indexQuery.data?.indexPath) {
      void saveLastIndex(indexQuery.data.indexPath);
    }
  }, [indexQuery.data?.indexPath, mode?.kind]);

  useEffect(() => {
    if (indexQuery.data) {
      setStatusMessage(`Loaded ${indexQuery.data.chunks.length} chunk${indexQuery.data.chunks.length === 1 ? "" : "s"}.`);
    }
  }, [indexQuery.data, setStatusMessage]);

  useEffect(() => {
    if (hfQuery.data) {
      const suffix = hfQuery.data.partial ? " (partial)" : "";
      setStatusMessage(`Loaded ${hfQuery.data.dataset} · ${hfQuery.data.config}/${hfQuery.data.split}${suffix}.`);
    }
  }, [hfQuery.data, setStatusMessage]);

  useEffect(() => {
    if (indexQuery.data && mode?.kind === "litdata-index" && chunkSelection.length) {
      setChunkSelection([]);
    }
  }, [chunkSelection.length, indexQuery.data, mode?.kind, setChunkSelection]);

  useEffect(() => {
    if (!indexQuery.data || !isLitdataMode) {
      selectChunk(null);
      return;
    }
    const nextChunk =
      indexQuery.data.chunks.find((chunk) => chunk.filename === selectedChunkName)?.filename ||
      indexQuery.data.chunks[0]?.filename ||
      null;
    if (nextChunk !== selectedChunkName) {
      selectChunk(nextChunk);
    }
  }, [indexQuery.data, isLitdataMode, selectChunk, selectedChunkName]);

  const selectedChunk = useMemo(
    () => indexQuery.data?.chunks.find((chunk) => chunk.filename === selectedChunkName) ?? null,
    [indexQuery.data, selectedChunkName],
  );

  const itemsQuery = useQuery<ItemMeta[]>({
    queryKey: ["chunk-items", indexQuery.data?.indexPath, selectedChunk?.filename],
    enabled: Boolean(isLitdataMode && indexQuery.data && selectedChunk && !indexQuery.isFetching),
    queryFn: () =>
      listChunkItems({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
      }),
    staleTime: 5 * 60 * 1000,
  });

  useEffect(() => {
    if (!isLitdataMode) return;
    const items = itemsQuery.data ?? [];
    if (!items.length) {
      selectItem(null);
      return;
    }
    const exists = items.some((item) => item.itemIndex === selectedItemIndex);
    if (!exists) {
      selectItem(items[0].itemIndex);
    }
  }, [isLitdataMode, itemsQuery.data, selectItem, selectedItemIndex]);

  const selectedItem = useMemo(
    () => itemsQuery.data?.find((item) => item.itemIndex === selectedItemIndex) ?? null,
    [itemsQuery.data, selectedItemIndex],
  );

  useEffect(() => {
    if (!isLitdataMode) return;
    if (!selectedItem) {
      selectField(null);
      return;
    }
    const exists = selectedItem.fields.some((field) => field.fieldIndex === selectedFieldIndex);
    if (!exists) {
      selectField(selectedItem.fields[0]?.fieldIndex ?? null);
    }
  }, [isLitdataMode, selectField, selectedFieldIndex, selectedItem]);

  const selectedField = useMemo(
    () => selectedItem?.fields.find((field) => field.fieldIndex === selectedFieldIndex) ?? null,
    [selectedFieldIndex, selectedItem],
  );

  const previewQuery = useQuery<FieldPreview>({
    queryKey: [
      "field-preview",
      indexQuery.data?.indexPath,
      selectedChunk?.filename,
      selectedItem?.itemIndex,
      selectedField?.fieldIndex,
    ],
    enabled: Boolean(isLitdataMode && indexQuery.data && selectedChunk && selectedItem && selectedField && !itemsQuery.isFetching),
    queryFn: () =>
      peekField({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
        itemIndex: selectedItem?.itemIndex ?? 0,
        fieldIndex: selectedField?.fieldIndex ?? 0,
      }),
    staleTime: 60 * 1000,
  });

  const openWithAppMutation = useMutation({
    mutationFn: (params: { path: string; appPath: string }) => openPathWithApp(params),
    onSuccess: (message) => setStatusMessage(message),
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected file with the chosen app."),
  });

  const openFieldMutation = useMutation({
    mutationFn: async () => {
      if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) {
        throw new Error("Select a field to open.");
      }
      const guessedExt = (previewQuery.data?.guessedExt ?? "").trim().replace(/^\\./, "");
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      return openLeaf({
        indexPath: indexQuery.data.indexPath,
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItem.itemIndex,
        fieldIndex: selectedField.fieldIndex,
        openerAppPath,
      });
    },
    onSuccess: (result) => {
      setStatusMessage(result.message);
      if (!result.needsOpener) return;
      void (async () => {
        const picked = await chooseOpenerApp();
        if (!picked) return;
        const extLabel = (result.ext ?? "").trim().replace(/^\\./, "") || "bin";
        const remember = window.confirm(`Remember this app for .${extLabel} files?`);
        if (remember) {
          await savePreferredOpenerForExt(extLabel, picked);
        }
        openWithAppMutation.mutate({ path: result.path, appPath: picked });
      })();
    },
    onError: (err: unknown) => setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected field."),
  });

  const localAudioPreviewMutation = useMutation({
    mutationFn: async () => {
      if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) {
        throw new Error("Select an audio field to preview.");
      }
      return prepareAudioPreview({
        indexPath: indexQuery.data.indexPath,
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItem.itemIndex,
        fieldIndex: selectedField.fieldIndex,
      });
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to prepare the audio preview."),
  });

  const copyText = async (text: string) => {
    const trimmed = text.trim();
    if (!trimmed) return;
    try {
      await navigator.clipboard.writeText(trimmed);
      setStatusMessage("Copied to clipboard.");
      return;
    } catch {
      // Fall back to execCommand for older WebViews / permissions.
    }

    try {
      const textarea = document.createElement("textarea");
      textarea.value = trimmed;
      textarea.setAttribute("readonly", "true");
      textarea.style.position = "fixed";
      textarea.style.top = "-9999px";
      textarea.style.left = "-9999px";
      document.body.appendChild(textarea);
      textarea.select();
      document.execCommand("copy");
      document.body.removeChild(textarea);
      setStatusMessage("Copied to clipboard.");
    } catch {
      setStatusMessage("Copy failed.");
    }
  };

  const handleLoad = () => {
    setStatusMessage(null);
    if (chunkSelection.length > 0) {
      triggerLoad("litdata-chunks", chunkSelection);
      return;
    }
    const trimmed = sourceInput.trim();
    if (!trimmed) return;
    if (looksLikeHfInput(trimmed)) {
      triggerLoad("huggingface", trimmed);
      return;
    }
    triggerLoad("litdata-index");
  };

  const handleChoose = async () => {
    try {
      const pick = await chooseIndexSource(sourceInput, indexQuery.data?.rootDir);
      if (!pick) return;
      setStatusMessage(null);
      setSourceInput(pick.indexPath);
      setChunkSelection([]);
      triggerLoad("litdata-index");
    } catch (err) {
      setStatusMessage(err instanceof Error ? err.message : String(err));
    }
  };

  const totalBytes =
    indexQuery.data?.chunks?.reduce(
      (acc, chunk) => acc + (Number.isFinite(chunk.chunkBytes) ? chunk.chunkBytes : 0),
      0,
    ) ?? 0;

  const totalItems =
    indexQuery.data?.chunks?.reduce(
      (acc, chunk) => acc + (Number.isFinite(chunk.chunkSize) ? chunk.chunkSize : 0),
      0,
    ) ?? 0;

  const hfSplitPairs = useMemo(() => {
    const configs = hfQuery.data?.configs ?? [];
    return configs.flatMap((c) => c.splits.map((s) => ({ config: c.config, split: s })));
  }, [hfQuery.data?.configs]);

  const hfSelectedPairKey = hfQuery.data ? `${hfQuery.data.config}:${hfQuery.data.split}` : null;

  const hfRows = useMemo(() => hfQuery.data?.rows ?? EMPTY_ROWS, [hfQuery.data?.rows]);
  const derivedSelectedRowIndex =
    hfSelectedRowIndex !== null && hfSelectedRowIndex >= hfOffset && hfSelectedRowIndex < hfOffset + hfRows.length
      ? hfSelectedRowIndex
      : hfRows.length
        ? hfOffset
        : null;
  const hfSelectedRow =
    derivedSelectedRowIndex === null ? null : (hfRows[derivedSelectedRowIndex - hfOffset] as unknown);

  const hfFeatures = useMemo(() => hfQuery.data?.features ?? EMPTY_HF_FEATURES, [hfQuery.data?.features]);
  const derivedSelectedFieldName = useMemo(() => {
    const names = new Set(hfFeatures.map((f) => f.name));
    if (hfSelectedFieldName && names.has(hfSelectedFieldName)) return hfSelectedFieldName;
    return hfFeatures[0]?.name ?? null;
  }, [hfFeatures, hfSelectedFieldName]);

  const hfSelectedValue = useMemo(() => {
    if (!hfSelectedRow || !derivedSelectedFieldName) return null;
    if (typeof hfSelectedRow !== "object" || hfSelectedRow === null) return null;
    const rowObj = hfSelectedRow as Record<string, unknown>;
    return rowObj[derivedSelectedFieldName] ?? null;
  }, [derivedSelectedFieldName, hfSelectedRow]);

  const hfOpenMutation = useMutation({
    mutationFn: async (fieldName: string) => {
      if (!mode || mode.kind !== "huggingface") {
        throw new Error("Select a Hugging Face dataset to open.");
      }
      const rowIndex = derivedSelectedRowIndex;
      if (rowIndex === null) {
        throw new Error("Select a row to open a field.");
      }
      if (!hfQuery.data) {
        throw new Error("Dataset not loaded.");
      }
      const value =
        hfSelectedRow && typeof hfSelectedRow === "object" && hfSelectedRow !== null
          ? (hfSelectedRow as Record<string, unknown>)[fieldName]
          : null;
      const ext = guessHfFieldExt(value);
      const openerAppPath = ext ? await readPreferredOpenerForExt(ext) : null;
      return hfOpenField({
        input: mode.input,
        config: hfQuery.data.config,
        split: hfQuery.data.split,
        rowIndex,
        fieldName,
        openerAppPath,
        token: hfToken,
      });
    },
    onSuccess: (result) => {
      setStatusMessage(result.message);
      if (!result.needsOpener) return;
      void (async () => {
        const picked = await chooseOpenerApp();
        if (!picked) return;
        const extLabel = (result.ext ?? "").trim().replace(/^\./, "") || "bin";
        const remember = window.confirm(`Remember this app for .${extLabel} files?`);
        if (remember) {
          await savePreferredOpenerForExt(extLabel, picked);
        }
        openWithAppMutation.mutate({ path: result.path, appPath: picked });
      })();
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected Hugging Face field."),
  });

  const canPaginateHf = Boolean(hfQuery.data && !hfQuery.isFetching);
  const hfCanPrev = canPaginateHf && hfOffset > 0;
  const hfCanNext =
    canPaginateHf &&
    (hfQuery.data?.partial ? hfRows.length === HF_PAGE_SIZE : hfOffset + hfRows.length < (hfQuery.data?.numRowsTotal ?? 0));

  const handleHfJump = () => {
    const raw = hfOffsetDraft.trim();
    if (!raw) {
      setHfOffsetDraft(String(hfOffset));
      return;
    }
    const parsed = Number.parseInt(raw, 10);
    if (!Number.isFinite(parsed)) {
      setHfOffsetDraft(String(hfOffset));
      return;
    }
    const max = Math.max(0, (hfQuery.data?.numRowsTotal ?? 0) - 1);
    const clamped = Math.min(Math.max(0, parsed), max);
    setHfOffsetDraft(String(clamped));
    setHfOffset(clamped);
    selectHfRow(null);
  };

  const busy =
    indexQuery.isFetching ||
    itemsQuery.isFetching ||
    previewQuery.isFetching ||
    hfQuery.isFetching ||
    hfOpenMutation.isPending ||
    openFieldMutation.isPending ||
    localAudioPreviewMutation.isPending ||
    openWithAppMutation.isPending;
  const latestError =
    indexQuery.error ||
    itemsQuery.error ||
    previewQuery.error ||
    hfQuery.error ||
    hfOpenMutation.error ||
    openFieldMutation.error ||
    localAudioPreviewMutation.error ||
    openWithAppMutation.error ||
    undefined;
  const errorMessage = useMemo(() => {
    if (!latestError) return null;
    if (latestError instanceof Error) return latestError.message;
    if (typeof latestError === "string") return latestError;
    if (typeof latestError === "object" && latestError !== null) {
      const maybe = latestError as Record<string, unknown>;
      if (typeof maybe.message === "string" && maybe.message.trim()) return maybe.message;
      if (typeof maybe.error === "string" && maybe.error.trim()) return maybe.error;
      if (typeof maybe.code === "string" && typeof maybe.message === "string") return `${maybe.code}: ${maybe.message}`;
      try {
        return JSON.stringify(latestError);
      } catch {
        return String(latestError);
      }
    }
    return String(latestError);
  }, [latestError]);
  const authHint =
    errorMessage && errorMessage.toLowerCase().includes("authentication")
      ? "This dataset may be private or gated. Set a Hugging Face access token to continue."
      : null;
  const logMessage = errorMessage ? `${errorMessage}${authHint ? `\n${authHint}` : ""}` : statusMessage ?? "Idle";

  const loadIcon = autodetectedHf ? <Database className="mr-2 h-4 w-4" /> : <HardDrive className="mr-2 h-4 w-4" />;
  const loadLabel = "Load";
  const showHfStats = autodetectedHf || isHfMode;
  const datasetPreviewLabel = hfQuery.data?.dataset ?? (showHfStats ? displayHfDatasetId(sourceInput) ?? "—" : "—");
  const localFieldKey = useMemo(() => {
    if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) return null;
    return `${indexQuery.data.indexPath}|${selectedChunk.filename}|${selectedItem.itemIndex}|${selectedField.fieldIndex}`;
  }, [indexQuery.data, selectedChunk, selectedField, selectedItem]);
  return (
    <main className="h-screen w-screen overflow-hidden bg-transparent">
      <div className="mx-auto flex h-full max-w-screen-2xl flex-col gap-4 px-3 pb-3 pt-4">
        <section className="relative overflow-hidden rounded-[24px] border border-white/60 bg-white/70 p-5 shadow-lg backdrop-blur">
          <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,#d7fce9_0%,#e0f6ff_60%,#f3f7ff_100%)]" />
          <div className="relative grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
            <div className="space-y-3">
              <h1 className="text-3xl font-semibold uppercase text-slate-900">Dataset Inspector</h1>
              <p className="text-sm text-slate-600">
                Choose a local <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">LitData</code> path, or a
                Hugging Face dataset URL, or{" "}
                <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">hf://datasets/...</code>.
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  className="w-full min-w-[280px] rounded-full border-slate-200 bg-white/70 backdrop-blur sm:max-w-[420px]"
                  placeholder="/abs/path/to/index.json  OR  https://huggingface.co/datasets/<namespace>/<dataset-name>"
                  value={sourceInput}
                  onChange={(e) => setSourceInput(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Enter") handleLoad();
                  }}
                  aria-label="Source"
                />
                <Button
                  variant="outline"
                  className="border-emerald-200 bg-white/80 text-emerald-700 hover:bg-emerald-50"
                  onClick={handleChoose}
                  disabled={busy}
                >
                  <FolderOpen className="mr-2 h-4 w-4" />
                  Browse
                </Button>
                <Button onClick={handleLoad} disabled={busy || (!sourceInput.trim() && chunkSelection.length === 0) || !isTauri()}>
                  {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : loadIcon}
                  {loadLabel}
                </Button>
              </div>

              <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                <Badge variant="secondary" className="bg-slate-100/80">
                  {autodetectedHf ? "Auto: Hugging Face" : "Auto: Local"}
                </Badge>
                {chunkSelection.length > 0 ? (
                  <Badge variant="secondary" className="bg-slate-100/80">
                    {chunkSelection.length} selected chunk{chunkSelection.length > 1 ? "s" : ""}
                  </Badge>
                ) : null}
                {chunkSelection.length > 0 ? (
                  <span className="truncate">
                    {chunkSelection
                      .slice(0, 3)
                      .map((p) => p.split(/[\\/]/).pop() ?? p)
                      .join(" · ")}
                    {chunkSelection.length > 3 ? " …" : ""}
                  </span>
                ) : null}
                {chunkSelection.length > 0 ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    className="h-7 px-2 text-[11px] font-semibold text-slate-600 hover:bg-slate-100"
                    onClick={() => setChunkSelection([])}
                    disabled={busy}
                  >
                    Clear
                  </Button>
                ) : null}
              </div>

              {!isTauri() ? (
                <div className="flex items-center gap-2 text-xs text-amber-700">
                  <TriangleAlert className="h-4 w-4" />
                  Loading requires the Tauri runtime.
                </div>
              ) : null}
            </div>

            <div className="grid gap-3 sm:grid-cols-3">
              {showHfStats ? (
                <>
	                  <StatPill
	                    label="Dataset"
	                    value={datasetPreviewLabel}
	                    className="min-h-[132px]"
	                    footer={
	                      <Button
	                        type="button"
	                        size="sm"
	                        variant="outline"
	                        className="h-8 w-full max-w-full justify-start overflow-hidden rounded-full border-slate-200 bg-white/80 text-slate-700 hover:bg-slate-50"
	                        disabled={!isTauri()}
	                        onClick={() => {
	                          setHfTokenDraft(hfToken ?? "");
	                          setHfTokenDialogOpen(true);
	                        }}
	                      >
	                        <KeyRound className="mr-2 h-4 w-4" />
	                        <span className="truncate">{hfTokenMasked ? `HF Token ${hfTokenMasked}` : "HF Token"}</span>
	                      </Button>
	                    }
	                  />
                  <StatPill label="Split" value={hfQuery.data ? `${hfQuery.data.config}/${hfQuery.data.split}` : "—"} />
                  <StatPill label="Rows" value={hfQuery.data?.numRowsTotal ?? "—"} />
                </>
              ) : (
                <>
                  <StatPill label="Chunks" value={indexQuery.data?.chunks.length ?? "—"} />
                  <StatPill label="Items" value={totalItems ? totalItems.toLocaleString() : "—"} />
                  <StatPill label="Size" value={totalBytes ? formatBytes(totalBytes) : "—"} />
                </>
              )}
            </div>
          </div>
        </section>

        {hfTokenDialogOpen ? (
          <div
            className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 p-4"
            onMouseDown={(e) => {
              if (e.target === e.currentTarget) setHfTokenDialogOpen(false);
            }}
          >
	            <div className="w-full max-w-md rounded-[18px] border border-slate-200 bg-white p-4 shadow-xl">
	              <div className="flex items-start justify-between gap-3">
	                <div>
	                  <div className="text-sm font-semibold text-slate-900">Hugging Face Token</div>
	                  <div className="text-xs text-slate-500">
	                    Saved locally on this device. Required for private or gated datasets.
	                  </div>
	                </div>
	              </div>

              <div className="mt-3 space-y-2">
                <Input
                  type="password"
                  className="rounded-xl select-text cursor-text"
                  placeholder={hfTokenMasked ? `Token saved (${hfTokenMasked})` : "Paste token here"}
                  value={hfTokenDraft}
                  onChange={(e) => setHfTokenDraft(e.target.value)}
                  onKeyDown={(e) => {
                    if (e.key === "Escape") setHfTokenDialogOpen(false);
                    if (e.key !== "Enter") return;
                    const trimmed = hfTokenDraft.trim();
                    void (async () => {
                      if (!trimmed) {
                        await clearHfToken();
                        setHfToken(null);
                      } else {
                        await saveHfToken(trimmed);
                        setHfToken(trimmed);
                      }
                      setHfTokenDialogOpen(false);
                    })();
                  }}
                  autoFocus
                  aria-label="Hugging Face token"
                />

	                <div className="flex items-center justify-end gap-2">
	                  {hfToken ? (
	                    <Button
	                      size="sm"
	                      variant="outline"
                      onClick={() => {
                        void (async () => {
                          await clearHfToken();
                          setHfToken(null);
                          setHfTokenDialogOpen(false);
                        })();
                      }}
                    >
	                      Clear
	                    </Button>
	                  ) : null}
	                  <Button size="sm" variant="outline" onClick={() => setHfTokenDialogOpen(false)}>
	                    Close
	                  </Button>
	                  <Button
	                    size="sm"
	                    onClick={() => {
	                      const trimmed = hfTokenDraft.trim();
                      void (async () => {
                        if (!trimmed) {
                          await clearHfToken();
                          setHfToken(null);
                        } else {
                          await saveHfToken(trimmed);
                          setHfToken(trimmed);
                        }
                        setHfTokenDialogOpen(false);
                      })();
                    }}
                  >
                    Save
                  </Button>
                </div>
              </div>
            </div>
          </div>
        ) : null}

        <div className="grid flex-1 min-h-0 gap-4 lg:grid-cols-3">
          {isHfMode ? (
            <>
              <DataCard title="Splits" icon={<Database className="h-4 w-4 text-emerald-600" />} footerHint="Select a split.">
                <div className="flex h-full flex-col min-h-0">
                  <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                    {hfSplitPairs.map((pair) => {
                      const key = `${pair.config}:${pair.split}`;
                      const selected = key === hfSelectedPairKey;
                      return (
                        <div
                          key={key}
                          className={cn(
                            "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-4 py-3 transition",
                            selected ? "border-l-[3px] border-l-emerald-500 bg-emerald-50/70" : "hover:bg-slate-50",
                          )}
                          onClick={() => setHfConfigSplit(pair.config, pair.split)}
                        >
                          <div className="font-semibold text-slate-900">{`${pair.config}/${pair.split}`}</div>
                        </div>
                      );
                    })}
                    {hfQuery.isPending ? (
                      <div className="p-4">
                        <Skeleton className="h-10 w-full" />
                      </div>
                    ) : null}
                    {!hfSplitPairs.length && !hfQuery.isPending ? <EmptyState hint="Load a dataset to list its splits." /> : null}
                  </ScrollArea>
                </div>
              </DataCard>

              <DataCard
                title="Rows"
                icon={<HardDrive className="h-4 w-4 text-sky-600" />}
                footerHint="Pick a row to inspect its fields."
              >
                <div className="flex h-full flex-col space-y-3 min-h-0">
                  <div className="flex flex-wrap items-center gap-2">
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={!hfCanPrev}
                      onClick={() => setHfOffset(Math.max(0, hfOffset - HF_PAGE_SIZE))}
                    >
                      <ChevronLeft className="mr-1 h-4 w-4" />
                      Prev
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={!hfCanNext}
                      onClick={() => setHfOffset(hfOffset + HF_PAGE_SIZE)}
                    >
                      Next
                      <ChevronRight className="ml-1 h-4 w-4" />
                    </Button>
                    <Badge variant="secondary" className="bg-slate-100/80">
                      <span className="mr-1">offset</span>
                      <input
                        type="text"
                        inputMode="numeric"
                        pattern="[0-9]*"
                        className="w-20 bg-transparent p-0 text-xs font-semibold text-slate-800 outline-none select-text cursor-text focus:bg-white/40 rounded-sm [appearance:textfield]"
                        value={hfOffsetDraft}
                        onChange={(e) => setHfOffsetDraft(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") {
                            (e.currentTarget as HTMLInputElement).blur();
                            handleHfJump();
                          }
                        }}
                        onBlur={handleHfJump}
                        disabled={!canPaginateHf}
                        aria-label="Offset"
                      />
                    </Badge>
                    {hfQuery.data?.partial ? (
                      <Badge variant="secondary" className="bg-amber-100/80 text-amber-800">
                        Partial
                      </Badge>
                    ) : null}
                  </div>

                  <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                    {hfRows.map((row, idx) => {
                      const rowIndex = hfOffset + idx;
                      const selected = rowIndex === derivedSelectedRowIndex;
                      const rowObj = (row ?? {}) as Record<string, unknown>;
                      const firstCol = hfFeatures[0]?.name;
                      const snippet = firstCol ? formatCell(rowObj[firstCol]) : formatCell(row);
                      return (
                        <div
                          key={rowIndex}
                          className={cn(
                            "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-4 py-3 transition",
                            selected ? "border-l-[3px] border-l-sky-500 bg-sky-50/70" : "hover:bg-slate-50",
                          )}
                          onClick={() => selectHfRow(rowIndex)}
                        >
                          <div className="font-semibold text-slate-900">Row {rowIndex}</div>
                          <div className="max-w-[18rem] truncate text-xs text-slate-600">{snippet}</div>
                        </div>
                      );
                    })}
                    {hfQuery.isPending ? (
                      <div className="p-4">
                        <Skeleton className="h-10 w-full" />
                      </div>
                    ) : null}
                    {!hfRows.length && !hfQuery.isPending ? <EmptyState hint="Load a split to list its rows." /> : null}
                  </ScrollArea>
                </div>
              </DataCard>

              <DataCard
                title="Fields"
                icon={<Play className="h-4 w-4 text-cyan-600" />}
                footerHint="Pick a field to preview its value."
              >
                <div className="flex h-full flex-col space-y-3 min-h-0">
                  <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                    {hfFeatures.map((feature) => {
                      const selected = feature.name === derivedSelectedFieldName;
                      return (
                        <div
                          key={feature.name}
                          className={cn(
                            "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-slate-100 px-4 py-3 transition",
                            selected ? "border-l-[3px] border-l-cyan-500 bg-cyan-50/70" : "hover:bg-slate-50",
                          )}
                          onClick={() => selectHfField(feature.name)}
                          onDoubleClick={() => {
                            if (!hfSelectedRow || busy) return;
                            hfOpenMutation.mutate(feature.name);
                          }}
                        >
                          <div className="font-semibold text-slate-900">{featureLabel(feature)}</div>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="hover:bg-cyan-50"
                            disabled={busy || !hfSelectedRow}
                            onClick={(e) => {
                              e.stopPropagation();
                              hfOpenMutation.mutate(feature.name);
                            }}
                          >
                            <Play className="mr-1 h-4 w-4" />
                            Open
                          </Button>
                        </div>
                      );
                    })}
                    {hfQuery.isPending ? (
                      <div className="p-4">
                        <Skeleton className="h-10 w-full" />
                      </div>
                    ) : null}
                    {!hfFeatures.length && !hfQuery.isPending ? <EmptyState hint="Load a dataset to list its fields." /> : null}
                  </ScrollArea>

                  <div className="rounded-[18px] border border-dashed border-slate-200/90 bg-white/80 p-3 shadow-inner">
                    {hfQuery.isPending ? (
                      <Skeleton className="h-20 w-full" />
                    ) : hfSelectedRow && derivedSelectedFieldName ? (
                      <JsonPreviewPanel
                        title={`${derivedSelectedFieldName}`}
                        value={hfSelectedValue}
                        onCopy={() => copyText(safeJson(hfSelectedValue))}
                      />
                    ) : (
                      <div className="flex items-center gap-2 text-xs text-slate-500">
                        <TriangleAlert className="h-4 w-4" />
                        Pick a row and field to preview.
                      </div>
                    )}
                  </div>

                  <div className="flex items-center gap-2 text-sm">
                    {busy ? <Loader2 className="h-4 w-4 animate-spin text-emerald-600" /> : null}
                    {errorMessage ? <TriangleAlert className="h-4 w-4 text-amber-500" /> : null}
                    <span
                      className={cn(
                        "select-text cursor-text whitespace-pre-wrap",
                        errorMessage ? "text-amber-700" : "text-slate-600",
                      )}
                    >
                      {logMessage}
                    </span>
                  </div>
                </div>
              </DataCard>
            </>
          ) : (
            <>
              <DataCard title="Chunks" icon={<HardDrive className="h-4 w-4 text-emerald-600" />} footerHint="Pick a chunk to list its items.">
                <div className="flex h-full flex-col">
                  <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                    {(indexQuery.data?.chunks ?? []).map((chunk) => (
                      <div
                        key={chunk.filename}
                        className={cn(
                          "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-4 py-3 transition",
                          selectedChunk?.filename === chunk.filename
                            ? "border-l-[3px] border-l-emerald-500 bg-emerald-50/70"
                            : "hover:bg-slate-50",
                        )}
                        onClick={() => selectChunk(chunk.filename)}
                      >
                        <div className="font-semibold text-slate-900">{chunk.filename}</div>
                        <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                          <Badge variant="secondary">{chunk.chunkSize} items</Badge>
                          <Badge variant="secondary">{formatBytes(chunk.chunkBytes)}</Badge>
                        </div>
                      </div>
                    ))}
                    {!indexQuery.data?.chunks?.length ? <EmptyState hint="Load a LitData index/chunk to list shards." /> : null}
                  </ScrollArea>
                </div>
              </DataCard>

              <DataCard title="Items" icon={<BadgeInfo className="h-4 w-4 text-sky-600" />} footerHint="Pick an item to inspect its leaves.">
                <div className="flex h-full flex-col">
                  <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                    {(itemsQuery.data ?? []).map((item) => (
                      <div
                        key={item.itemIndex}
                        className={cn(
                          "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-4 py-3 transition",
                          selectedItem?.itemIndex === item.itemIndex ? "border-l-[3px] border-l-sky-500 bg-sky-50/70" : "hover:bg-slate-50",
                        )}
                        onClick={() => selectItem(item.itemIndex)}
                      >
                        <div className="font-semibold text-slate-900">Item {item.itemIndex}</div>
                        <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                          <Badge variant="secondary">{formatBytes(item.totalBytes)}</Badge>
                          <Badge variant="secondary">{item.fields.length} leaves</Badge>
                        </div>
                      </div>
                    ))}
                    {!itemsQuery.data?.length ? <EmptyState hint="Pick a chunk to list its items." /> : null}
                  </ScrollArea>
                </div>
              </DataCard>

              <DataCard title="Fields" icon={<Play className="h-4 w-4 text-cyan-600" />} footerHint="Double-click a field to open with your default app.">
                <div className="flex h-full flex-col space-y-3 min-h-0">
                  {selectedItem ? (
                    <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                      {(selectedItem.fields ?? []).map((field) => (
                        <div
                          key={field.fieldIndex}
                          className={cn(
                            "grid cursor-pointer grid-cols-[1fr_auto_auto] items-center gap-2 border-b border-slate-100 px-4 py-3 transition",
                            selectedField?.fieldIndex === field.fieldIndex
                              ? "border-l-[3px] border-l-cyan-500 bg-cyan-50/70"
                              : "hover:bg-slate-50",
                          )}
                          onClick={() => selectField(field.fieldIndex)}
                          onDoubleClick={() => openFieldMutation.mutate()}
                        >
                          <div className="font-semibold text-slate-900">
                            #{field.fieldIndex} · {indexQuery.data?.dataFormat[field.fieldIndex] ?? "unknown"}
                          </div>
                          <div className="text-xs text-slate-500">{formatBytes(field.size)}</div>
                          <Button
                            size="sm"
                            variant="ghost"
                            className="hover:bg-cyan-50"
                            disabled={busy}
                            onClick={(e) => {
                              e.stopPropagation();
                              openFieldMutation.mutate();
                            }}
                          >
                            <Play className="mr-1 h-4 w-4" />
                            Open
                          </Button>
                        </div>
                      ))}
                    </ScrollArea>
                  ) : (
                    <EmptyState hint="Select an item to see its fields." />
                  )}

                  <div className="rounded-[18px] border border-dashed border-slate-200/90 bg-white/80 p-3 shadow-inner">
                    {previewQuery.isPending ? (
                      <Skeleton className="h-20 w-full" />
                    ) : previewQuery.data ? (
                      <PreviewPanel
                        key={localFieldKey ?? "preview"}
                        preview={previewQuery.data}
                        onCopy={copyText}
                        onRequestAudioPreview={
                          localFieldKey
                            ? async () => {
                                const prepared = await localAudioPreviewMutation.mutateAsync();
                                return {
                                  src: toFileSrc(prepared.path),
                                  ext: prepared.ext,
                                };
                              }
                            : null
                        }
                      />
                    ) : (
                      <div className="flex items-center gap-2 text-xs text-slate-500">
                        <TriangleAlert className="h-4 w-4" />
                        Pick a field to preview its bytes.
                      </div>
                    )}
                  </div>

                  <div className="flex items-center gap-2 text-sm">
                    {busy ? <Loader2 className="h-4 w-4 animate-spin text-emerald-600" /> : null}
                    {errorMessage ? <TriangleAlert className="h-4 w-4 text-amber-500" /> : null}
                    <span
                      className={cn(
                        "select-text cursor-text whitespace-pre-wrap",
                        errorMessage ? "text-amber-700" : "text-slate-600",
                      )}
                    >
                      {logMessage}
                    </span>
                  </div>
                </div>
              </DataCard>
            </>
          )}
        </div>
      </div>
    </main>
  );
}

	function StatPill({
	  label,
	  value,
	  footer,
	  className,
	}: {
  label: string;
  value: string | number;
  footer?: ReactNode;
  className?: string;
	}) {
	  return (
	    <div
	      className={cn(
	        "rounded-[16px] border border-white/70 bg-white/80 px-3 py-2 text-sm shadow-sm flex min-w-0 flex-col",
	        className,
	      )}
	    >
      <div className="text-[11px] uppercase tracking-[0.08em] text-slate-500">{label}</div>
      <div className="text-lg font-semibold text-slate-900 whitespace-normal break-words leading-snug">{value}</div>
      {footer ? <div className="mt-auto pt-3">{footer}</div> : null}
    </div>
  );
}

function DataCard({
  title,
  icon,
  children,
  footerHint,
}: {
  title: string;
  icon: ReactNode;
  children: ReactNode;
  footerHint?: string;
}) {
  return (
    <Card className="min-w-0 border-slate-200/80 bg-white/80 shadow-sm backdrop-blur flex h-full flex-col overflow-hidden">
      <CardHeader className="pb-2 shrink-0">
        <CardTitle className="flex items-center gap-2 text-slate-900">
          {icon}
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent className="flex flex-1 min-h-0 flex-col space-y-3 overflow-hidden">
        {children}
        {footerHint ? (
          <div className="flex items-center gap-2 text-xs text-slate-500">
            <ArrowRight className="h-3.5 w-3.5 text-slate-400" />
            {footerHint}
          </div>
        ) : null}
      </CardContent>
    </Card>
  );
}

function PreviewPanel({
  preview,
  onCopy,
  onRequestAudioPreview,
}: {
  preview: FieldPreview;
  onCopy: (text: string) => void;
  onRequestAudioPreview: null | (() => Promise<{ src: string; ext: string }>);
}) {
  const [audioSource, setAudioSource] = useState<{ src: string; type?: string } | null>(null);
  const [audioPreparing, setAudioPreparing] = useState(false);
  const audioRef = useRef<HTMLAudioElement | null>(null);

  const ext = (preview.guessedExt ?? "").trim().replace(/^\./, "").toLowerCase();
  const supportsAudio = ["wav", "mp3", "flac", "sph", "m4a", "ogg", "opus", "aac"].includes(ext);

  const mimeForExt = (value: string) => {
    switch (value) {
      case "wav":
        return "audio/wav";
      case "mp3":
        return "audio/mpeg";
      case "flac":
        return "audio/flac";
      case "m4a":
        return "audio/mp4";
      case "ogg":
        return "audio/ogg";
      case "opus":
        return "audio/opus";
      case "aac":
        return "audio/aac";
      default:
        return undefined;
    }
  };

  const prepareAndPlayAudio = async () => {
    if (!onRequestAudioPreview || audioPreparing) return;
    if (audioSource) {
      void audioRef.current?.play().catch(() => undefined);
      return;
    }
    setAudioPreparing(true);
    try {
      const prepared = await onRequestAudioPreview();
      setAudioSource({ src: prepared.src, type: mimeForExt(prepared.ext) });
      setTimeout(() => {
        audioRef.current?.load();
        void audioRef.current?.play().catch(() => undefined);
      }, 0);
    } finally {
      setAudioPreparing(false);
    }
  };

  const copyPayload = preview.previewText ? preview.previewText : preview.hexSnippet ? `Hex: ${preview.hexSnippet}` : "";
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
          <Sparkles className="h-4 w-4 text-emerald-600" />
          Preview
        </div>
        <div className="flex items-center gap-2">
          <Badge variant="secondary">
            {preview.guessedExt ? `.${preview.guessedExt}` : "unknown"} · {formatBytes(preview.size)}
          </Badge>
          {audioPreparing ? <Loader2 className="h-4 w-4 animate-spin text-slate-500" /> : null}
          <Button
            type="button"
            size="sm"
            variant="ghost"
            className="h-8 w-8 p-0 text-slate-600 hover:bg-slate-100"
            onClick={() => onCopy(copyPayload)}
            aria-label="Copy preview"
          >
            <Copy className="h-4 w-4" />
          </Button>
        </div>
      </div>

	      {supportsAudio ? (
	        <div className="space-y-2">
	          <div
	            className={cn("relative rounded-xl border border-slate-200 bg-white/70 p-2", !audioSource ? "cursor-pointer" : "")}
	            onClick={() => {
	              if (!audioSource) void prepareAndPlayAudio();
	            }}
          >
            <audio
              ref={audioRef}
              controls
              preload="none"
              className={cn("w-full h-10", !audioSource ? "pointer-events-none opacity-70" : "")}
            >
              {audioSource ? <source src={audioSource.src} type={audioSource.type} /> : null}
            </audio>
          </div>
        </div>
      ) : null}

      {!supportsAudio && preview.previewText ? (
        <pre className="max-h-60 overflow-auto whitespace-pre-wrap break-all rounded-xl bg-slate-50 px-3 py-2 text-xs text-slate-800 shadow-inner select-text cursor-text">
          {preview.previewText}
        </pre>
      ) : !supportsAudio ? (
        <div className="text-xs text-slate-600 break-all select-text cursor-text">
          Hex:{" "}
          <span className="font-mono text-slate-800 break-all whitespace-pre-wrap select-text cursor-text">
            {preview.hexSnippet}
          </span>
        </div>
      ) : null}
    </div>
  );
}

function JsonPreviewPanel({ title, value, onCopy }: { title: string; value: unknown; onCopy: () => void }) {
  const [imageFailedSrc, setImageFailedSrc] = useState<string | null>(null);
  const [audioFailedSrc, setAudioFailedSrc] = useState<string | null>(null);
  const previewText = useMemo(() => {
    const limit = 900;
    const ellipsis = "…";

    if (typeof value === "string") {
      const raw = value;
      return raw.length > limit ? `${raw.slice(0, limit)}${ellipsis}` : raw;
    }

    if (value && typeof value === "object" && !Array.isArray(value)) {
      const obj = value as Record<string, unknown>;
      const src = typeof obj.src === "string" ? obj.src.trim() : "";
      if (src) {
        const width = typeof obj.width === "number" && Number.isFinite(obj.width) ? obj.width : undefined;
        const height = typeof obj.height === "number" && Number.isFinite(obj.height) ? obj.height : undefined;
        let cleanedSrc = src;
        try {
          const u = new URL(src);
          u.search = "";
          cleanedSrc = u.toString();
        } catch {
          // ignore
        }
        const compact = safeJson({ width, height, src: cleanedSrc });
        return compact.length > limit ? `${compact.slice(0, limit)}${ellipsis}` : compact;
      }
    }

    if (Array.isArray(value)) {
      const entries = value
        .filter((it) => it && typeof it === "object" && !Array.isArray(it))
        .slice(0, 5)
        .map((it) => {
          const obj = it as Record<string, unknown>;
          const src = typeof obj.src === "string" ? obj.src.trim() : "";
          if (!src) return null;
          let cleanedSrc = src;
          try {
            const u = new URL(src);
            u.search = "";
            cleanedSrc = u.toString();
          } catch {
            // ignore
          }
          const type = typeof obj.type === "string" ? obj.type.trim() : undefined;
          return { type, src: cleanedSrc };
        })
        .filter(Boolean);
      if (entries.length) {
        const compact = safeJson(entries);
        return compact.length > limit ? `${compact.slice(0, limit)}${ellipsis}` : compact;
      }
    }

    const raw = safeJson(value);
    return raw.length > limit ? `${raw.slice(0, limit)}${ellipsis}` : raw;
  }, [value]);

  const imageCandidate = useMemo(() => {
    if (!value || typeof value !== "object" || Array.isArray(value)) return null;
    const obj = value as Record<string, unknown>;
    const src = typeof obj.src === "string" ? obj.src.trim() : "";
    if (!src) return null;
    if (imageFailedSrc === src) return null;
    const isAllowed = src.startsWith("https://") || src.startsWith("http://") || src.startsWith("data:image/");
    if (!isAllowed) return null;
    const width = typeof obj.width === "number" && Number.isFinite(obj.width) ? obj.width : undefined;
    const height = typeof obj.height === "number" && Number.isFinite(obj.height) ? obj.height : undefined;
    return { src, width, height };
  }, [imageFailedSrc, value]);

  const audioCandidates = useMemo(() => {
    const guessIsAudio = (src: string, mime?: string) => {
      const t = (mime ?? "").trim().toLowerCase();
      if (t.startsWith("audio/")) return true;
      const ext = extFromUrl(src) ?? "";
      return ["wav", "mp3", "flac", "m4a", "ogg", "opus", "aac"].includes(ext);
    };

    const normalizeCandidate = (src: string, mime?: string) => {
      const cleaned = src.trim();
      if (!cleaned) return null;
      if (audioFailedSrc === cleaned) return null;
      const isAllowed =
        cleaned.startsWith("https://") || cleaned.startsWith("http://") || cleaned.startsWith("data:audio/");
      if (!isAllowed) return null;
      if (!guessIsAudio(cleaned, mime)) return null;
      return { src: cleaned, type: mime?.trim() || undefined };
    };

    if (!value) return null;

    if (Array.isArray(value)) {
      const out = value
        .filter((it) => it && typeof it === "object" && !Array.isArray(it))
        .map((it) => {
          const obj = it as Record<string, unknown>;
          const src = typeof obj.src === "string" ? obj.src : "";
          const type = typeof obj.type === "string" ? obj.type : undefined;
          return normalizeCandidate(src, type);
        })
        .filter(Boolean) as Array<{ src: string; type?: string }>;
      return out.length ? out.slice(0, 3) : null;
    }

    if (typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const src = typeof obj.src === "string" ? obj.src : "";
      const type = typeof obj.type === "string" ? obj.type : undefined;
      return normalizeCandidate(src, type) ? [normalizeCandidate(src, type)!] : null;
    }

    return null;
  }, [audioFailedSrc, value]);

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
          <Sparkles className="h-4 w-4 text-emerald-600" />
          {title}
        </div>
        <Button
          type="button"
          size="sm"
          variant="ghost"
          className="h-8 w-8 p-0 text-slate-600 hover:bg-slate-100"
          onClick={onCopy}
          aria-label="Copy JSON value"
        >
          <Copy className="h-4 w-4" />
        </Button>
      </div>
      {audioCandidates ? (
        <div className="space-y-2">
          {audioCandidates.map((candidate) => (
            <div key={candidate.src} className="rounded-xl border border-slate-200 bg-white/70 p-2">
              <audio
                controls
                preload="none"
                className="w-full"
                onError={() => setAudioFailedSrc(candidate.src)}
              >
                <source src={candidate.src} type={candidate.type} />
              </audio>
            </div>
          ))}
        </div>
      ) : null}
      {imageCandidate ? (
        <div className="rounded-xl border border-slate-200 bg-white/70 p-2">
          {/* eslint-disable-next-line @next/next/no-img-element */}
          <img
            src={imageCandidate.src}
            alt={title}
            width={imageCandidate.width}
            height={imageCandidate.height}
            loading="lazy"
            decoding="async"
            className="max-h-60 w-full rounded-lg object-contain bg-slate-50"
            onError={() => setImageFailedSrc(imageCandidate.src)}
          />
        </div>
      ) : null}
      {!audioCandidates ? (
        <pre className="max-h-60 overflow-auto whitespace-pre-wrap break-all rounded-xl bg-slate-50 px-3 py-2 text-xs text-slate-800 shadow-inner select-text cursor-text">
          {previewText}
        </pre>
      ) : null}
    </div>
  );
}

function EmptyState({ hint }: { hint: string }) {
  return (
    <div className="flex h-24 flex-col items-center justify-center text-xs text-slate-500">
      <TriangleAlert className="mb-1 h-4 w-4" />
      {hint}
    </div>
  );
}
