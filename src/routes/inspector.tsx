import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useForm } from "@tanstack/react-form";
import { useMutation, useQuery } from "@tanstack/react-query";
import { motion } from "framer-motion";
import { Kbd, Modal, ModalBody, ModalContent, ModalHeader, Tab, Tabs, Tooltip } from "@heroui/react";
import {
  ArrowRight,
  ArrowUpRight,
  ArrowUpRightFromSquare,
  BadgeInfo,
  ChevronLeft,
  ChevronRight,
  Cloud,
  Copy,
  Database,
  FolderOpen,
  HardDrive,
  KeyRound,
  Loader2,
  Play,
  Search,
  Sparkles,
  Terminal,
  TriangleAlert,
} from "lucide-react";

import { Badge } from "@/components/ui/badge";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import {
  chooseIndexSource,
  chooseOpenerApp,
  clearHfToken,
  detectLocalDataset,
  hfDatasetPreview,
  hfOpenField,
  isTauri,
  listChunkItems,
  loadChunkList,
  loadIndex,
  mosaicmlListSamples,
  mosaicmlLoadIndex,
  mosaicmlOpenLeaf,
  mosaicmlPeekField,
  mosaicmlPrepareAudioPreview,
  openLeaf,
  openPathWithApp,
  peekField,
  prepareAudioPreview,
  readHfToken,
  readPreferredOpenerForExt,
  readLastIndex,
  saveHfToken,
  saveLastIndex,
  savePreferredOpenerForExt,
  toFileSrc,
  wdsListSamples,
  wdsLoadDir,
  wdsOpenMember,
  wdsPeekMember,
  wdsPrepareAudioPreview,
  zenodoOpenFile,
  zenodoPeekFile,
  zenodoRecordSummary,
  zenodoTarInlineEntryMedia,
  zenodoTarListEntries,
  zenodoTarOpenEntry,
  zenodoTarPeekEntry,
  zenodoZipListEntries,
  zenodoZipInlineEntryMedia,
  zenodoZipOpenEntry,
  zenodoZipPeekEntry,
  type HfConfigSummary,
  type FieldPreview,
  type HfDatasetPreview,
  type HfFeature,
  type IndexSummary,
  type ItemMeta,
  type WdsDirSummary,
  type WdsSampleListResponse,
  type InlineMediaResponse,
  type ZenodoFileSummary,
  type ZenodoRecordSummary,
  type ZenodoTarEntryListResponse,
  type ZenodoTarEntrySummary,
  type ZenodoZipEntrySummary,
} from "@/lib/tauri-api";
import { cn } from "@/lib/utils";
import { useViewerStore } from "@/store/viewer";

const HF_PAGE_SIZE = 25;
const WDS_PAGE_SIZE = 50;
const ZENODO_TAR_PAGE_SIZE = 25;
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

const audioMimeFromExt = (value: string) => {
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

const buildPreviewMeta = (preview: FieldPreview | null) => {
  if (!preview) return [];
  const ext = (preview.guessedExt ?? "").trim().replace(/^\./, "");
  const typeLabel = ext ? `.${ext}` : "unknown";
  return [typeLabel, formatBytes(preview.size), preview.isBinary ? "binary" : "text"];
};

function StatChip({
  label,
  value,
  title,
}: {
  label: string;
  value: string | number;
  title?: string;
}) {
  return (
    <Badge variant="secondary" className="bg-white/80 text-slate-700" title={title}>
      <span className="flex items-center gap-2">
        <span className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">{label}</span>
        <span className="font-semibold text-slate-900">{value}</span>
      </span>
    </Badge>
  );
}

const normalizeFilter = (value: string) => value.trim().toLowerCase();

const matchesFilter = (haystack: string, needle: string) => {
  if (!needle) return true;
  return haystack.toLowerCase().includes(needle);
};

type SourceKind = "auto" | "litdata" | "mds" | "wds" | "hf" | "zenodo";

function ListFilterInput({
  value,
  onValueChange,
  placeholder,
  ariaLabel,
}: {
  value: string;
  onValueChange: (next: string) => void;
  placeholder: string;
  ariaLabel: string;
}) {
  return (
    <Input
      size="sm"
      variant="bordered"
      radius="full"
      className="bg-white/80"
      placeholder={placeholder}
      value={value}
      onValueChange={onValueChange}
      isClearable
      startContent={<Search className="h-4 w-4 text-slate-500" />}
      aria-label={ariaLabel}
    />
  );
}

const looksLikeHfInput = (value: string) => {
  const v = value.trim();
  if (!v) return false;
  if (v.startsWith("hf://datasets/")) return true;
  if (v.startsWith("https://huggingface.co/datasets/") || v.startsWith("http://huggingface.co/datasets/")) return true;
  if (v.startsWith("https://hf.co/datasets/") || v.startsWith("http://hf.co/datasets/")) return true;
  return false;
};

const looksLikeZenodoInput = (value: string) => {
  const v = value.trim();
  if (!v) return false;
  try {
    const u = new URL(v);
    const host = u.hostname.toLowerCase();
    if (!(host === "zenodo.org" || host.endsWith(".zenodo.org"))) return false;
    const segments = u.pathname.split("/").filter(Boolean);
    for (let i = 0; i < segments.length; i += 1) {
      if (segments[i] !== "records" && segments[i] !== "record") continue;
      const id = segments[i + 1] ?? "";
      if (/^[0-9]+$/.test(id)) return true;
    }
    return false;
  } catch {
    return false;
  }
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

function commonPathPrefix(values: string[]) {
  if (values.length < 2) return "";
  const filtered = values.map((v) => v.trim()).filter(Boolean);
  if (filtered.length < 2) return "";
  let prefix = filtered[0]!;
  for (let i = 1; i < filtered.length; i += 1) {
    const next = filtered[i]!;
    let j = 0;
    const max = Math.min(prefix.length, next.length);
    while (j < max && prefix[j] === next[j]) j += 1;
    prefix = prefix.slice(0, j);
    if (!prefix) return "";
  }
  const cut = prefix.lastIndexOf("/");
  if (cut <= 0) return "";
  return prefix.slice(0, cut + 1);
}

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

function extFromFilename(name: string) {
  const base = name.split(/[\\/]/).pop() ?? name;
  const ext = base.includes(".") ? base.split(".").pop() : "";
  const cleaned = String(ext ?? "").trim().replace(/^\\./, "").toLowerCase();
  return cleaned || null;
}

function looksLikeTarFilename(name: string) {
  const n = name.trim().toLowerCase();
  return (
    n.endsWith(".tar") ||
    n.endsWith(".tar.gz") ||
    n.endsWith(".tgz") ||
    n.endsWith(".tar.zst") ||
    n.endsWith(".tar.zstd")
  );
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

export default function InspectorPage() {
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
    wdsOffset,
    setWdsOffset,
    setHfConfigSplit,
    hfSelectedRowIndex,
    selectHfRow,
    hfSelectedFieldName,
    selectHfField,
    zenodoSelectedFileKey,
    selectZenodoFile,
    zenodoSelectedEntryName,
    selectZenodoEntry,
    zenodoEntriesOffset,
    setZenodoEntriesOffset,
    statusMessage,
    setStatusMessage,
  } = useViewerStore();

  const tauri = useMemo(() => isTauri(), []);

  const [hfToken, setHfToken] = useState<string | null>(null);
  const hfTokenMasked = hfToken ? `…${hfToken.slice(-6)}` : null;
  const [hfTokenDialogOpen, setHfTokenDialogOpen] = useState(false);
  const [hfOffsetDraft, setHfOffsetDraft] = useState(String(hfOffset));
  const [logDockOpen, setLogDockOpen] = useState(false);
  const [explorerTabKey, setExplorerTabKey] = useState<"level1" | "level2" | "level3">("level1");
  const [sourceKind, setSourceKind] = useState<SourceKind>("auto");
  const [filterLevel1, setFilterLevel1] = useState("");
  const [filterLevel2, setFilterLevel2] = useState("");
  const [filterLevel3, setFilterLevel3] = useState("");
  const level1Needle = useMemo(() => normalizeFilter(filterLevel1), [filterLevel1]);
  const level2Needle = useMemo(() => normalizeFilter(filterLevel2), [filterLevel2]);
  const level3Needle = useMemo(() => normalizeFilter(filterLevel3), [filterLevel3]);

  const tokenForm = useForm({
    defaultValues: {
      token: "",
    },
    onSubmit: async ({ value }) => {
      const trimmed = value.token.trim();
      try {
        if (!trimmed) {
          await clearHfToken();
          setHfToken(null);
        } else {
          await saveHfToken(trimmed);
          setHfToken(trimmed);
        }
        setHfTokenDialogOpen(false);
      } catch (err) {
        setStatusMessage(err instanceof Error ? err.message : "Unable to save token.");
      }
    },
  });
  

  const isLitdataMode = mode?.kind === "litdata-index" || mode?.kind === "litdata-chunks";
  const isMdsMode = mode?.kind === "mds-index";
  const isLocalIndexMode = isLitdataMode || isMdsMode;
  const isWdsMode = mode?.kind === "webdataset-dir";
  const isHfMode = mode?.kind === "huggingface";
  const isZenodoMode = mode?.kind === "zenodo";
  const autodetectedHf = sourceKind === "auto" && looksLikeHfInput(sourceInput) && chunkSelection.length === 0;
  const autodetectedZenodo =
    sourceKind === "auto" && looksLikeZenodoInput(sourceInput) && chunkSelection.length === 0;

  const latestSourceInputRef = useRef(sourceInput);
  useEffect(() => {
    latestSourceInputRef.current = sourceInput;
  }, [sourceInput]);

  useEffect(() => {
    if (chunkSelection.length > 0 && sourceKind !== "litdata") {
      setSourceKind("litdata");
    }
  }, [chunkSelection.length, sourceKind]);

  useEffect(() => {
    setHfOffsetDraft(String(hfOffset));
  }, [hfOffset]);

  

  useEffect(() => {
    if (!isTauri()) return;
    void readLastIndex()
      .then((last) => {
        if (!last) return;
        if (!latestSourceInputRef.current.trim()) setSourceInput(last);
      })
      .catch((err) => console.error("Unable to read last index:", err));
  }, [setSourceInput]);

  useEffect(() => {
    if (!isTauri()) return;
    void readHfToken()
      .then((token) => setHfToken(token))
      .catch((err) => console.error("Unable to read HF token:", err));
  }, []);

  useEffect(() => {
    if (!hfTokenDialogOpen) return;
    tokenForm.update({
      defaultValues: {
        token: hfToken ?? "",
      },
    });
  }, [hfToken, hfTokenDialogOpen, tokenForm]);

  useEffect(() => {
    setLogDockOpen(false);
    setExplorerTabKey("level1");
    setFilterLevel1("");
    setFilterLevel2("");
    setFilterLevel3("");
  }, [mode?.requestId]);

  const indexQuery = useQuery<IndexSummary>({
    queryKey: ["index-summary", mode?.requestId ?? 0],
    enabled: Boolean(isLocalIndexMode),
    queryFn: () => {
      if (!mode) throw new Error("No source selected.");
      if (mode.kind === "litdata-index") return loadIndex(mode.indexPath);
      if (mode.kind === "litdata-chunks") return loadChunkList(mode.paths);
      if (mode.kind === "mds-index") return mosaicmlLoadIndex(mode.indexPath);
      throw new Error("Not a local index mode.");
    },
  });

  const wdsDirQuery = useQuery<WdsDirSummary>({
    queryKey: ["wds-dir", mode?.requestId ?? 0],
    enabled: Boolean(isWdsMode),
    queryFn: () => {
      if (!mode || mode.kind !== "webdataset-dir") throw new Error("No WebDataset selected.");
      return wdsLoadDir(mode.dirPath);
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

  const zenodoQuery = useQuery<ZenodoRecordSummary>({
    queryKey: ["zenodo-record", isZenodoMode ? mode.input : null],
    enabled: Boolean(isZenodoMode && isTauri()),
    queryFn: () => {
      if (!mode || mode.kind !== "zenodo") throw new Error("No Zenodo record selected.");
      return zenodoRecordSummary({ input: mode.input });
    },
    staleTime: 5 * 60 * 1000,
  });

  const hfDatasetInput = isHfMode ? mode.input : null;
  const [hfSplitsCache, setHfSplitsCache] = useState<{ input: string; configs: HfConfigSummary[] } | null>(null);
  const [hfSelectedCache, setHfSelectedCache] = useState<{ input: string; config: string; split: string } | null>(null);

  useEffect(() => {
    if (!hfDatasetInput) {
      setHfSplitsCache(null);
      setHfSelectedCache(null);
      return;
    }
    setHfSplitsCache((prev) => (prev?.input === hfDatasetInput ? prev : null));
    setHfSelectedCache((prev) => (prev?.input === hfDatasetInput ? prev : null));
  }, [hfDatasetInput]);

  useEffect(() => {
    if (!hfDatasetInput || !hfQuery.data) return;
    setHfSplitsCache({ input: hfDatasetInput, configs: hfQuery.data.configs });
    setHfSelectedCache({ input: hfDatasetInput, config: hfQuery.data.config, split: hfQuery.data.split });
  }, [hfDatasetInput, hfQuery.data]);

  useEffect(() => {
    if ((mode?.kind === "litdata-index" || mode?.kind === "mds-index") && indexQuery.data?.indexPath) {
      void saveLastIndex(indexQuery.data.indexPath).catch((err) => console.error("Unable to save last index:", err));
    }
  }, [indexQuery.data?.indexPath, mode?.kind]);

  useEffect(() => {
    if (indexQuery.data) {
      const noun = isMdsMode ? "shard" : "chunk";
      setStatusMessage(`Loaded ${indexQuery.data.chunks.length} ${noun}${indexQuery.data.chunks.length === 1 ? "" : "s"}.`);
    }
  }, [indexQuery.data, isMdsMode, setStatusMessage]);

  useEffect(() => {
    if (wdsDirQuery.data) {
      setStatusMessage(`Loaded ${wdsDirQuery.data.shards.length} shard${wdsDirQuery.data.shards.length === 1 ? "" : "s"}.`);
    }
  }, [setStatusMessage, wdsDirQuery.data]);

  useEffect(() => {
    if (hfQuery.data) {
      const suffix = hfQuery.data.partial ? " (partial)" : "";
      setStatusMessage(`Loaded ${hfQuery.data.dataset} · ${hfQuery.data.config}/${hfQuery.data.split}${suffix}.`);
    }
  }, [hfQuery.data, setStatusMessage]);

  useEffect(() => {
    if (zenodoQuery.data) {
      const count = zenodoQuery.data.files.length;
      setStatusMessage(`Loaded Zenodo record ${zenodoQuery.data.recordId} · ${count} file${count === 1 ? "" : "s"}.`);
    }
  }, [setStatusMessage, zenodoQuery.data]);

  useEffect(() => {
    if (indexQuery.data && (mode?.kind === "litdata-index" || mode?.kind === "mds-index") && chunkSelection.length) {
      setChunkSelection([]);
    }
    if (wdsDirQuery.data && mode?.kind === "webdataset-dir" && chunkSelection.length) {
      setChunkSelection([]);
    }
  }, [chunkSelection.length, indexQuery.data, mode?.kind, setChunkSelection, wdsDirQuery.data]);

  useEffect(() => {
    if (!isLocalIndexMode) return;
    if (!indexQuery.data) {
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
  }, [indexQuery.data, isLocalIndexMode, selectChunk, selectedChunkName]);

  useEffect(() => {
    if (!isWdsMode) return;
    if (!wdsDirQuery.data) {
      selectChunk(null);
      return;
    }
    const nextShard =
      wdsDirQuery.data.shards.find((shard) => shard.filename === selectedChunkName)?.filename ||
      wdsDirQuery.data.shards[0]?.filename ||
      null;
    if (nextShard !== selectedChunkName) {
      selectChunk(nextShard);
    }
  }, [isWdsMode, selectChunk, selectedChunkName, wdsDirQuery.data]);

  const selectedChunk = useMemo(
    () => indexQuery.data?.chunks.find((chunk) => chunk.filename === selectedChunkName) ?? null,
    [indexQuery.data, selectedChunkName],
  );

  const selectedShard = useMemo(
    () => wdsDirQuery.data?.shards.find((shard) => shard.filename === selectedChunkName) ?? null,
    [selectedChunkName, wdsDirQuery.data],
  );

  const zenodoFiles = useMemo(() => zenodoQuery.data?.files ?? [], [zenodoQuery.data?.files]);
  const selectedZenodoFile = useMemo(() => {
    if (!zenodoFiles.length) return null;
    if (zenodoSelectedFileKey) {
      const found = zenodoFiles.find((f) => f.key === zenodoSelectedFileKey);
      if (found) return found;
    }
    return zenodoFiles[0] ?? null;
  }, [zenodoFiles, zenodoSelectedFileKey]);
  const selectedZenodoExt = selectedZenodoFile ? extFromFilename(selectedZenodoFile.key) : null;
  const zenodoIsZip = selectedZenodoExt === "zip";
  const zenodoIsTar = Boolean(selectedZenodoFile && looksLikeTarFilename(selectedZenodoFile.key));
  const zenodoIsArchive = zenodoIsZip || zenodoIsTar;

  useEffect(() => {
    if (!isZenodoMode) return;
    if (!zenodoFiles.length) {
      if (zenodoSelectedFileKey !== null) selectZenodoFile(null);
      return;
    }
    const exists = zenodoSelectedFileKey ? zenodoFiles.some((f) => f.key === zenodoSelectedFileKey) : false;
    if (!exists) {
      selectZenodoFile(zenodoFiles[0]!.key);
    }
  }, [isZenodoMode, selectZenodoFile, zenodoFiles, zenodoSelectedFileKey]);

  useEffect(() => {
    if (!isWdsMode) return;
    setWdsOffset(0);
    selectItem(null);
    selectField(null);
  }, [isWdsMode, selectField, selectItem, selectedShard?.filename, setWdsOffset]);

  const wdsSamplesQuery = useQuery<WdsSampleListResponse>({
    queryKey: ["wds-samples", wdsDirQuery.data?.dirPath, selectedShard?.filename, wdsOffset, WDS_PAGE_SIZE],
    enabled: Boolean(isWdsMode && wdsDirQuery.data && selectedShard && !wdsDirQuery.isFetching),
    queryFn: () =>
      wdsListSamples({
        dirPath: wdsDirQuery.data?.dirPath ?? "",
        shardFilename: selectedShard?.filename ?? "",
        offset: wdsOffset,
        length: WDS_PAGE_SIZE,
      }),
    staleTime: 5 * 60 * 1000,
  });

  const itemsQuery = useQuery<ItemMeta[]>({
    queryKey: ["chunk-items", indexQuery.data?.indexPath, selectedChunk?.filename],
    enabled: Boolean(isLocalIndexMode && indexQuery.data && selectedChunk && !indexQuery.isFetching),
    queryFn: () => {
      if (!mode) throw new Error("No source selected.");
      if (mode.kind === "mds-index") {
        return mosaicmlListSamples({
          indexPath: indexQuery.data?.indexPath ?? "",
          shardFilename: selectedChunk?.filename ?? "",
        });
      }
      return listChunkItems({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
      });
    },
    staleTime: 5 * 60 * 1000,
  });

  useEffect(() => {
    if (!isLocalIndexMode) return;
    const items = itemsQuery.data ?? [];
    if (!items.length) {
      selectItem(null);
      return;
    }
    const exists = items.some((item) => item.itemIndex === selectedItemIndex);
    if (!exists) {
      selectItem(items[0].itemIndex);
    }
  }, [isLocalIndexMode, itemsQuery.data, selectItem, selectedItemIndex]);

  const selectedItem = useMemo(
    () => itemsQuery.data?.find((item) => item.itemIndex === selectedItemIndex) ?? null,
    [itemsQuery.data, selectedItemIndex],
  );

  useEffect(() => {
    if (!isLocalIndexMode) return;
    if (!selectedItem) {
      selectField(null);
      return;
    }
    const exists = selectedItem.fields.some((field) => field.fieldIndex === selectedFieldIndex);
    if (!exists) {
      selectField(selectedItem.fields[0]?.fieldIndex ?? null);
    }
  }, [isLocalIndexMode, selectField, selectedFieldIndex, selectedItem]);

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
    enabled: Boolean(
      isLocalIndexMode && indexQuery.data && selectedChunk && selectedItem && selectedField && !itemsQuery.isFetching,
    ),
    queryFn: () => {
      if (!mode) throw new Error("No source selected.");
      if (mode.kind === "mds-index") {
        return mosaicmlPeekField({
          indexPath: indexQuery.data?.indexPath ?? "",
          shardFilename: selectedChunk?.filename ?? "",
          itemIndex: selectedItem?.itemIndex ?? 0,
          fieldIndex: selectedField?.fieldIndex ?? 0,
        });
      }
      return peekField({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
        itemIndex: selectedItem?.itemIndex ?? 0,
        fieldIndex: selectedField?.fieldIndex ?? 0,
      });
    },
    staleTime: 60 * 1000,
  });

  useEffect(() => {
    if (!isWdsMode) return;
    const samples = wdsSamplesQuery.data?.samples ?? [];
    if (!samples.length) {
      selectItem(null);
      return;
    }
    const exists = samples.some((sample) => sample.sampleIndex === selectedItemIndex);
    if (!exists) {
      selectItem(samples[0].sampleIndex);
    }
  }, [isWdsMode, selectItem, selectedItemIndex, wdsSamplesQuery.data?.samples]);

  const selectedWdsSample = useMemo(
    () => wdsSamplesQuery.data?.samples.find((sample) => sample.sampleIndex === selectedItemIndex) ?? null,
    [selectedItemIndex, wdsSamplesQuery.data?.samples],
  );

  useEffect(() => {
    if (!isWdsMode) return;
    if (!selectedWdsSample) {
      selectField(null);
      return;
    }
    const idx = selectedFieldIndex ?? -1;
    if (idx < 0 || idx >= selectedWdsSample.fields.length) {
      selectField(selectedWdsSample.fields.length ? 0 : null);
    }
  }, [isWdsMode, selectField, selectedFieldIndex, selectedWdsSample]);

  const selectedWdsField = useMemo(() => {
    if (!selectedWdsSample) return null;
    const idx = selectedFieldIndex ?? -1;
    if (idx < 0 || idx >= selectedWdsSample.fields.length) return null;
    return selectedWdsSample.fields[idx] ?? null;
  }, [selectedFieldIndex, selectedWdsSample]);

  const wdsPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["wds-preview", wdsDirQuery.data?.dirPath, selectedShard?.filename, selectedWdsField?.memberPath],
    enabled: Boolean(isWdsMode && wdsDirQuery.data && selectedShard && selectedWdsField && !wdsSamplesQuery.isFetching),
    queryFn: () =>
      wdsPeekMember({
        dirPath: wdsDirQuery.data?.dirPath ?? "",
        shardFilename: selectedShard?.filename ?? "",
        memberPath: selectedWdsField?.memberPath ?? "",
      }),
    staleTime: 60 * 1000,
  });

  const zenodoPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-preview", selectedZenodoFile?.contentUrl ?? null],
    enabled: Boolean(isZenodoMode && selectedZenodoFile && !zenodoQuery.isFetching && !zenodoIsArchive),
    queryFn: () => zenodoPeekFile({ contentUrl: selectedZenodoFile?.contentUrl ?? "" }),
    staleTime: 60 * 1000,
  });

  const zenodoPreview: FieldPreview | null = useMemo(() => {
    if (!zenodoPreviewQuery.data || !selectedZenodoFile) return null;
    return { ...zenodoPreviewQuery.data, size: selectedZenodoFile.size };
  }, [selectedZenodoFile, zenodoPreviewQuery.data]);

  const zenodoZipEntriesQuery = useQuery<ZenodoZipEntrySummary[]>({
    queryKey: ["zenodo-zip-entries", selectedZenodoFile?.contentUrl ?? null],
    enabled: Boolean(isZenodoMode && zenodoIsZip && selectedZenodoFile && !zenodoQuery.isFetching),
    queryFn: () =>
      zenodoZipListEntries({
        contentUrl: selectedZenodoFile?.contentUrl ?? "",
        filename: selectedZenodoFile?.key ?? "",
      }),
    staleTime: 10 * 60 * 1000,
  });

  const zenodoZipEntries = useMemo(() => zenodoZipEntriesQuery.data ?? [], [zenodoZipEntriesQuery.data]);
  const zenodoZipEntryPrefix = useMemo(
    () => commonPathPrefix(zenodoZipEntries.map((e) => e.name)),
    [zenodoZipEntries],
  );
  const zenodoTarEntriesQuery = useQuery<ZenodoTarEntryListResponse>({
    queryKey: ["zenodo-tar-entries", selectedZenodoFile?.contentUrl ?? null, zenodoEntriesOffset, ZENODO_TAR_PAGE_SIZE],
    enabled: Boolean(isZenodoMode && zenodoIsTar && selectedZenodoFile && !zenodoQuery.isFetching),
    queryFn: () =>
      zenodoTarListEntries({
        contentUrl: selectedZenodoFile?.contentUrl ?? "",
        filename: selectedZenodoFile?.key ?? "",
        offset: zenodoEntriesOffset,
        length: ZENODO_TAR_PAGE_SIZE,
      }),
    staleTime: 10 * 60 * 1000,
  });

  const zenodoTarEntries = useMemo(() => zenodoTarEntriesQuery.data?.entries ?? [], [zenodoTarEntriesQuery.data?.entries]);
  const zenodoTarCanPrev = zenodoIsTar && zenodoEntriesOffset > 0;
  const zenodoTarCanNext =
    zenodoIsTar &&
    Boolean(
      zenodoTarEntriesQuery.data?.partial ||
        (typeof zenodoTarEntriesQuery.data?.numEntriesTotal === "number" &&
          zenodoEntriesOffset + ZENODO_TAR_PAGE_SIZE < zenodoTarEntriesQuery.data.numEntriesTotal),
    );
  const zenodoTarEntryPrefix = useMemo(
    () => commonPathPrefix(zenodoTarEntries.map((e) => e.name)),
    [zenodoTarEntries],
  );

  const selectedZenodoEntry = useMemo(() => {
    if (zenodoIsZip) {
      if (!zenodoZipEntries.length) return null;
      if (zenodoSelectedEntryName) {
        const found = zenodoZipEntries.find((e) => e.name === zenodoSelectedEntryName);
        if (found) return found;
      }
      return zenodoZipEntries.find((e) => !e.isDir) ?? null;
    }
    if (zenodoIsTar) {
      if (!zenodoTarEntries.length) return null;
      if (zenodoSelectedEntryName) {
        const found = zenodoTarEntries.find((e) => e.name === zenodoSelectedEntryName);
        if (found) return found;
      }
      return zenodoTarEntries.find((e) => !e.isDir) ?? null;
    }
    return null;
  }, [zenodoIsTar, zenodoIsZip, zenodoSelectedEntryName, zenodoTarEntries, zenodoZipEntries]);

  useEffect(() => {
    if (!isZenodoMode || !zenodoIsArchive) return;
    const entries = zenodoIsZip ? zenodoZipEntries : zenodoTarEntries;
    if (!entries.length) {
      if (zenodoSelectedEntryName !== null) selectZenodoEntry(null);
      return;
    }
    const exists = zenodoSelectedEntryName ? entries.some((e) => e.name === zenodoSelectedEntryName) : false;
    if (!exists) {
      const first = entries.find((e) => !e.isDir)?.name ?? null;
      selectZenodoEntry(first);
    }
  }, [isZenodoMode, selectZenodoEntry, zenodoIsArchive, zenodoIsZip, zenodoSelectedEntryName, zenodoTarEntries, zenodoZipEntries]);

  const zenodoZipEntryPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-zip-entry-preview", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null],
    enabled: Boolean(
      isZenodoMode &&
        zenodoIsZip &&
        selectedZenodoFile &&
        selectedZenodoEntry &&
        !zenodoZipEntriesQuery.isFetching &&
        !zenodoQuery.isFetching,
    ),
    queryFn: () =>
      zenodoZipPeekEntry({
        contentUrl: selectedZenodoFile?.contentUrl ?? "",
        filename: selectedZenodoFile?.key ?? "",
        entryName: selectedZenodoEntry?.name ?? "",
      }),
    staleTime: 60 * 1000,
  });

  const zenodoZipEntryPreview: FieldPreview | null = useMemo(() => {
    if (!zenodoZipEntryPreviewQuery.data || !selectedZenodoEntry || !zenodoIsZip) return null;
    return { ...zenodoZipEntryPreviewQuery.data, size: (selectedZenodoEntry as ZenodoZipEntrySummary).uncompressedSize };
  }, [selectedZenodoEntry, zenodoIsZip, zenodoZipEntryPreviewQuery.data]);

  const zenodoTarEntryPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-tar-entry-preview", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null],
    enabled: Boolean(
      isZenodoMode &&
        zenodoIsTar &&
        selectedZenodoFile &&
        selectedZenodoEntry &&
        !zenodoTarEntriesQuery.isFetching &&
        !zenodoQuery.isFetching,
    ),
    queryFn: () =>
      zenodoTarPeekEntry({
        contentUrl: selectedZenodoFile?.contentUrl ?? "",
        filename: selectedZenodoFile?.key ?? "",
        entryName: selectedZenodoEntry?.name ?? "",
      }),
    staleTime: 60 * 1000,
  });

  const zenodoTarEntryPreview: FieldPreview | null = useMemo(() => {
    if (!zenodoTarEntryPreviewQuery.data || !selectedZenodoEntry || !zenodoIsTar) return null;
    return { ...zenodoTarEntryPreviewQuery.data, size: (selectedZenodoEntry as ZenodoTarEntrySummary).size };
  }, [selectedZenodoEntry, zenodoIsTar, zenodoTarEntryPreviewQuery.data]);

  const [zenodoZipInlineMedia, setZenodoZipInlineMedia] = useState<null | { src: string; mime: string; ext: string }>(
    null,
  );
  const [zenodoZipInlineMediaError, setZenodoZipInlineMediaError] = useState<string | null>(null);
  const zenodoZipVideoRef = useRef<HTMLVideoElement | null>(null);
  useEffect(() => {
    setZenodoZipInlineMediaError(null);
    setZenodoZipInlineMedia((prev) => {
      if (prev?.src) URL.revokeObjectURL(prev.src);
      return null;
    });
  }, [selectedZenodoFile?.contentUrl, selectedZenodoEntry?.name]);

  const [zenodoTarInlineMedia, setZenodoTarInlineMedia] = useState<null | { src: string; mime: string; ext: string }>(
    null,
  );
  const [zenodoTarInlineMediaError, setZenodoTarInlineMediaError] = useState<string | null>(null);
  const zenodoTarVideoRef = useRef<HTMLVideoElement | null>(null);
  useEffect(() => {
    setZenodoTarInlineMediaError(null);
    setZenodoTarInlineMedia((prev) => {
      if (prev?.src) URL.revokeObjectURL(prev.src);
      return null;
    });
  }, [selectedZenodoFile?.contentUrl, selectedZenodoEntry?.name]);

  const openWithAppMutation = useMutation({
    mutationFn: (params: { path: string; appPath: string }) => openPathWithApp(params),
    onSuccess: (message) => setStatusMessage(message),
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected file with the chosen app."),
  });

  const zenodoZipInlineMediaMutation = useMutation({
    mutationFn: async () => {
      if (!selectedZenodoFile) throw new Error("Select a Zenodo ZIP file.");
      if (!selectedZenodoEntry || selectedZenodoEntry.isDir) throw new Error("Select a ZIP entry.");
      return zenodoZipInlineEntryMedia({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const zenodoTarInlineMediaMutation = useMutation({
    mutationFn: async () => {
      if (!selectedZenodoFile) throw new Error("Select a Zenodo TAR file.");
      if (!selectedZenodoEntry || (selectedZenodoEntry as ZenodoTarEntrySummary).isDir) throw new Error("Select a TAR entry.");
      return zenodoTarInlineEntryMedia({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const inlineMediaToObjectUrl = async (result: InlineMediaResponse) => {
    const mime = result.mime || "application/octet-stream";
    const dataUrl = `data:${mime};base64,${result.base64}`;
    const blob = await (await fetch(dataUrl)).blob();
    return { src: URL.createObjectURL(blob), mime, ext: result.ext };
  };

  const loadZenodoZipInlineMedia = async () => {
    try {
      setZenodoZipInlineMediaError(null);
      const result = await zenodoZipInlineMediaMutation.mutateAsync();
      const next = await inlineMediaToObjectUrl(result);
      setZenodoZipInlineMedia((prev) => {
        if (prev?.src) URL.revokeObjectURL(prev.src);
        return next;
      });
      return next;
    } catch (err) {
      let message = "Unable to load media preview.";
      if (err instanceof Error) {
        message = err.message || message;
      } else if (typeof err === "string" && err.trim()) {
        message = err;
      } else if (err && typeof err === "object") {
        const maybe = err as Record<string, unknown>;
        if (typeof maybe.message === "string" && maybe.message.trim()) {
          message = maybe.message;
        } else if (typeof maybe.error === "string" && maybe.error.trim()) {
          message = maybe.error;
        } else if (typeof maybe.code === "string" && typeof maybe.message === "string") {
          message = `${maybe.code}: ${maybe.message}`;
        }
      }
      setZenodoZipInlineMediaError(message);
      throw err;
    }
  };

  const autoplayVideoWhenReady = (ref: React.RefObject<HTMLVideoElement | null>) => {
    let tries = 0;
    const maxTries = 12;
    const tick = () => {
      tries += 1;
      const el = ref.current;
      if (el) {
        void el.play().catch(() => undefined);
        return;
      }
      if (tries >= maxTries) return;
      requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  };

  const loadZenodoTarInlineMedia = async () => {
    try {
      setZenodoTarInlineMediaError(null);
      const result = await zenodoTarInlineMediaMutation.mutateAsync();
      const next = await inlineMediaToObjectUrl(result);
      setZenodoTarInlineMedia((prev) => {
        if (prev?.src) URL.revokeObjectURL(prev.src);
        return next;
      });
      return next;
    } catch (err) {
      let message = "Unable to load media preview.";
      if (err instanceof Error) {
        message = err.message || message;
      } else if (typeof err === "string" && err.trim()) {
        message = err;
      } else if (err && typeof err === "object") {
        const maybe = err as Record<string, unknown>;
        if (typeof maybe.message === "string" && maybe.message.trim()) {
          message = maybe.message;
        } else if (typeof maybe.error === "string" && maybe.error.trim()) {
          message = maybe.error;
        } else if (typeof maybe.code === "string" && typeof maybe.message === "string") {
          message = `${maybe.code}: ${maybe.message}`;
        }
      }
      setZenodoTarInlineMediaError(message);
      throw err;
    }
  };

  const zenodoOpenEntryMutation = useMutation({
    mutationFn: async (entry: ZenodoZipEntrySummary) => {
      if (!selectedZenodoFile) throw new Error("Select a Zenodo ZIP file to open.");
      if (!entry || entry.isDir) throw new Error("Select a ZIP entry to open.");
      const guessedExt = extFromFilename(entry.name);
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      return zenodoZipOpenEntry({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: entry.name,
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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected ZIP entry."),
  });

  const zenodoOpenTarEntryMutation = useMutation({
    mutationFn: async (entry: ZenodoTarEntrySummary) => {
      if (!selectedZenodoFile) throw new Error("Select a Zenodo TAR file to open.");
      if (!entry || entry.isDir) throw new Error("Select a TAR entry to open.");
      const guessedExt = extFromFilename(entry.name);
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      return zenodoTarOpenEntry({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: entry.name,
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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected TAR entry."),
  });

  const zenodoOpenFileMutation = useMutation({
    mutationFn: async (file: ZenodoFileSummary) => {
      if (!file) throw new Error("Select a Zenodo file to open.");
      const guessedExt = extFromFilename(file.key) ?? (zenodoPreviewQuery.data?.guessedExt ?? null);
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      return zenodoOpenFile({
        contentUrl: file.contentUrl,
        filename: file.key,
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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected Zenodo file."),
  });

  const openFieldMutation = useMutation({
    mutationFn: async () => {
      if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) {
        throw new Error("Select a field to open.");
      }
      const guessedExt = (previewQuery.data?.guessedExt ?? "").trim().replace(/^\\./, "");
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      if (mode?.kind === "mds-index") {
        return mosaicmlOpenLeaf({
          indexPath: indexQuery.data.indexPath,
          shardFilename: selectedChunk.filename,
          itemIndex: selectedItem.itemIndex,
          fieldIndex: selectedField.fieldIndex,
          openerAppPath,
        });
      }
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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
    },
    onError: (err: unknown) => setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected field."),
  });

  const localAudioPreviewMutation = useMutation({
    mutationFn: async () => {
      if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) {
        throw new Error("Select an audio field to preview.");
      }
      if (mode?.kind === "mds-index") {
        return mosaicmlPrepareAudioPreview({
          indexPath: indexQuery.data.indexPath,
          shardFilename: selectedChunk.filename,
          itemIndex: selectedItem.itemIndex,
          fieldIndex: selectedField.fieldIndex,
        });
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

  const wdsOpenFieldMutation = useMutation({
    mutationFn: async () => {
      if (!wdsDirQuery.data || !selectedShard || !selectedWdsSample || !selectedWdsField) {
        throw new Error("Select a field to open.");
      }
      const guessedExt = (wdsPreviewQuery.data?.guessedExt ?? "").trim().replace(/^\\./, "");
      const openerAppPath = guessedExt ? await readPreferredOpenerForExt(guessedExt) : null;
      return wdsOpenMember({
        dirPath: wdsDirQuery.data.dirPath,
        shardFilename: selectedShard.filename,
        memberPath: selectedWdsField.memberPath,
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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected WebDataset field."),
  });

  const wdsAudioPreviewMutation = useMutation({
    mutationFn: async () => {
      if (!wdsDirQuery.data || !selectedShard || !selectedWdsField) {
        throw new Error("Select an audio field to preview.");
      }
      return wdsPrepareAudioPreview({
        dirPath: wdsDirQuery.data.dirPath,
        shardFilename: selectedShard.filename,
        memberPath: selectedWdsField.memberPath,
      });
    },
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to prepare the WebDataset audio preview."),
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

  const handleLoad = async () => {
    setStatusMessage(null);
    if (chunkSelection.length > 0) {
      triggerLoad("litdata-chunks", chunkSelection);
      return;
    }
    const trimmed = sourceInput.trim();
    if (!trimmed) return;

    if (sourceKind === "zenodo" || (sourceKind === "auto" && looksLikeZenodoInput(trimmed))) {
      triggerLoad("zenodo", trimmed);
      return;
    }

    if (sourceKind === "hf" || (sourceKind === "auto" && looksLikeHfInput(trimmed))) {
      triggerLoad("huggingface", trimmed);
      return;
    }

    if (!isTauri()) {
      setStatusMessage("Loading requires the Tauri runtime.");
      return;
    }

    const expectedLocalKind = sourceKind === "auto" ? null : sourceKind;
    try {
      const detected = await detectLocalDataset(trimmed);
      if (detected.kind === "litdata-index") {
        if (expectedLocalKind && expectedLocalKind !== "litdata") setSourceKind("litdata");
        setSourceInput(detected.indexPath);
        setChunkSelection([]);
        triggerLoad("litdata-index");
        return;
      }
      if (detected.kind === "mds-index") {
        if (expectedLocalKind && expectedLocalKind !== "mds") setSourceKind("mds");
        setSourceInput(detected.indexPath);
        setChunkSelection([]);
        triggerLoad("mds-index");
        return;
      }
      if (detected.kind === "webdataset-dir") {
        if (expectedLocalKind && expectedLocalKind !== "wds") setSourceKind("wds");
        setSourceInput(detected.dirPath);
        setChunkSelection([]);
        triggerLoad("webdataset-dir", detected.dirPath);
        return;
      }
      setStatusMessage(`Unsupported dataset kind: ${(detected as { kind?: string }).kind ?? "unknown"}`);
    } catch (err) {
      setStatusMessage(err instanceof Error ? err.message : String(err));
    }
  };

  const handleChoose = async () => {
    try {
      const pick = await chooseIndexSource(sourceInput, indexQuery.data?.rootDir ?? wdsDirQuery.data?.dirPath);
      if (!pick) return;
      setStatusMessage(null);
      const detected = await detectLocalDataset(pick.indexPath);
      if (detected.kind === "litdata-index") {
        setSourceKind("litdata");
        setSourceInput(detected.indexPath);
        setChunkSelection([]);
        triggerLoad("litdata-index");
        return;
      }
      if (detected.kind === "mds-index") {
        setSourceKind("mds");
        setSourceInput(detected.indexPath);
        setChunkSelection([]);
        triggerLoad("mds-index");
        return;
      }
      if (detected.kind === "webdataset-dir") {
        setSourceKind("wds");
        setSourceInput(detected.dirPath);
        setChunkSelection([]);
        triggerLoad("webdataset-dir", detected.dirPath);
        return;
      }
      setStatusMessage(`Unsupported dataset kind: ${(detected as { kind?: string }).kind ?? "unknown"}`);
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

  const wdsTotalBytes =
    wdsDirQuery.data?.shards?.reduce((acc, shard) => acc + (Number.isFinite(shard.bytes) ? shard.bytes : 0), 0) ?? 0;

  const hfSplitPairs = useMemo(() => {
    const configs =
      hfQuery.data?.configs ?? (hfSplitsCache?.input === hfDatasetInput ? hfSplitsCache.configs : []);
    return configs.flatMap((c) => c.splits.map((s) => ({ config: c.config, split: s })));
  }, [hfDatasetInput, hfQuery.data?.configs, hfSplitsCache]);

  const hfSelectedPairKey =
    hfConfigOverride && hfSplitOverride
      ? `${hfConfigOverride}:${hfSplitOverride}`
      : hfQuery.data
        ? `${hfQuery.data.config}:${hfQuery.data.split}`
        : hfSelectedCache?.input === hfDatasetInput
          ? `${hfSelectedCache.config}:${hfSelectedCache.split}`
          : null;

  const hfSelectedSplitLabel = useMemo(() => {
    const selectedConfig = (hfConfigOverride ?? hfQuery.data?.config ?? hfSelectedCache?.config ?? "").trim();
    const selectedSplit = (hfSplitOverride ?? hfQuery.data?.split ?? hfSelectedCache?.split ?? "").trim();
    if (!selectedConfig || !selectedSplit) return "—";
    return `${selectedConfig}/${selectedSplit}`;
  }, [hfConfigOverride, hfQuery.data?.config, hfQuery.data?.split, hfSelectedCache?.config, hfSelectedCache?.split, hfSplitOverride]);

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

  const hfSelectedFeature = useMemo(() => {
    if (!derivedSelectedFieldName) return null;
    return hfFeatures.find((feature) => feature.name === derivedSelectedFieldName) ?? null;
  }, [derivedSelectedFieldName, hfFeatures]);

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
      })().catch((err) => setStatusMessage(err instanceof Error ? err.message : "Unable to choose an opener app."));
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

  const wdsPageSamples = useMemo(() => wdsSamplesQuery.data?.samples ?? [], [wdsSamplesQuery.data?.samples]);
  const canPaginateWds = Boolean(isWdsMode && wdsSamplesQuery.data && !wdsSamplesQuery.isFetching);
  const wdsCanPrev = canPaginateWds && wdsOffset > 0;
  const wdsTotal = wdsSamplesQuery.data?.numSamplesTotal ?? null;
  const wdsCanNext =
    canPaginateWds &&
    (wdsTotal !== null ? wdsOffset + wdsPageSamples.length < wdsTotal : wdsPageSamples.length === WDS_PAGE_SIZE);

  const busy =
    indexQuery.isFetching ||
    itemsQuery.isFetching ||
    previewQuery.isFetching ||
    wdsDirQuery.isFetching ||
    wdsSamplesQuery.isFetching ||
    wdsPreviewQuery.isFetching ||
    zenodoQuery.isFetching ||
    zenodoPreviewQuery.isFetching ||
    zenodoZipEntriesQuery.isFetching ||
    zenodoZipEntryPreviewQuery.isFetching ||
    hfQuery.isFetching ||
    zenodoOpenFileMutation.isPending ||
    zenodoOpenEntryMutation.isPending ||
    hfOpenMutation.isPending ||
    openFieldMutation.isPending ||
    localAudioPreviewMutation.isPending ||
    wdsOpenFieldMutation.isPending ||
    wdsAudioPreviewMutation.isPending ||
    openWithAppMutation.isPending;
  const latestError =
    indexQuery.error ||
    itemsQuery.error ||
    previewQuery.error ||
    wdsDirQuery.error ||
    wdsSamplesQuery.error ||
    wdsPreviewQuery.error ||
    zenodoQuery.error ||
    zenodoPreviewQuery.error ||
    zenodoZipEntriesQuery.error ||
    zenodoZipEntryPreviewQuery.error ||
    hfQuery.error ||
    zenodoOpenFileMutation.error ||
    zenodoOpenEntryMutation.error ||
    hfOpenMutation.error ||
    openFieldMutation.error ||
    localAudioPreviewMutation.error ||
    wdsOpenFieldMutation.error ||
    wdsAudioPreviewMutation.error ||
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

  useEffect(() => {
    if (errorMessage) setLogDockOpen(true);
  }, [errorMessage]);

  const effectiveKind: Exclude<SourceKind, "auto"> =
    chunkSelection.length > 0
      ? "litdata"
      : sourceKind === "auto"
        ? autodetectedZenodo
          ? "zenodo"
          : autodetectedHf
            ? "hf"
            : isMdsMode
              ? "mds"
              : isWdsMode
                ? "wds"
                : "litdata"
        : sourceKind;
  const canBrowse = effectiveKind === "litdata" || effectiveKind === "mds" || effectiveKind === "wds";
  const sourcePlaceholder =
    sourceKind === "auto" && !sourceInput.trim() && chunkSelection.length === 0
      ? "Supports: LitData • MosaicML MDS • WebDataset • Hugging Face streaming • Zenodo"
      : effectiveKind === "hf"
        ? "Hugging Face streaming (e.g. google/fleurs)"
        : effectiveKind === "zenodo"
          ? "Zenodo record (e.g. 1234567 or https://zenodo.org/records/1234567)"
          : effectiveKind === "wds"
            ? "WebDataset directory (shards/*.tar) or shard path (.tar)"
            : effectiveKind === "mds"
              ? "MosaicML MDS index.json (or a .mds shard)"
              : "LitData index.json (or a .bin shard)";

  const loadIcon =
    effectiveKind === "hf" ? (
      <Database className="mr-2 h-4 w-4" />
    ) : effectiveKind === "zenodo" ? (
      <BadgeInfo className="mr-2 h-4 w-4" />
    ) : (
      <HardDrive className="mr-2 h-4 w-4" />
    );
  const loadLabel = "Load";
  const showHfStats = isHfMode || effectiveKind === "hf";
  const showZenodoStats = isZenodoMode || effectiveKind === "zenodo";
  const datasetPreviewLabel = hfQuery.data?.dataset ?? (showHfStats ? displayHfDatasetId(sourceInput) ?? "—" : "—");
  const zenodoTotalBytes =
    zenodoQuery.data?.files?.reduce((acc, f) => acc + (Number.isFinite(f.size) ? f.size : 0), 0) ?? 0;
  const zenodoRecordLabel = useMemo(() => {
    const version = (zenodoQuery.data?.version ?? "").trim();
    const v = version.replace(/^[vV]/, "");
    return v ? `Version ${v}` : "—";
  }, [zenodoQuery.data?.version]);
  const localFieldKey = useMemo(() => {
    if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) return null;
    return `${indexQuery.data.indexPath}|${selectedChunk.filename}|${selectedItem.itemIndex}|${selectedField.fieldIndex}`;
  }, [indexQuery.data, selectedChunk, selectedField, selectedItem]);
  const wdsFieldKey = useMemo(() => {
    if (!wdsDirQuery.data || !selectedShard || !selectedWdsField) return null;
    return `${wdsDirQuery.data.dirPath}|${selectedShard.filename}|${selectedWdsField.memberPath}`;
  }, [selectedShard, selectedWdsField, wdsDirQuery.data]);

  const localAudioLabel = useMemo(() => {
    if (!selectedChunk || !selectedItem || !selectedField) return "Local audio preview";
    const format = indexQuery.data?.dataFormat[selectedField.fieldIndex] ?? "field";
    return `${selectedChunk.filename} · item ${selectedItem.itemIndex} · ${format}`;
  }, [indexQuery.data?.dataFormat, selectedChunk, selectedField, selectedItem]);

  const wdsAudioLabel = useMemo(() => {
    if (!selectedShard || !selectedWdsSample || !selectedWdsField) return "WebDataset audio preview";
    return `${selectedShard.filename} · ${selectedWdsSample.key} · ${selectedWdsField.name}`;
  }, [selectedShard, selectedWdsField, selectedWdsSample]);

  const zenodoAudioLabel = useMemo(() => {
    const entry = selectedZenodoEntry?.name ?? selectedZenodoFile?.key ?? "Zenodo audio preview";
    return entry;
  }, [selectedZenodoEntry?.name, selectedZenodoFile?.key]);

  const zenodoInspectorSubtitle = useMemo(() => {
    if (!selectedZenodoFile) return "Select a file to inspect.";
    if (zenodoIsArchive) {
      if (selectedZenodoEntry?.name) return `Entry ${selectedZenodoEntry.name}`;
      return "Select an entry to inspect.";
    }
    return selectedZenodoFile.key;
  }, [selectedZenodoEntry?.name, selectedZenodoFile, zenodoIsArchive]);

  const wdsInspectorSubtitle = useMemo(() => {
    if (!selectedWdsSample) return "Select a sample to inspect.";
    const parts = [`Sample ${selectedWdsSample.sampleIndex}`];
    if (selectedWdsField?.name) parts.push(selectedWdsField.name);
    return parts.join(" · ");
  }, [selectedWdsField?.name, selectedWdsSample]);

  const localInspectorSubtitle = useMemo(() => {
    if (!selectedItem) return isMdsMode ? "Select a sample to inspect." : "Select an item to inspect.";
    const label = isMdsMode ? "Sample" : "Item";
    const parts = [`${label} ${selectedItem.itemIndex}`];
    if (selectedField) parts.push(`Field #${selectedField.fieldIndex}`);
    return parts.join(" · ");
  }, [isMdsMode, selectedField, selectedItem]);

  const localPreviewMeta = useMemo(() => {
    const out: string[] = [];
    if (selectedField) {
      const format = indexQuery.data?.dataFormat[selectedField.fieldIndex] ?? "unknown";
      if (format) out.push(format);
      if (Number.isFinite(selectedField.size)) out.push(formatBytes(selectedField.size));
    }
    const previewMeta = buildPreviewMeta(previewQuery.data ?? null);
    previewMeta.forEach((item) => {
      if (!out.includes(item)) out.push(item);
    });
    return out;
  }, [indexQuery.data?.dataFormat, previewQuery.data, selectedField]);

  const wdsPreviewMeta = useMemo(() => {
    const out: string[] = [];
    if (selectedWdsField) {
      const ext = extFromFilename(selectedWdsField.name);
      if (ext) out.push(`.${ext}`);
      if (Number.isFinite(selectedWdsField.size)) out.push(formatBytes(selectedWdsField.size));
    }
    const previewMeta = buildPreviewMeta(wdsPreviewQuery.data ?? null);
    previewMeta.forEach((item) => {
      if (!out.includes(item)) out.push(item);
    });
    return out;
  }, [selectedWdsField, wdsPreviewQuery.data]);

  const zenodoActivePreview = useMemo(() => {
    if (zenodoIsZip) return zenodoZipEntryPreview;
    if (zenodoIsTar) return zenodoTarEntryPreview;
    return zenodoPreview;
  }, [zenodoIsTar, zenodoIsZip, zenodoPreview, zenodoTarEntryPreview, zenodoZipEntryPreview]);

  const zenodoPreviewMeta = useMemo(() => {
    const out: string[] = [];
    const ext =
      (zenodoIsZip || zenodoIsTar) && selectedZenodoEntry
        ? extFromFilename(selectedZenodoEntry.name)
        : selectedZenodoFile
          ? extFromFilename(selectedZenodoFile.key)
          : null;
    if (ext) out.push(`.${ext}`);
    const sizeBytes =
      (zenodoIsZip && selectedZenodoEntry && !selectedZenodoEntry.isDir
        ? (selectedZenodoEntry as ZenodoZipEntrySummary).uncompressedSize
        : zenodoIsTar && selectedZenodoEntry && !selectedZenodoEntry.isDir
          ? (selectedZenodoEntry as ZenodoTarEntrySummary).size
          : selectedZenodoFile?.size) ?? null;
    if (sizeBytes !== null && Number.isFinite(sizeBytes)) out.push(formatBytes(sizeBytes));
    const previewMeta = buildPreviewMeta(zenodoActivePreview ?? null);
    previewMeta.forEach((item) => {
      if (!out.includes(item)) out.push(item);
    });
    return out;
  }, [selectedZenodoEntry, selectedZenodoFile, zenodoActivePreview, zenodoIsTar, zenodoIsZip]);

  const hfPreviewMeta = useMemo(() => {
    const out: string[] = [];
    if (hfSelectedFeature?.dtype) out.push(hfSelectedFeature.dtype);
    if (derivedSelectedFieldName) out.push(derivedSelectedFieldName);
    if (derivedSelectedRowIndex !== null) out.push(`row ${derivedSelectedRowIndex}`);
    return out;
  }, [derivedSelectedFieldName, derivedSelectedRowIndex, hfSelectedFeature?.dtype]);

  const hfExplorerMeta = useMemo(() => {
    const out: string[] = [];
    if (hfSelectedSplitLabel !== "—") out.push(hfSelectedSplitLabel);
    if (derivedSelectedRowIndex !== null) out.push(`row ${derivedSelectedRowIndex}`);
    if (derivedSelectedFieldName) out.push(derivedSelectedFieldName);
    return out;
  }, [derivedSelectedFieldName, derivedSelectedRowIndex, hfSelectedSplitLabel]);

  const zenodoExplorerMeta = useMemo(() => {
    const out: string[] = [];
    if (selectedZenodoFile?.key) out.push(selectedZenodoFile.key.split(/[\\/]/).pop() ?? selectedZenodoFile.key);
    if (zenodoIsArchive && selectedZenodoEntry?.name) {
      out.push(selectedZenodoEntry.name.split(/[\\/]/).pop() ?? selectedZenodoEntry.name);
    }
    return out;
  }, [selectedZenodoEntry?.name, selectedZenodoFile?.key, zenodoIsArchive]);

  const wdsExplorerMeta = useMemo(() => {
    const out: string[] = [];
    if (selectedShard?.filename) out.push(selectedShard.filename);
    if (selectedWdsSample?.key) out.push(selectedWdsSample.key);
    if (selectedWdsField?.name) out.push(selectedWdsField.name);
    return out;
  }, [selectedShard?.filename, selectedWdsField?.name, selectedWdsSample?.key]);

  const localExplorerMeta = useMemo(() => {
    const out: string[] = [];
    if (selectedChunk?.filename) out.push(selectedChunk.filename);
    if (selectedItem) out.push(`${isMdsMode ? "sample" : "item"} ${selectedItem.itemIndex}`);
    if (selectedField) {
      const format = indexQuery.data?.dataFormat[selectedField.fieldIndex] ?? null;
      out.push(format ? `#${selectedField.fieldIndex} ${format}` : `#${selectedField.fieldIndex}`);
    }
    return out;
  }, [indexQuery.data?.dataFormat, isMdsMode, selectedChunk?.filename, selectedField, selectedItem]);

  const cardGridVariants = {
    hidden: { opacity: 0 },
    show: {
      opacity: 1,
      transition: { staggerChildren: 0.08 },
    },
  };
  const panelVariants = {
    hidden: { opacity: 0, y: 12 },
    show: { opacity: 1, y: 0 },
  };

  const renderLocalPreview = () => {
    if (previewQuery.isFetching && !previewQuery.data) return <Skeleton className="h-full w-full rounded-xl" />;
    if (previewQuery.data) {
      return (
        <PreviewPanel
          key={localFieldKey ?? "preview"}
          preview={previewQuery.data}
          onCopy={copyText}
          onOpen={() => openFieldMutation.mutate()}
          openDisabled={busy || !tauri}
          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
          audioLabel={localAudioLabel}
          onAudioError={() => setLogDockOpen(true)}
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
      );
    }
    return <EmptyState hint="Pick a field to preview its bytes." />;
  };

  const renderWdsPreview = () => {
    if (wdsPreviewQuery.isFetching && !wdsPreviewQuery.data) return <Skeleton className="h-full w-full rounded-xl" />;
    if (wdsPreviewQuery.data) {
      return (
        <PreviewPanel
          key={wdsFieldKey ?? "wds-preview"}
          preview={wdsPreviewQuery.data}
          onCopy={copyText}
          onOpen={() => wdsOpenFieldMutation.mutate()}
          openDisabled={busy || !tauri}
          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
          audioLabel={wdsAudioLabel}
          onAudioError={() => setLogDockOpen(true)}
          onRequestAudioPreview={
            wdsFieldKey
              ? async () => {
                  const prepared = await wdsAudioPreviewMutation.mutateAsync();
                  return {
                    src: toFileSrc(prepared.path),
                    ext: prepared.ext,
                  };
                }
              : null
          }
        />
      );
    }
    return <EmptyState hint="Pick a field to preview its bytes." />;
  };

  const renderHfPreview = () => {
    if (hfQuery.isFetching && !hfQuery.data) return <Skeleton className="h-full w-full rounded-xl" />;
    if (hfSelectedRow && derivedSelectedFieldName) {
      return (
        <JsonPreviewPanel
          title={`${derivedSelectedFieldName}`}
          value={hfSelectedValue}
          onCopy={() => copyText(safeJson(hfSelectedValue))}
          onOpen={() => hfOpenMutation.mutate(derivedSelectedFieldName)}
          openDisabled={busy || !tauri}
          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
        />
      );
    }
    return <EmptyState hint="Pick a row and field to preview." />;
  };

  const renderZenodoPreview = () => {
    const isAudioExt = (ext: string | null) =>
      Boolean(ext && ["wav", "mp3", "flac", "m4a", "ogg", "opus", "aac"].includes(ext));

    const isVideoExt = (ext: string | null) =>
      Boolean(ext && ["mp4", "webm", "mov", "m4v"].includes(ext));

    const fileExt = selectedZenodoFile ? extFromFilename(selectedZenodoFile.key) : null;
    const entryExt = selectedZenodoEntry ? extFromFilename(selectedZenodoEntry.name) : null;
    const directExt = zenodoIsArchive ? null : fileExt;
    const zipExt = zenodoIsZip ? entryExt : null;
    const tarExt = zenodoIsTar ? entryExt : null;

    const isDirectVideo = Boolean(!zenodoIsArchive && isVideoExt(directExt) && selectedZenodoFile?.contentUrl);
    const isZipVideo = Boolean(zenodoIsZip && isVideoExt(zipExt) && selectedZenodoEntry && !selectedZenodoEntry.isDir);
    const isTarVideo = Boolean(zenodoIsTar && isVideoExt(tarExt) && selectedZenodoEntry && !selectedZenodoEntry.isDir);

	    const formatVideoMeta = (ext: string | null, sizeBytes: number | null) => {
	      const extLabel = ext ? `.${ext}` : "video";
	      if (sizeBytes !== null && Number.isFinite(sizeBytes)) return `${extLabel} · ${formatBytes(sizeBytes)}`;
	      return extLabel;
	    };

	    if (isDirectVideo) {
	      const meta = formatVideoMeta(directExt, selectedZenodoFile?.size ?? null);
	      return (
	        <MediaPreviewPanel
	          meta={meta}
	          onOpen={
	            selectedZenodoFile
	              ? () => {
	                  if (busy || !tauri) return;
	                  zenodoOpenFileMutation.mutate(selectedZenodoFile);
	                }
	              : null
	          }
	          openDisabled={busy || !tauri}
	          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	          onCopy={copyText}
	          copyPayload={selectedZenodoFile?.contentUrl ?? ""}
	        >
	          <video controls preload="metadata" className="h-full w-full" src={selectedZenodoFile?.contentUrl ?? ""} />
	        </MediaPreviewPanel>
	      );
	    }

	    if (isZipVideo) {
	      const sizeBytes = selectedZenodoEntry
	        ? (selectedZenodoEntry as ZenodoZipEntrySummary).uncompressedSize
        : null;
	      const meta = formatVideoMeta(zipExt, sizeBytes ?? null);
	      if (zenodoZipInlineMedia?.src) {
	        return (
	          <MediaPreviewPanel
	            meta={meta}
	            onOpen={
	              selectedZenodoEntry && !selectedZenodoEntry.isDir
	                ? () => {
	                    if (busy || !tauri) return;
	                    zenodoOpenEntryMutation.mutate(selectedZenodoEntry as ZenodoZipEntrySummary);
	                  }
	                : null
	            }
	            openDisabled={busy || !tauri}
	            openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	            onCopy={copyText}
	            copyPayload={selectedZenodoFile && selectedZenodoEntry ? `${selectedZenodoFile.key}::${selectedZenodoEntry.name}` : ""}
	          >
	            <video
	              ref={zenodoZipVideoRef}
	              controls
	              preload="metadata"
	              className="h-full w-full"
	              src={zenodoZipInlineMedia.src}
	            />
	          </MediaPreviewPanel>
	        );
	      }

	      return (
	        <MediaPreviewPanel
	          meta={meta}
	          onOpen={
	            selectedZenodoEntry && !selectedZenodoEntry.isDir
	              ? () => {
	                  if (busy || !tauri) return;
	                  zenodoOpenEntryMutation.mutate(selectedZenodoEntry as ZenodoZipEntrySummary);
	                }
	              : null
	          }
	          openDisabled={busy || !tauri}
	          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	          onCopy={copyText}
	          copyPayload={selectedZenodoFile && selectedZenodoEntry ? `${selectedZenodoFile.key}::${selectedZenodoEntry.name}` : ""}
	          below={
	            zenodoZipInlineMediaError ? <div className="text-xs text-amber-700">{zenodoZipInlineMediaError}</div> : null
	          }
	        >
	          <button
	            type="button"
	            className={cn(
	              "relative flex h-full w-full items-center justify-center",
	              zenodoZipInlineMediaMutation.isPending ? "cursor-wait opacity-80" : "hover:bg-white/75",
	            )}
	            disabled={zenodoZipInlineMediaMutation.isPending || !isTauri()}
	            onClick={() => {
	              void loadZenodoZipInlineMedia().then((media) => {
	                if (!media?.src) return;
	                autoplayVideoWhenReady(zenodoZipVideoRef);
	              });
	            }}
	            aria-label="Load and play video"
	          >
	            {zenodoZipInlineMediaMutation.isPending ? (
	              <Loader2 className="h-6 w-6 animate-spin text-slate-500" />
	            ) : (
	              <Play className="h-10 w-10 text-slate-500" />
	            )}
	          </button>
	        </MediaPreviewPanel>
	      );
	    }

	    if (isTarVideo) {
	      const sizeBytes = selectedZenodoEntry ? (selectedZenodoEntry as ZenodoTarEntrySummary).size : null;
	      const meta = formatVideoMeta(tarExt, sizeBytes ?? null);
	      if (zenodoTarInlineMedia?.src) {
	        return (
	          <MediaPreviewPanel
	            meta={meta}
	            onOpen={
	              selectedZenodoEntry && !selectedZenodoEntry.isDir
	                ? () => {
	                    if (busy || !tauri) return;
	                    zenodoOpenTarEntryMutation.mutate(selectedZenodoEntry as ZenodoTarEntrySummary);
	                  }
	                : null
	            }
	            openDisabled={busy || !tauri}
	            openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	            onCopy={copyText}
	            copyPayload={selectedZenodoFile && selectedZenodoEntry ? `${selectedZenodoFile.key}::${selectedZenodoEntry.name}` : ""}
	          >
	            <video
	              ref={zenodoTarVideoRef}
	              controls
	              preload="metadata"
	              className="h-full w-full"
	              src={zenodoTarInlineMedia.src}
	            />
	          </MediaPreviewPanel>
	        );
	      }

	      return (
	        <MediaPreviewPanel
	          meta={meta}
	          onOpen={
	            selectedZenodoEntry && !selectedZenodoEntry.isDir
	              ? () => {
	                  if (busy || !tauri) return;
	                  zenodoOpenTarEntryMutation.mutate(selectedZenodoEntry as ZenodoTarEntrySummary);
	                }
	              : null
	          }
	          openDisabled={busy || !tauri}
	          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	          onCopy={copyText}
	          copyPayload={selectedZenodoFile && selectedZenodoEntry ? `${selectedZenodoFile.key}::${selectedZenodoEntry.name}` : ""}
	          below={
	            zenodoTarInlineMediaError ? <div className="text-xs text-amber-700">{zenodoTarInlineMediaError}</div> : null
	          }
	        >
	          <button
	            type="button"
	            className={cn(
	              "relative flex h-full w-full items-center justify-center",
	              zenodoTarInlineMediaMutation.isPending ? "cursor-wait opacity-80" : "hover:bg-white/75",
	            )}
	            disabled={zenodoTarInlineMediaMutation.isPending || !isTauri()}
	            onClick={() => {
	              void loadZenodoTarInlineMedia().then((media) => {
	                if (!media?.src) return;
	                autoplayVideoWhenReady(zenodoTarVideoRef);
	              });
	            }}
	            aria-label="Load and play video"
	          >
	            {zenodoTarInlineMediaMutation.isPending ? (
	              <Loader2 className="h-6 w-6 animate-spin text-slate-500" />
	            ) : (
	              <Play className="h-10 w-10 text-slate-500" />
	            )}
	          </button>
	        </MediaPreviewPanel>
	      );
	    }

    if (zenodoIsZip) {
      if (zenodoZipEntryPreviewQuery.isFetching && !zenodoZipEntryPreviewQuery.data) {
        return <Skeleton className="h-full w-full rounded-xl" />;
      }
      if (zenodoZipEntryPreview) {
        const ext = (zenodoZipEntryPreview.guessedExt ?? "").trim().replace(/^\\./, "").toLowerCase();
        const onRequestAudioPreview = isAudioExt(ext)
          ? async () => {
              if (zenodoZipInlineMedia?.src && zenodoZipInlineMedia.ext === ext) {
                return { src: zenodoZipInlineMedia.src, ext };
              }
              const media = await loadZenodoZipInlineMedia();
              return { src: media.src, ext };
            }
          : null;
	        return (
	          <PreviewPanel
	            preview={zenodoZipEntryPreview}
	            onCopy={copyText}
	            onOpen={
	              selectedZenodoEntry && !selectedZenodoEntry.isDir
	                ? () => {
	                    if (busy || !tauri) return;
	                    zenodoOpenEntryMutation.mutate(selectedZenodoEntry as ZenodoZipEntrySummary);
	                  }
	                : null
	            }
	            openDisabled={busy || !tauri}
	            openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	            onRequestAudioPreview={onRequestAudioPreview}
	            audioLabel={zenodoAudioLabel}
	            onAudioError={() => setLogDockOpen(true)}
	          />
	        );
      }
      return <EmptyState hint="Select a ZIP entry to preview." />;
    }

    if (zenodoIsTar) {
      if (zenodoTarEntryPreviewQuery.isFetching && !zenodoTarEntryPreviewQuery.data) {
        return <Skeleton className="h-full w-full rounded-xl" />;
      }
      if (zenodoTarEntryPreview) {
        const ext = (zenodoTarEntryPreview.guessedExt ?? "").trim().replace(/^\\./, "").toLowerCase();
        const onRequestAudioPreview = isAudioExt(ext)
          ? async () => {
              if (zenodoTarInlineMedia?.src && zenodoTarInlineMedia.ext === ext) {
                return { src: zenodoTarInlineMedia.src, ext };
              }
              const media = await loadZenodoTarInlineMedia();
              return { src: media.src, ext };
            }
          : null;
	        return (
	          <PreviewPanel
	            preview={zenodoTarEntryPreview}
	            onCopy={copyText}
	            onOpen={
	              selectedZenodoEntry && !selectedZenodoEntry.isDir
	                ? () => {
	                    if (busy || !tauri) return;
	                    zenodoOpenTarEntryMutation.mutate(selectedZenodoEntry as ZenodoTarEntrySummary);
	                  }
	                : null
	            }
	            openDisabled={busy || !tauri}
	            openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	            onRequestAudioPreview={onRequestAudioPreview}
	            audioLabel={zenodoAudioLabel}
	            onAudioError={() => setLogDockOpen(true)}
	          />
	        );
      }
      return <EmptyState hint="Select a TAR entry to preview." />;
    }

    if (zenodoPreviewQuery.isFetching && !zenodoPreviewQuery.data) return <Skeleton className="h-full w-full rounded-xl" />;
    if (zenodoPreview) {
      const ext = (zenodoPreview.guessedExt ?? "").trim().replace(/^\\./, "").toLowerCase();
      const onRequestAudioPreview = isAudioExt(ext)
        ? async () => {
            if (!selectedZenodoFile?.contentUrl) throw new Error("Missing content URL.");
            return { src: selectedZenodoFile.contentUrl, ext };
          }
        : null;
      if (isVideoExt(ext) && selectedZenodoFile?.contentUrl) {
        return (
          <div className="flex h-full min-h-0 overflow-hidden rounded-lg bg-white/60 ring-1 ring-black/[0.05]">
            <video controls preload="metadata" className="h-full w-full">
              <source src={selectedZenodoFile?.contentUrl ?? ""} />
            </video>
          </div>
        );
      }
	      return (
	        <PreviewPanel
	          preview={zenodoPreview}
	          onCopy={copyText}
	          onOpen={
	            selectedZenodoFile
	              ? () => {
	                  if (busy || !tauri) return;
	                  zenodoOpenFileMutation.mutate(selectedZenodoFile);
	                }
	              : null
	          }
	          openDisabled={busy || !tauri}
	          openTooltip={tauri ? "Open in default app" : "Opening requires the Tauri runtime."}
	          onRequestAudioPreview={onRequestAudioPreview}
	          audioLabel={zenodoAudioLabel}
	          onAudioError={() => setLogDockOpen(true)}
	        />
	      );
    }
    return <EmptyState hint="Select a file to preview." />;
  };

  return (
    <main className="h-full overflow-hidden">
      <div className="flex h-full flex-col gap-2">
        <section className="shrink-0 overflow-hidden rounded-2xl bg-white/55 shadow-[var(--shadow-soft)] backdrop-blur ring-1 ring-black/5">
          <div className="flex flex-col gap-2 p-3">
            <div className="flex flex-wrap items-center gap-2">
              <div className="flex items-center gap-2 pr-1 text-sm font-semibold tracking-tight text-slate-900">
                <Sparkles className="h-4 w-4 text-emerald-600" />
                Dataset Inspector
              </div>

              <Tabs
                aria-label="Source kind"
                variant="solid"
                selectedKey={sourceKind}
                onSelectionChange={(key) => {
                  const next = String(key);
                  if (
                    next === "auto" ||
                    next === "litdata" ||
                    next === "mds" ||
                    next === "wds" ||
                    next === "hf" ||
                    next === "zenodo"
                  ) {
                    setSourceKind(next);
                  }
                }}
                classNames={{
                  tabList: "rounded-full bg-black/[0.04] p-1 ring-1 ring-black/[0.04]",
                  tab: "h-8 px-3 text-[11px] font-semibold text-slate-600 data-[selected=true]:text-slate-900",
                  tabContent: "gap-2",
                  cursor: "rounded-full bg-white/80 shadow-sm",
                  panel: "hidden",
                }}
              >
                <Tab
                  key="auto"
                  title={
                    <Tooltip
                      showArrow
                      placement="bottom-start"
                      content="Auto-detect: WebDataset, LitData, MosaicML MDS, Hugging Face streaming, Zenodo."
                    >
                      <div className="flex items-center gap-2">
                        <Sparkles className="h-3.5 w-3.5 text-emerald-600" />
                        Auto
                      </div>
                    </Tooltip>
                  }
                  isDisabled={chunkSelection.length > 0}
                />
                <Tab
                  key="litdata"
                  title={
                    <Tooltip showArrow placement="bottom-start" content="LitData local preview (index.json / .bin shards).">
                      <div className="flex items-center gap-2">
                        <FolderOpen className="h-3.5 w-3.5" />
                        LitData
                      </div>
                    </Tooltip>
                  }
                />
                <Tab
                  key="mds"
                  title={
                    <Tooltip showArrow placement="bottom-start" content="MosaicML MDS local preview (index.json / .mds shards).">
                      <div className="flex items-center gap-2">
                        <Database className="h-3.5 w-3.5" />
                        MDS
                      </div>
                    </Tooltip>
                  }
                  isDisabled={chunkSelection.length > 0}
                />
                <Tab
                  key="wds"
                  title={
                    <Tooltip showArrow placement="bottom-start" content="WebDataset local preview (shards/*.tar).">
                      <div className="flex items-center gap-2">
                        <HardDrive className="h-3.5 w-3.5" />
                        WebDataset
                      </div>
                    </Tooltip>
                  }
                  isDisabled={chunkSelection.length > 0}
                />
                <Tab
                  key="hf"
                  title={
                    <Tooltip showArrow placement="bottom-start" content="Hugging Face streaming dataset online preview.">
                      <div className="flex items-center gap-2">
                        <Cloud className="h-3.5 w-3.5" />
                        Hugging Face
                      </div>
                    </Tooltip>
                  }
                  isDisabled={chunkSelection.length > 0}
                />
                <Tab
                  key="zenodo"
                  title={
                    <Tooltip showArrow placement="bottom-start" content="Zenodo dataset online preview.">
                      <div className="flex items-center gap-2">
                        <BadgeInfo className="h-3.5 w-3.5" />
                        Zenodo
                      </div>
                    </Tooltip>
                  }
                  isDisabled={chunkSelection.length > 0}
                />
              </Tabs>

              {sourceKind === "auto" ? (
                <Badge variant="secondary" className="bg-white/85 text-slate-700">
                  Detected:{" "}
                  {autodetectedHf
                    ? "Hugging Face"
                    : autodetectedZenodo
                      ? "Zenodo"
                      : isMdsMode
                        ? "MosaicML MDS"
                        : isWdsMode
                          ? "WebDataset"
                          : isLitdataMode
                            ? "LitData"
                            : "Local"}
                </Badge>
              ) : null}

              {chunkSelection.length > 0 ? (
                <Tooltip
                  showArrow
                  placement="bottom-start"
                  content={
                    <div className="max-w-[340px] space-y-1 p-1 text-xs text-slate-700">
                      {chunkSelection.slice(0, 8).map((path) => (
                        <div key={path} className="truncate">
                          {path}
                        </div>
                      ))}
                      {chunkSelection.length > 8 ? (
                        <div className="text-[11px] text-slate-500">… and {chunkSelection.length - 8} more</div>
                      ) : null}
                    </div>
                  }
                >
                  <div>
                    <Badge variant="secondary" className="bg-white/85 text-slate-700">
                      {chunkSelection.length} shard{chunkSelection.length > 1 ? "s" : ""} selected
                    </Badge>
                  </div>
                </Tooltip>
              ) : null}

              {chunkSelection.length > 0 ? (
                <Button
                  type="button"
                  size="sm"
                  variant="ghost"
                  className="h-7 px-2 text-[11px] font-semibold text-slate-600 hover:bg-black/[0.05]"
                  onClick={() => setChunkSelection([])}
                  disabled={busy}
                >
                  Clear selection
                </Button>
              ) : null}

              <div className="ml-auto flex flex-wrap items-center gap-2">
                {showHfStats ? (
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    className="h-8 justify-start overflow-hidden rounded-full bg-transparent px-3 text-slate-700 hover:bg-black/[0.05]"
                    disabled={!tauri}
                    onClick={() => setHfTokenDialogOpen(true)}
                  >
                    <KeyRound className="mr-2 h-4 w-4" />
                    <span className="truncate">{hfTokenMasked ? `HF Token ${hfTokenMasked}` : "HF Token"}</span>
                  </Button>
                ) : null}

                <Tooltip
                  showArrow
                  placement="bottom-end"
                  content={<pre className="max-w-[520px] whitespace-pre-wrap break-words p-2 text-xs">{logMessage}</pre>}
                >
                  <div>
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      className={cn(
                        "h-8 px-3 text-xs font-semibold",
                        errorMessage
                          ? "text-rose-700 hover:bg-rose-50"
                          : busy
                            ? "text-amber-700 hover:bg-amber-50"
                            : "text-emerald-700 hover:bg-emerald-50",
                      )}
                      onClick={() => setLogDockOpen((prev) => !prev)}
                    >
                      {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : null}
                      {errorMessage ? <TriangleAlert className="mr-2 h-4 w-4" /> : null}
                      {busy ? "Working" : errorMessage ? "Error" : "Ready"}
                    </Button>
                  </div>
                </Tooltip>

                {tauri ? (
                  <Tooltip content="Check Updates" placement="bottom-end" showArrow>
                    <div>
                      <Button
                        type="button"
                        isIconOnly
                        size="sm"
                        variant="ghost"
                        className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
                        onClick={() => {
                          window.dispatchEvent(new CustomEvent("dataset-inspector:check-updates"));
                        }}
                        aria-label="Check updates"
                      >
                        <ArrowUpRight className="h-4 w-4" />
                      </Button>
                    </div>
                  </Tooltip>
                ) : null}
              </div>
            </div>

            <div className="flex flex-col gap-2 lg:flex-row lg:items-center">
              <Input
                variant="bordered"
                radius="full"
                className="w-full bg-white/70"
                placeholder={sourcePlaceholder}
                value={sourceInput}
                onChange={(e) => setSourceInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Enter") void handleLoad();
                }}
                aria-label="Source"
              />
              <div className="flex items-center gap-2">
                {canBrowse ? (
                  <Button
                    variant="ghost"
                    className="h-9 rounded-full bg-transparent px-3 text-slate-700 hover:bg-black/[0.05]"
                    onClick={handleChoose}
                    disabled={busy || !tauri}
                  >
                    <FolderOpen className="mr-2 h-4 w-4" />
                    Browse
                  </Button>
                ) : null}
                <Tooltip
                  isDisabled={tauri}
                  showArrow
                  placement="bottom-end"
                  content="Loading requires the Tauri runtime."
                >
                  <div>
                    <Button
                      className="shadow-[var(--shadow-glow)]"
                      onClick={() => void handleLoad()}
                      disabled={busy || (!sourceInput.trim() && chunkSelection.length === 0) || !tauri}
                    >
                      {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : loadIcon}
                      {loadLabel}
                    </Button>
                  </div>
                </Tooltip>
              </div>
            </div>

	            <div className="flex flex-wrap items-center gap-2">
	              {showHfStats ? (
	                <>
	                  <StatChip label="Dataset" value={datasetPreviewLabel} title={datasetPreviewLabel} />
	                  <StatChip label="Split" value={hfSelectedSplitLabel} />
	                  <StatChip
	                    label="Rows"
	                    value={
	                      hfQuery.data && hfQuery.data.numRowsTotal !== null && hfQuery.data.numRowsTotal !== undefined
	                        ? `${hfQuery.data.numRowsTotal.toLocaleString()}${hfQuery.data.partial ? " (Partial)" : ""}`
	                        : "—"
	                    }
	                  />
	                </>
	              ) : showZenodoStats ? (
                <>
                  <StatChip label="Record" value={zenodoRecordLabel} />
                  <StatChip label="Files" value={zenodoQuery.data?.files.length ?? "—"} />
                  <StatChip label="Size" value={zenodoTotalBytes ? formatBytes(zenodoTotalBytes) : "—"} />
                </>
              ) : isWdsMode ? (
                <>
                  <StatChip label="Shards" value={wdsDirQuery.data?.shards.length ?? "—"} />
                  <StatChip
                    label="Samples"
                    value={
                      wdsSamplesQuery.data
                        ? wdsSamplesQuery.data.numSamplesTotal !== null && wdsSamplesQuery.data.numSamplesTotal !== undefined
                          ? `${wdsSamplesQuery.data.numSamplesTotal.toLocaleString()}`
                          : `≥ ${(wdsOffset + (wdsSamplesQuery.data.samples?.length ?? 0)).toLocaleString()}`
                        : "—"
                    }
                  />
                  <StatChip label="Size" value={wdsTotalBytes ? formatBytes(wdsTotalBytes) : "—"} />
                </>
              ) : (
                <>
                  <StatChip label="Shards" value={indexQuery.data?.chunks.length ?? "—"} />
                  <StatChip label="Items" value={totalItems ? totalItems.toLocaleString() : "—"} />
                  <StatChip label="Size" value={totalBytes ? formatBytes(totalBytes) : "—"} />
                </>
              )}

              <div className="ml-auto flex items-center gap-2">
                <Tooltip
                  showArrow
                  placement="bottom-end"
                  content={
                    <div className="space-y-1 p-1 text-xs text-slate-700">
                      <div className="flex items-center gap-2">
                        <Kbd keys={["enter"]} className="bg-white/80">
                          Enter
                        </Kbd>
                        <span>Load</span>
                      </div>
                      <div className="flex items-center gap-2">
                        <Kbd keys={["escape"]} className="bg-white/80">
                          Esc
                        </Kbd>
                        <span>Close</span>
                      </div>
                    </div>
                  }
                >
                  <div>
                    <Button
                      type="button"
                      isIconOnly
                      size="sm"
                      variant="ghost"
                      className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
                      aria-label="Keyboard shortcuts"
                    >
                      <BadgeInfo className="h-4 w-4" />
                    </Button>
                  </div>
                </Tooltip>
              </div>
            </div>
          </div>
        </section>

        <Modal isOpen={hfTokenDialogOpen} onClose={() => setHfTokenDialogOpen(false)} backdrop="blur" size="md">
          <ModalContent>
            <ModalHeader className="flex flex-col gap-1">
              <div className="text-sm font-semibold text-slate-900">Hugging Face Token</div>
              <div className="text-xs text-slate-500">
                Saved locally on this device. Required for private or gated datasets.
              </div>
            </ModalHeader>
            <ModalBody>
              <form
                className="space-y-3"
                onSubmit={(event) => {
                  event.preventDefault();
                  void tokenForm.handleSubmit();
                }}
              >
                <tokenForm.Field name="token">
                  {(field) => (
                    <Input
                      type="password"
                      variant="bordered"
                      radius="lg"
                      placeholder={hfTokenMasked ? `Token saved (${hfTokenMasked})` : "Paste token here"}
                      value={field.state.value}
                      onChange={(e) => field.handleChange(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Escape") setHfTokenDialogOpen(false);
                      }}
                      autoFocus
                      aria-label="Hugging Face token"
                    />
                  )}
                </tokenForm.Field>

                <div className="flex items-center justify-end gap-2">
                  {hfToken ? (
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={() => {
                        void (async () => {
                          try {
                            await clearHfToken();
                            setHfToken(null);
                            setHfTokenDialogOpen(false);
                          } catch (err) {
                            setStatusMessage(err instanceof Error ? err.message : "Unable to clear token.");
                          }
                        })();
                      }}
                    >
                      Clear
                    </Button>
                  ) : null}
                  <Button size="sm" variant="ghost" onClick={() => setHfTokenDialogOpen(false)}>
                    Close
                  </Button>
                  <Button size="sm" type="submit">
                    Save
                  </Button>
                </div>
              </form>
            </ModalBody>
          </ModalContent>
        </Modal>

        <motion.div
          variants={cardGridVariants}
          initial="hidden"
          animate="show"
          className="grid flex-1 min-h-0 gap-2 lg:grid-cols-[minmax(0,1fr)_560px]"
        >
          {isHfMode ? (
            <>
              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
                <ExplorerPanel
                  title="Explorer"
                  subtitle="Splits → Rows → Fields"
                  icon={<Database className="h-4 w-4 text-emerald-600" />}
                  meta={hfExplorerMeta}
                  tabKey={explorerTabKey}
                  onTabChange={setExplorerTabKey}
                  tabs={[
                    {
                      key: "level1",
                      title: "Splits",
                      icon: <Database className="h-4 w-4 text-emerald-600" />,
                      count: hfSplitPairs.length ? hfSplitPairs.length : undefined,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel1}
                            onValueChange={setFilterLevel1}
                            placeholder="Filter splits…"
                            ariaLabel="Filter splits"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const visible = hfSplitPairs.filter((pair) =>
                                matchesFilter(`${pair.config}/${pair.split}`, level1Needle),
                              );
                              if (visible.length) {
                                return visible.map((pair) => {
                                  const key = `${pair.config}:${pair.split}`;
                                  const selected = key === hfSelectedPairKey;
                                  return (
                                    <div
                                      key={key}
                                      className={cn(
                                        "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                        selected
                                          ? "bg-emerald-50/60"
                                          : "hover:bg-black/[0.03]",
                                      )}
                                      onClick={() => {
                                        setFilterLevel2("");
                                        setFilterLevel3("");
                                        setHfConfigSplit(pair.config, pair.split);
                                        setExplorerTabKey("level2");
                                      }}
                                    >
                                      <div className="min-w-0">
                                        <div
                                          className="truncate font-semibold text-slate-900"
                                          title={`${pair.config}/${pair.split}`}
                                        >
                                          {`${pair.config}/${pair.split}`}
                                        </div>
                                      </div>
                                    </div>
                                  );
                                });
                              }
                              if (hfQuery.isPending) return null;
                              if (!hfSplitPairs.length) return <EmptyState hint="Load a dataset to list its splits." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                            {hfQuery.isPending && !hfSplitPairs.length ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                          </ScrollArea>
                        </div>
                      ),
                      hint: "Select a split.",
                    },
                    {
                      key: "level2",
                      title: "Rows",
                      icon: <HardDrive className="h-4 w-4 text-sky-600" />,
                      count: hfQuery.data?.numRowsTotal ?? undefined,
                      isDisabled: !hfQuery.data,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel2}
                            onValueChange={setFilterLevel2}
                            placeholder="Filter rows…"
                            ariaLabel="Filter rows"
                          />

                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const visible = hfRows
                                .map((row, idx) => {
                                  const rowIndex = hfOffset + idx;
                                  const rowObj = (row ?? {}) as Record<string, unknown>;
                                  const firstCol = hfFeatures[0]?.name;
                                  const snippet = firstCol ? formatCell(rowObj[firstCol]) : formatCell(row);
                                  return { rowIndex, snippet };
                                })
                                .filter(({ rowIndex, snippet }) => matchesFilter(`${rowIndex} ${snippet}`, level2Needle));

                              if (visible.length) {
                                return visible.map(({ rowIndex, snippet }) => {
                                  const selected = rowIndex === derivedSelectedRowIndex;
                                  return (
                                    <div
                                      key={rowIndex}
                                      className={cn(
                                        "grid min-w-0 cursor-pointer grid-cols-[auto_1fr] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                        selected
                                          ? "bg-sky-50/60"
                                          : "hover:bg-black/[0.03]",
                                      )}
                                      onClick={() => {
                                        selectHfRow(rowIndex);
                                        setExplorerTabKey("level3");
                                      }}
                                    >
                                      <div className="whitespace-nowrap font-semibold text-slate-900 tabular-nums">
                                        Row {rowIndex}
                                      </div>
                                      <div className="min-w-0 truncate text-xs text-slate-600">{snippet}</div>
                                    </div>
                                  );
                                });
                              }

                              if (hfQuery.isPending) return null;
                              if (!hfRows.length) return <EmptyState hint="Load a split to list its rows." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                            {hfQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                          </ScrollArea>

	                          <div className="shrink-0 rounded-xl bg-white/40 px-2 py-2 ring-1 ring-black/[0.05]">
	                            <div className="flex items-center gap-2">
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

	                              <div className="ml-auto flex items-center gap-2">
	                                <span className="text-[11px] font-semibold text-slate-500 whitespace-nowrap">Offset</span>
	                                <Input
	                                  size="sm"
	                                  variant="bordered"
	                                  radius="lg"
	                                  className="w-20"
	                                  classNames={{
	                                    inputWrapper:
	                                      "h-8 min-h-0 bg-white/80 data-[hover=true]:bg-white/90 group-data-[focus=true]:bg-white/90",
	                                    input: "text-xs font-semibold text-slate-900 tabular-nums",
	                                  }}
	                                  value={hfOffsetDraft}
	                                  onValueChange={setHfOffsetDraft}
	                                  inputMode="numeric"
	                                  pattern="[0-9]*"
	                                  onKeyDown={(e) => {
	                                    if (e.key === "Enter") {
	                                      (e.currentTarget as HTMLInputElement).blur();
	                                      handleHfJump();
	                                    }
	                                  }}
	                                  onBlur={handleHfJump}
	                                  isDisabled={!canPaginateHf}
	                                  aria-label="Offset"
	                                />
	                              </div>
	                            </div>
	                          </div>
	                        </div>
	                      ),
	                      hint: "Pick a row to inspect its fields.",
                    },
                    {
                      key: "level3",
                      title: "Fields",
                      icon: <Play className="h-4 w-4 text-cyan-600" />,
                      count: hfFeatures.length ? hfFeatures.length : undefined,
                      isDisabled: !hfQuery.data,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel3}
                            onValueChange={setFilterLevel3}
                            placeholder="Filter fields…"
                            ariaLabel="Filter Hugging Face fields"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const visible = hfFeatures.filter((feature) => matchesFilter(feature.name, level3Needle));
                              if (visible.length) {
                                return visible.map((feature) => {
                                  const selected = feature.name === derivedSelectedFieldName;
                                  return (
                                    <div
                                      key={feature.name}
                                      className={cn(
                                        "grid min-w-0 cursor-pointer grid-cols-[1fr] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                        selected
                                          ? "bg-cyan-50/60"
                                          : "hover:bg-black/[0.03]",
                                      )}
                                      onClick={() => {
                                        selectHfField(feature.name);
                                      }}
                                      onDoubleClick={() => {
                                        if (!hfSelectedRow || busy) return;
                                        hfOpenMutation.mutate(feature.name);
                                      }}
                                    >
                                      <div className="flex min-w-0 items-center gap-2">
                                        <div className="min-w-0 truncate font-semibold text-slate-900" title={feature.name}>
                                          {feature.name}
                                        </div>
                                        <Badge variant="secondary" className="shrink-0 bg-slate-100/80 text-slate-600">
                                          {(feature.dtype ?? "raw").toString()}
                                        </Badge>
                                      </div>
                                    </div>
                                  );
                                });
                              }
                              if (hfQuery.isPending) return null;
                              if (!hfFeatures.length) return <EmptyState hint="Load a dataset to list its fields." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                            {hfQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                          </ScrollArea>
                        </div>
                      ),
                      hint: "Pick a field to preview.",
                    },
                  ]}
                />
              </motion.div>

	              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
	                <InspectorPanel
	                  title="Inspector"
	                  subtitle={hfSelectedSplitLabel !== "—" ? `Split ${hfSelectedSplitLabel}` : "Select a split to inspect."}
	                  meta={hfPreviewMeta}
	                  showMeta={false}
	                  previewContent={renderHfPreview()}
	                  logMessage={logMessage}
	                  busy={busy}
	                  errorMessage={errorMessage}
	                  logDockOpen={logDockOpen}
                  onToggleLogDock={() => setLogDockOpen((prev) => !prev)}
                  onCopyLog={() => copyText(logMessage)}
                  onClearLog={() => setStatusMessage(null)}
                />
              </motion.div>
            </>
	          ) : isZenodoMode ? (
	            <>
	              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
	                <ExplorerPanel
	                  title="Explorer"
	                  subtitle="Files → Entries"
	                  icon={<BadgeInfo className="h-4 w-4 text-sky-600" />}
	                  meta={zenodoExplorerMeta}
	                  tabKey={explorerTabKey}
	                  onTabChange={setExplorerTabKey}
	                  tabs={[
                    {
                      key: "level1",
                      title: "Files",
                      icon: <HardDrive className="h-4 w-4 text-emerald-600" />,
                      count: zenodoFiles.length ? zenodoFiles.length : undefined,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel1}
                            onValueChange={setFilterLevel1}
                            placeholder="Filter files…"
                            ariaLabel="Filter Zenodo files"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const visible = zenodoFiles.filter((file) => matchesFilter(file.key, level1Needle));
	                              if (visible.length) {
	                                return visible.map((file) => {
	                                  const selected = selectedZenodoFile?.key === file.key;
	                                  return (
	                                    <div
	                                      key={file.key}
	                                      className={cn(
	                                        "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
	                                        selected
	                                          ? "bg-emerald-50/60"
	                                          : "hover:bg-black/[0.03]",
	                                      )}
	                                      onClick={() => {
	                                        setFilterLevel2("");
	                                        setFilterLevel3("");
	                                        selectZenodoFile(file.key);
	                                        setExplorerTabKey("level2");
	                                      }}
	                                    >
	                                      <div className="min-w-0">
	                                        <div className="truncate font-semibold text-slate-900" title={file.key}>
	                                          {file.key}
	                                        </div>
	                                      </div>
	                                      <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
	                                        {formatBytes(file.size)}
	                                      </div>
	                                    </div>
	                                  );
	                                });
	                              }
                              if (zenodoQuery.isPending) return null;
                              if (!zenodoFiles.length) return <EmptyState hint="Load a Zenodo record to list files." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                            {zenodoQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                          </ScrollArea>
                        </div>
                      ),
                      hint: "Select a file to preview.",
                    },
                    {
                      key: "level2",
                      title: "Entries",
                      icon: <BadgeInfo className="h-4 w-4 text-sky-600" />,
                      count: zenodoIsZip
                        ? zenodoZipEntries.length || undefined
                        : zenodoIsTar
                          ? (zenodoTarEntriesQuery.data?.numEntriesTotal ?? (zenodoTarEntries.length || undefined))
                          : undefined,
                      isDisabled: !selectedZenodoFile,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          {zenodoIsArchive ? (
                            <ListFilterInput
                              value={filterLevel2}
                              onValueChange={setFilterLevel2}
                              placeholder="Filter entries…"
                              ariaLabel="Filter Zenodo entries"
                            />
                          ) : null}

                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {zenodoIsZip ? (
	                              zenodoZipEntries
	                                .filter((entry) => matchesFilter(entry.name, level2Needle))
	                                .map((entry) => {
	                                  const selected = selectedZenodoEntry?.name === entry.name;
	                                  const displayName =
	                                    zenodoZipEntryPrefix &&
	                                    entry.name.startsWith(zenodoZipEntryPrefix) &&
	                                    entry.name.length > zenodoZipEntryPrefix.length
	                                      ? entry.name.slice(zenodoZipEntryPrefix.length)
	                                      : entry.name;
	                                  const label = entry.isDir ? `${displayName.replace(/\/+$/, "")}/` : displayName;
	                                  return (
	                                    <div
	                                      key={entry.name}
	                                      className={cn(
	                                        "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
	                                        selected ? "bg-sky-50/60" : "hover:bg-black/[0.03]",
	                                      )}
	                                      onClick={() => {
	                                        selectZenodoEntry(entry.name);
	                                      }}
	                                    >
	                                      <div className="min-w-0">
	                                        <div
	                                          className="truncate font-semibold text-slate-900"
	                                          title={entry.name}
	                                        >
	                                          {label}
	                                        </div>
	                                      </div>
	                                      <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
	                                        {entry.isDir ? "" : formatBytes(entry.uncompressedSize)}
	                                      </div>
	                                    </div>
	                                  );
	                                })
	                            ) : zenodoIsTar ? (
	                              zenodoTarEntries
	                                .filter((entry) => matchesFilter(entry.name, level2Needle))
	                                .map((entry) => {
	                                  const selected = selectedZenodoEntry?.name === entry.name;
	                                  const displayName =
	                                    zenodoTarEntryPrefix &&
	                                    entry.name.startsWith(zenodoTarEntryPrefix) &&
	                                    entry.name.length > zenodoTarEntryPrefix.length
	                                      ? entry.name.slice(zenodoTarEntryPrefix.length)
	                                      : entry.name;
	                                  const label = entry.isDir ? `${displayName.replace(/\/+$/, "")}/` : displayName;
	                                  return (
	                                    <div
	                                      key={entry.name}
	                                      className={cn(
	                                        "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
	                                        selected ? "bg-sky-50/60" : "hover:bg-black/[0.03]",
	                                      )}
	                                      onClick={() => {
	                                        selectZenodoEntry(entry.name);
	                                      }}
	                                    >
	                                      <div className="min-w-0">
	                                        <div
	                                          className="truncate font-semibold text-slate-900"
	                                          title={entry.name}
	                                        >
	                                          {label}
	                                        </div>
	                                      </div>
	                                      <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
	                                        {entry.isDir ? "" : formatBytes(entry.size)}
	                                      </div>
	                                    </div>
	                                  );
	                                })
	                            ) : selectedZenodoFile ? (
	                              <div className="grid min-w-0 grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] bg-sky-50/50 px-2 py-1.5 text-sm">
                                <div className="min-w-0">
                                  <div className="truncate font-semibold text-slate-900" title={selectedZenodoFile.key}>
	                                    {selectedZenodoFile.key}
	                                  </div>
	                                </div>
	                                <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
	                                  {formatBytes(selectedZenodoFile.size)}
	                                </div>
	                              </div>
	                            ) : null}

                            {zenodoIsZip && zenodoZipEntriesQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                            {zenodoIsTar && zenodoTarEntriesQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                            {zenodoIsZip &&
                            level2Needle &&
                            !zenodoZipEntriesQuery.isPending &&
                            zenodoZipEntries.length &&
                            !zenodoZipEntries.some((entry) => matchesFilter(entry.name, level2Needle)) ? (
                              <EmptyState hint="No matches. Try a different filter." />
                            ) : null}
                            {zenodoIsTar &&
                            level2Needle &&
                            !zenodoTarEntriesQuery.isPending &&
                            zenodoTarEntries.length &&
                            !zenodoTarEntries.some((entry) => matchesFilter(entry.name, level2Needle)) ? (
                              <EmptyState hint="No matches. Try a different filter." />
                            ) : null}
                            {zenodoIsZip && !zenodoZipEntries.length && !zenodoZipEntriesQuery.isPending ? (
                              <EmptyState hint="No ZIP entries found (or unsupported ZIP format)." />
                            ) : null}
                            {zenodoIsTar && !zenodoTarEntries.length && !zenodoTarEntriesQuery.isPending ? (
                              <EmptyState hint="No TAR entries found (or unsupported TAR format)." />
                            ) : null}
                            {!zenodoIsArchive && !selectedZenodoFile && !zenodoQuery.isPending ? (
                              <EmptyState hint="Load a Zenodo record to list files." />
                            ) : null}
                          </ScrollArea>

	                          {zenodoIsTar ? (
	                            <div className="shrink-0 rounded-xl bg-white/40 px-2 py-2 ring-1 ring-black/[0.05]">
	                              <div className="flex items-center gap-2">
	                                <Button
	                                  size="sm"
	                                  variant="outline"
	                                  disabled={!zenodoTarCanPrev || !isTauri()}
	                                  onClick={() =>
	                                    setZenodoEntriesOffset(Math.max(0, zenodoEntriesOffset - ZENODO_TAR_PAGE_SIZE))
	                                  }
	                                >
	                                  <ChevronLeft className="mr-1 h-4 w-4" />
	                                  Prev
	                                </Button>
	                                <Button
	                                  size="sm"
	                                  variant="outline"
	                                  disabled={!zenodoTarCanNext || !isTauri()}
	                                  onClick={() => setZenodoEntriesOffset(zenodoEntriesOffset + ZENODO_TAR_PAGE_SIZE)}
	                                >
	                                  Next
	                                  <ChevronRight className="ml-1 h-4 w-4" />
	                                </Button>

	                                <div className="ml-auto flex items-center gap-2">
	                                  <Badge variant="secondary" className="bg-slate-100/80">
	                                    Offset {zenodoEntriesOffset}
	                                  </Badge>
	                                  {zenodoTarEntriesQuery.data?.partial ? (
	                                    <Badge variant="secondary" className="bg-amber-100/80 text-amber-800">
	                                      Partial
	                                    </Badge>
	                                  ) : null}
	                                </div>
	                              </div>
	                            </div>
	                          ) : null}
                        </div>
                      ),
                      hint: zenodoIsZip
                        ? "ZIP entries parsed via HTTP Range (central directory)."
                        : zenodoIsTar
                          ? "TAR entries streamed over HTTP (WebDataset-style)."
                          : "Selected file.",
                    },
	                  ]}
	                />
	              </motion.div>

	              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
	                <InspectorPanel
	                  title="Inspector"
	                  subtitle={zenodoInspectorSubtitle}
	                  meta={zenodoPreviewMeta}
	                  showMeta={false}
	                  previewContent={renderZenodoPreview()}
	                  logMessage={logMessage}
	                  busy={busy}
	                  errorMessage={errorMessage}
	                  logDockOpen={logDockOpen}
                  onToggleLogDock={() => setLogDockOpen((prev) => !prev)}
                  onCopyLog={() => copyText(logMessage)}
                  onClearLog={() => setStatusMessage(null)}
                />
              </motion.div>
            </>
          ) : isWdsMode ? (
            <>
              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
                <ExplorerPanel
                  title="Explorer"
                  subtitle="Shards → Samples → Fields"
                  icon={<HardDrive className="h-4 w-4 text-emerald-600" />}
                  meta={wdsExplorerMeta}
                  tabKey={explorerTabKey}
                  onTabChange={setExplorerTabKey}
                  tabs={[
                    {
                      key: "level1",
                      title: "Shards",
                      icon: <HardDrive className="h-4 w-4 text-emerald-600" />,
                      count: wdsDirQuery.data?.shards.length ?? undefined,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel1}
                            onValueChange={setFilterLevel1}
                            placeholder="Filter shards…"
                            ariaLabel="Filter WebDataset shards"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const shards = wdsDirQuery.data?.shards ?? [];
                              const visible = shards.filter((shard) => matchesFilter(shard.filename, level1Needle));
                              if (visible.length) {
                                return visible.map((shard) => (
                                  <div
                                    key={shard.filename}
                                    className={cn(
                                      "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                      selectedShard?.filename === shard.filename
                                        ? "bg-emerald-50/60"
                                        : "hover:bg-black/[0.03]",
                                    )}
                                    onClick={() => {
                                      setFilterLevel2("");
                                      setFilterLevel3("");
                                      selectChunk(shard.filename);
                                      setExplorerTabKey("level2");
                                    }}
                                  >
                                    <div className="min-w-0">
                                      <div className="truncate font-semibold text-slate-900" title={shard.filename}>
                                        {shard.filename}
                                      </div>
                                    </div>
                                    <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                      {formatBytes(shard.bytes)}
                                    </div>
                                  </div>
                                ));
                              }
                              if (!shards.length) return <EmptyState hint="Load a WebDataset directory to list shards." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                          </ScrollArea>
                        </div>
                      ),
                      hint: "Pick a shard to list its samples.",
                    },
                    {
                      key: "level2",
                      title: "Samples",
                      icon: <BadgeInfo className="h-4 w-4 text-sky-600" />,
                      count: wdsSamplesQuery.data
                        ? wdsSamplesQuery.data.numSamplesTotal !== null && wdsSamplesQuery.data.numSamplesTotal !== undefined
                          ? `${wdsSamplesQuery.data.numSamplesTotal.toLocaleString()}`
                          : `≥ ${(wdsOffset + (wdsSamplesQuery.data.samples?.length ?? 0)).toLocaleString()}`
                        : undefined,
                      isDisabled: !selectedShard,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel2}
                            onValueChange={setFilterLevel2}
                            placeholder="Filter samples…"
                            ariaLabel="Filter WebDataset samples"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const visible = wdsPageSamples.filter(
                                (sample) =>
                                  matchesFilter(sample.key, level2Needle) ||
                                  matchesFilter(String(sample.sampleIndex), level2Needle),
                              );
                              if (visible.length) {
                                return visible.map((sample) => (
                                  <div
                                    key={`${sample.sampleIndex}:${sample.key}`}
                                    className={cn(
                                      "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                      selectedWdsSample?.sampleIndex === sample.sampleIndex
                                        ? "bg-sky-50/60"
                                        : "hover:bg-black/[0.03]",
                                    )}
                                    onClick={() => {
                                      setFilterLevel3("");
                                      selectItem(sample.sampleIndex);
                                      setExplorerTabKey("level3");
                                    }}
                                  >
                                    <div className="min-w-0">
                                      <div className="truncate font-semibold text-slate-900">{sample.key}</div>
                                      <div className="text-xs text-slate-500">Sample {sample.sampleIndex}</div>
                                    </div>
                                    <div className="flex items-center gap-2 whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                      <span>{sample.fields.length.toLocaleString()} files</span>
                                      <span className="text-slate-400">·</span>
                                      <span>{formatBytes(sample.totalBytes)}</span>
                                    </div>
                                  </div>
                                ));
                              }
                              if (wdsSamplesQuery.isPending) return null;
                              if (!wdsPageSamples.length) {
                                return (
                                  <EmptyState
                                    hint={
                                      selectedShard ? "No samples found at this offset." : "Pick a shard to list its samples."
                                    }
                                  />
                                );
                              }
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                            {wdsSamplesQuery.isPending ? (
                              <div className="p-4">
                                <Skeleton className="h-10 w-full" />
                              </div>
                            ) : null}
                          </ScrollArea>

	                          <div className="shrink-0 rounded-xl bg-white/40 px-2 py-2 ring-1 ring-black/[0.05]">
	                            <div className="flex items-center gap-2">
	                              <Button
	                                size="sm"
	                                variant="outline"
	                                disabled={!wdsCanPrev}
	                                onClick={() => setWdsOffset(Math.max(0, wdsOffset - WDS_PAGE_SIZE))}
	                              >
	                                <ChevronLeft className="mr-1 h-4 w-4" />
	                                Prev
	                              </Button>
	                              <Button
	                                size="sm"
	                                variant="outline"
	                                disabled={!wdsCanNext}
	                                onClick={() => setWdsOffset(wdsOffset + WDS_PAGE_SIZE)}
	                              >
	                                Next
	                                <ChevronRight className="ml-1 h-4 w-4" />
	                              </Button>

	                              <div className="ml-auto flex items-center gap-2">
	                                <Badge variant="secondary" className="bg-slate-100/80">
	                                  Offset {wdsOffset}
	                                </Badge>
	                                {wdsSamplesQuery.data?.partial ? (
	                                  <Badge variant="secondary" className="bg-amber-100/80 text-amber-800">
	                                    Partial
	                                  </Badge>
	                                ) : null}
	                              </div>
	                            </div>
	                          </div>
                        </div>
                      ),
                      hint: "Pick a sample to inspect its fields.",
                    },
                    {
                      key: "level3",
                      title: "Fields",
                      icon: <Play className="h-4 w-4 text-cyan-600" />,
                      count: selectedWdsSample?.fields.length ?? undefined,
                      isDisabled: !selectedWdsSample,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          {selectedWdsSample ? (
                            <>
                              <ListFilterInput
                                value={filterLevel3}
                                onValueChange={setFilterLevel3}
                                placeholder="Filter fields…"
                                ariaLabel="Filter WebDataset fields"
                              />
                              <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                                {(() => {
                                  const visible = selectedWdsSample.fields
                                    .map((field, index) => ({ field, index }))
                                    .filter(({ field }) => matchesFilter(`${field.name} ${field.memberPath}`, level3Needle));
                                  if (visible.length) {
                                    return visible.map(({ field, index }) => {
                                      const ext = extFromFilename(field.name) ?? extFromFilename(field.memberPath);
                                      const selected = selectedWdsField?.memberPath === field.memberPath;
                                      return (
                                        <div
                                          key={`${field.memberPath}:${index}`}
                                          className={cn(
                                            "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                            selected ? "bg-cyan-50/60" : "hover:bg-black/[0.03]",
                                          )}
                                          onClick={() => {
                                            selectField(index);
                                          }}
                                          onDoubleClick={() => wdsOpenFieldMutation.mutate()}
                                        >
                                          <div className="min-w-0">
                                            <div className="truncate font-semibold text-slate-900">
                                              {field.name} · {field.memberPath}
                                            </div>
                                          </div>
                                          <div className="flex items-center gap-2 whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                            {ext ? (
                                              <Badge variant="secondary" className="bg-slate-100/80 text-slate-600">
                                                .{ext}
                                              </Badge>
                                            ) : null}
                                            <span>{formatBytes(field.size)}</span>
                                          </div>
                                        </div>
                                      );
                                    });
                                  }
                                  return (
                                    <EmptyState hint={level3Needle ? "No matches. Try a different filter." : "Select a field."} />
                                  );
                                })()}
                              </ScrollArea>
                            </>
                          ) : (
                            <EmptyState hint="Select a sample to see its fields." />
                          )}
                        </div>
                      ),
                      hint: "Pick a field to preview in the inspector.",
                    },
                  ]}
                />
              </motion.div>

              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
                <InspectorPanel
                  title="Inspector"
                  subtitle={wdsInspectorSubtitle}
                  meta={wdsPreviewMeta}
                  showMeta={false}
                  previewContent={renderWdsPreview()}
                  logMessage={logMessage}
                  busy={busy}
                  errorMessage={errorMessage}
                  logDockOpen={logDockOpen}
                  onToggleLogDock={() => setLogDockOpen((prev) => !prev)}
                  onCopyLog={() => copyText(logMessage)}
                  onClearLog={() => setStatusMessage(null)}
                />
              </motion.div>
            </>
          ) : (
            <>
              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
                <ExplorerPanel
                  title="Explorer"
                  subtitle={isMdsMode ? "Shards → Samples → Fields" : "Shards → Items → Fields"}
                  icon={<HardDrive className="h-4 w-4 text-emerald-600" />}
                  meta={localExplorerMeta}
                  tabKey={explorerTabKey}
                  onTabChange={setExplorerTabKey}
                  tabs={[
                    {
                      key: "level1",
                      title: "Shards",
                      icon: <HardDrive className="h-4 w-4 text-emerald-600" />,
                      count: indexQuery.data?.chunks.length ?? undefined,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel1}
                            onValueChange={setFilterLevel1}
                            placeholder="Filter shards…"
                            ariaLabel="Filter local shards"
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const chunks = indexQuery.data?.chunks ?? [];
                              const visible = chunks.filter((chunk) => matchesFilter(chunk.filename, level1Needle));
                              if (visible.length) {
                                return visible.map((chunk) => (
                                  <div
                                    key={chunk.filename}
                                    className={cn(
                                      "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                      selectedChunk?.filename === chunk.filename
                                        ? "bg-emerald-50/60"
                                        : "hover:bg-black/[0.03]",
                                    )}
                                    onClick={() => {
                                      setFilterLevel2("");
                                      setFilterLevel3("");
                                      selectChunk(chunk.filename);
                                      setExplorerTabKey("level2");
                                    }}
                                  >
                                    <div className="min-w-0">
                                      <div className="truncate font-semibold text-slate-900" title={chunk.filename}>
                                        {chunk.filename}
                                      </div>
                                    </div>
                                    <div className="flex items-center gap-2 whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                      <span>
                                        {chunk.chunkSize.toLocaleString()} {isMdsMode ? "samples" : "items"}
                                      </span>
                                      <span className="text-slate-400">·</span>
                                      <span>{formatBytes(chunk.chunkBytes)}</span>
                                    </div>
                                  </div>
                                ));
                              }
                              if (!chunks.length) return <EmptyState hint="Load a dataset to list shards." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                          </ScrollArea>
                        </div>
                      ),
                      hint: "Pick a shard to list its samples.",
                    },
                    {
                      key: "level2",
                      title: isMdsMode ? "Samples" : "Items",
                      icon: <BadgeInfo className="h-4 w-4 text-sky-600" />,
                      count: itemsQuery.data?.length ?? undefined,
                      isDisabled: !selectedChunk,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel2}
                            onValueChange={setFilterLevel2}
                            placeholder={isMdsMode ? "Filter samples…" : "Filter items…"}
                            ariaLabel={isMdsMode ? "Filter samples" : "Filter items"}
                          />
                          <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                            {(() => {
                              const items = itemsQuery.data ?? [];
                              const visible = items.filter((item) =>
                                matchesFilter(`${isMdsMode ? "sample" : "item"} ${item.itemIndex}`, level2Needle),
                              );
                              if (visible.length) {
                                return visible.map((item) => (
                                  <div
                                    key={item.itemIndex}
                                    className={cn(
                                      "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                      selectedItem?.itemIndex === item.itemIndex
                                        ? "bg-sky-50/60"
                                        : "hover:bg-black/[0.03]",
                                    )}
                                    onClick={() => {
                                      setFilterLevel3("");
                                      selectItem(item.itemIndex);
                                      setExplorerTabKey("level3");
                                    }}
                                  >
                                    <div className="font-semibold text-slate-900">
                                      {isMdsMode ? "Sample" : "Item"} {item.itemIndex}
                                    </div>
                                    <div className="flex items-center gap-2 whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                      <span>
                                        {item.fields.length.toLocaleString()} {isMdsMode ? "fields" : "leaves"}
                                      </span>
                                      <span className="text-slate-400">·</span>
                                      <span>{formatBytes(item.totalBytes)}</span>
                                    </div>
                                  </div>
                                ));
                              }
                              if (!items.length) return <EmptyState hint="Pick a shard to list its samples." />;
                              return <EmptyState hint="No matches. Try a different filter." />;
                            })()}
                          </ScrollArea>
                        </div>
                      ),
                      hint: isMdsMode ? "Pick a sample to inspect its fields." : "Pick an item to inspect its leaves.",
                    },
                    {
                      key: "level3",
                      title: "Fields",
                      icon: <Play className="h-4 w-4 text-cyan-600" />,
                      count: selectedItem?.fields.length ?? undefined,
                      isDisabled: !selectedItem,
                      content: (
                        <div className="flex flex-1 min-h-0 flex-col gap-2">
                          <ListFilterInput
                            value={filterLevel3}
                            onValueChange={setFilterLevel3}
                            placeholder="Filter fields…"
                            ariaLabel="Filter local fields"
                          />
                          {selectedItem ? (
                            <ScrollArea className="flex-1 min-h-0 overflow-x-hidden rounded-xl bg-white/55 ring-1 ring-black/[0.05]">
                              {(() => {
                                const fields = selectedItem.fields ?? [];
                                const visible = fields.filter((field) => {
                                  const format = indexQuery.data?.dataFormat[field.fieldIndex] ?? "unknown";
                                  return matchesFilter(`#${field.fieldIndex} ${format}`, level3Needle);
                                });
                                if (visible.length) {
                                  return visible.map((field) => {
                                    const format = indexQuery.data?.dataFormat[field.fieldIndex] ?? "unknown";
                                    return (
                                      <div
                                        key={field.fieldIndex}
                                        className={cn(
                                          "grid min-w-0 cursor-pointer grid-cols-[1fr_auto] items-center gap-2 border-b border-black/[0.05] px-2 py-1.5 text-[13px] transition-colors",
                                          selectedField?.fieldIndex === field.fieldIndex
                                            ? "bg-cyan-50/60"
                                            : "hover:bg-black/[0.03]",
                                        )}
                                        onClick={() => {
                                          selectField(field.fieldIndex);
                                        }}
                                        onDoubleClick={() => openFieldMutation.mutate()}
                                      >
                                        <div className="flex min-w-0 items-center gap-2">
                                          <div className="shrink-0 font-semibold text-slate-900">#{field.fieldIndex}</div>
                                          <Badge
                                            variant="secondary"
                                            className="min-w-0 max-w-full bg-slate-100/80 text-slate-600"
                                          >
                                            <span className="truncate">{format}</span>
                                          </Badge>
                                        </div>
                                        <div className="whitespace-nowrap text-[11px] text-slate-600 tabular-nums">
                                          {formatBytes(field.size)}
                                        </div>
                                      </div>
                                    );
                                  });
                                }
                                if (!fields.length) return <EmptyState hint="Select an item to see its fields." />;
                                return <EmptyState hint="No matches. Try a different filter." />;
                              })()}
                            </ScrollArea>
                          ) : (
                            <EmptyState hint="Select an item to see its fields." />
                          )}
                        </div>
                      ),
                      hint: "Pick a field to preview in the inspector.",
                    },
                  ]}
                />
              </motion.div>

              <motion.div variants={panelVariants} className="min-w-0 h-full min-h-0">
                <InspectorPanel
                  title="Inspector"
                  subtitle={localInspectorSubtitle}
                  meta={localPreviewMeta}
                  showMeta={false}
                  previewContent={renderLocalPreview()}
                  logMessage={logMessage}
                  busy={busy}
                  errorMessage={errorMessage}
                  logDockOpen={logDockOpen}
                  onToggleLogDock={() => setLogDockOpen((prev) => !prev)}
                  onCopyLog={() => copyText(logMessage)}
                  onClearLog={() => setStatusMessage(null)}
                />
              </motion.div>
            </>
          )}
        </motion.div>

      </div>
    </main>
  );
}

type ExplorerTabKey = "level1" | "level2" | "level3";

function ExplorerPanel({
  title,
  subtitle,
  icon,
  meta,
  tabKey,
  onTabChange,
  tabs,
}: {
  title: string;
  subtitle?: string | null;
  icon: ReactNode;
  meta: string[];
  tabKey: ExplorerTabKey;
  onTabChange: (next: ExplorerTabKey) => void;
  tabs: Array<{
    key: ExplorerTabKey;
    title: string;
    icon: ReactNode;
    count?: string | number;
    isDisabled?: boolean;
    content: ReactNode;
    hint?: string;
  }>;
}) {
	  const metaLine = (meta.length ? meta : ["—"]).join(" › ");
	  const desktopGridTemplate =
	    tabs.length === 1
	      ? "grid-cols-1"
	      : tabs.length === 2
	        ? "grid-cols-2"
	        : "grid-cols-3";
	  return (
	    <div className="min-w-0 flex h-full flex-col overflow-hidden rounded-2xl bg-white/55 shadow-[var(--shadow-soft)] backdrop-blur ring-1 ring-black/5">
      <div className="flex items-center gap-2 px-3 py-2 text-[11px] text-slate-600">
        <div className="flex min-w-0 items-center gap-2">
          <span className="text-slate-700">{icon}</span>
          <span className="font-semibold text-slate-900">{title}</span>
          {subtitle ? <span className="hidden sm:inline text-slate-500">{subtitle}</span> : null}
        </div>
        <div className="ml-auto max-w-[55%] truncate text-slate-500" title={metaLine}>
          {metaLine}
        </div>
      </div>

	      <div className={cn("hidden lg:grid flex-1 min-h-0 divide-x divide-black/5", desktopGridTemplate)}>
	        {tabs.map((tab) => (
	          <div
	            key={tab.key}
	            className={cn("flex min-w-0 min-h-0 flex-col", tab.isDisabled ? "opacity-50" : "")}
	          >
	            <div className="flex items-center gap-2 px-3 py-2 text-xs font-semibold text-slate-700">
	              <span className="text-slate-600">{tab.icon}</span>
	              <span className="truncate">{tab.title}</span>
              {tab.count !== undefined ? (
                <span className="ml-auto text-[11px] font-semibold text-slate-500">{tab.count}</span>
	              ) : null}
	            </div>
	            <div className="flex min-w-0 min-h-0 flex-1 flex-col overflow-hidden px-2 pb-2">
	              <div className="flex h-full min-h-0 flex-col gap-2">
	                {tab.content}
	                {tab.hint ? (
	                  <div className="flex items-center gap-2 px-1 text-[11px] text-slate-500">
                    <ArrowRight className="h-3.5 w-3.5 text-slate-400" />
                    {tab.hint}
                  </div>
                ) : null}
              </div>
            </div>
          </div>
        ))}
      </div>

      <div className="flex flex-1 min-h-0 flex-col overflow-hidden px-3 pb-3 lg:hidden">
        <Tabs
          aria-label="Explorer tabs"
          variant="solid"
          fullWidth
          selectedKey={tabKey}
          onSelectionChange={(key) => {
            const next = String(key);
            if (next === "level1" || next === "level2" || next === "level3") {
              onTabChange(next);
            }
          }}
          classNames={{
            base: "shrink-0",
            tabList: "shrink-0 rounded-full bg-black/[0.04] p-1 ring-1 ring-black/[0.04]",
            tab: "h-9 px-3 text-xs font-semibold text-slate-600 data-[selected=true]:text-slate-900",
            tabContent: "gap-2",
            cursor: "rounded-full bg-white/80 shadow-sm",
            panel: "flex-1 min-h-0 overflow-hidden pt-2",
          }}
        >
          {tabs.map((tab) => (
            <Tab
              key={tab.key}
              isDisabled={tab.isDisabled}
              title={
                <div className="flex items-center gap-2">
                  {tab.icon}
                  <span>{tab.title}</span>
                  {tab.count !== undefined ? (
                    <Badge variant="secondary" className="bg-white/75 text-slate-600">
                      {tab.count}
                    </Badge>
                  ) : null}
                </div>
              }
            >
              <div className="flex h-full min-h-0 flex-col gap-2">
                {tab.content}
                {tab.hint ? (
                  <div className="flex items-center gap-2 text-xs text-slate-500">
                    <ArrowRight className="h-3.5 w-3.5 text-slate-400" />
                    {tab.hint}
                  </div>
                ) : null}
              </div>
            </Tab>
          ))}
        </Tabs>
      </div>
    </div>
  );
}

function InspectorPanel({
  title,
  subtitle,
  meta,
  showMeta = true,
  previewContent,
  logMessage,
  busy,
  errorMessage,
  logDockOpen,
  onToggleLogDock,
  onCopyLog,
  onClearLog,
}: {
  title: string;
  subtitle?: string | null;
  meta: string[];
  showMeta?: boolean;
  previewContent: ReactNode;
  logMessage: string;
  busy: boolean;
  errorMessage: string | null;
  logDockOpen: boolean;
  onToggleLogDock: () => void;
  onCopyLog: () => void;
  onClearLog: () => void;
}) {
  const metaLine = (meta.length ? meta : ["no selection"]).join(" › ");
  const statusLabel = busy ? "Working" : errorMessage ? "Error" : "Ready";
  const statusBadgeClass = errorMessage
    ? "bg-rose-100/80 text-rose-700"
    : busy
      ? "bg-amber-100/80 text-amber-800"
      : "bg-emerald-100/80 text-emerald-800";
  const logSummary = useMemo(() => {
    const trimmed = logMessage.trim();
    if (!trimmed) return "—";
    const lines = trimmed.split("\n").map((l) => l.trim()).filter(Boolean);
    return lines[0] ?? trimmed;
  }, [logMessage]);
  return (
    <div className="min-w-0 flex h-full flex-col overflow-hidden rounded-2xl bg-white/55 shadow-[var(--shadow-soft)] backdrop-blur ring-1 ring-black/5">
      <div className="flex items-center gap-2 px-3 py-2 text-[11px] text-slate-600">
        <div className="flex min-w-0 items-center gap-2">
          <Sparkles className="h-4 w-4 text-emerald-600" />
          <span className="font-semibold text-slate-900">{title}</span>
          {subtitle ? <span className="hidden sm:inline text-slate-500">{subtitle}</span> : null}
        </div>
        {showMeta ? (
          <div className="ml-auto max-w-[60%] truncate text-slate-500" title={metaLine}>
            {metaLine}
          </div>
        ) : null}
      </div>

      <div className="flex flex-1 min-h-0 flex-col overflow-hidden px-3 pb-3">
        <div className="flex flex-1 min-h-0 flex-col overflow-hidden">
          {previewContent}
        </div>

        <div className="mt-2 shrink-0 overflow-hidden rounded-xl bg-white/40 ring-1 ring-black/[0.05]">
          <button
            type="button"
            className={cn(
              "flex w-full items-center gap-3 px-3 py-2 text-left transition",
              logDockOpen ? "bg-white/35" : "hover:bg-white/55",
            )}
            onClick={onToggleLogDock}
          >
            <div className="flex items-center gap-2">
              <Terminal className="h-4 w-4 text-slate-600" />
              <span className="text-xs font-semibold text-slate-700">Log</span>
              <Badge variant="secondary" className={cn("text-[11px] font-semibold", statusBadgeClass)}>
                {statusLabel}
              </Badge>
            </div>
            <div className="ml-auto flex min-w-0 items-center gap-2 text-xs text-slate-500">
              <span className="truncate">{logSummary}</span>
              <ChevronRight className={cn("h-4 w-4 shrink-0 text-slate-400 transition", logDockOpen ? "rotate-90" : "")} />
            </div>
          </button>

          {logDockOpen ? (
            <div className="space-y-3 border-t border-black/[0.06] bg-white/35 px-3 py-2">
              <ScrollArea
                className={cn(
                  "max-h-56 rounded-lg px-3 py-2 text-xs select-text cursor-text",
                  errorMessage ? "bg-rose-50/70 text-rose-700" : "bg-white/70 text-slate-700",
                )}
              >
                <pre className="whitespace-pre-wrap break-words font-mono">{logMessage}</pre>
              </ScrollArea>
              <div className="flex flex-wrap items-center justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Button size="sm" variant="ghost" onClick={onCopyLog}>
                    <Copy className="mr-1 h-3.5 w-3.5" />
                    Copy
                  </Button>
                  <Button size="sm" variant="ghost" onClick={onClearLog}>
                    Clear
                  </Button>
                </div>
                <div className="flex items-center gap-2 text-xs text-slate-500">
                  {busy ? <Loader2 className="h-3.5 w-3.5 animate-spin text-amber-600" /> : null}
                  {errorMessage ? <TriangleAlert className="h-3.5 w-3.5 text-rose-500" /> : null}
                  <span className="whitespace-nowrap">{busy ? "Working" : errorMessage ? "Resolve and retry" : "Idle"}</span>
                </div>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
	}

	function MediaPreviewPanel({
	  meta,
	  onOpen,
	  openDisabled,
	  openTooltip,
	  onCopy,
	  copyPayload,
	  below,
	  children,
	}: {
	  meta: string;
	  onOpen?: null | (() => void);
	  openDisabled?: boolean;
	  openTooltip?: string;
	  onCopy?: null | ((text: string) => void);
	  copyPayload?: string;
	  below?: ReactNode;
	  children: ReactNode;
	}) {
	  const copyTextPayload = (copyPayload ?? "").trim();
	  return (
	    <div className="flex h-full min-h-0 flex-col gap-2">
	      <div className="flex items-center justify-between gap-2 shrink-0">
	        <div className="text-xs font-semibold text-slate-700">Preview</div>
	        <div className="flex items-center gap-1">
	          {meta ? (
	            <Badge variant="secondary" className="bg-white/70 text-slate-600">
	              {meta}
	            </Badge>
	          ) : null}
	          {onOpen ? (
	            <Tooltip content={openTooltip ?? "Open in default app"} showArrow placement="bottom-end">
	              <div>
	                <Button
	                  type="button"
	                  isIconOnly
	                  size="sm"
	                  variant="ghost"
	                  className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
	                  onClick={onOpen}
	                  aria-label="Open in default app"
	                  disabled={openDisabled}
	                >
	                  <ArrowUpRightFromSquare className="h-4 w-4" />
	                </Button>
	              </div>
	            </Tooltip>
	          ) : null}
	          {onCopy && copyTextPayload ? (
	            <Button
	              type="button"
	              isIconOnly
	              size="sm"
	              variant="ghost"
	              className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
	              onClick={() => onCopy(copyTextPayload)}
	              aria-label="Copy preview"
	            >
	              <Copy className="h-4 w-4" />
	            </Button>
	          ) : null}
	        </div>
	      </div>

	      <div className="flex-1 min-h-0 overflow-hidden rounded-lg bg-white/60 ring-1 ring-black/[0.05]">
	        {children}
	      </div>

	      {below ? <div className="shrink-0">{below}</div> : null}
	    </div>
	  );
	}

	function PreviewPanel({
	  preview,
	  onCopy,
	  onRequestAudioPreview,
	  onOpen,
  openDisabled,
  openTooltip,
  onAudioError,
  audioLabel,
}: {
  preview: FieldPreview;
  onCopy: (text: string) => void;
  onRequestAudioPreview: null | (() => Promise<{ src: string; ext: string }>);
  onOpen?: null | (() => void);
  openDisabled?: boolean;
  openTooltip?: string;
  onAudioError?: (message: string) => void;
  audioLabel?: string;
}) {
  const [audioSource, setAudioSource] = useState<{ src: string; type?: string } | null>(null);
  const [audioPreparing, setAudioPreparing] = useState(false);
  const [audioError, setAudioError] = useState<string | null>(null);
  const audioRef = useRef<HTMLAudioElement | null>(null);

  const ext = (preview.guessedExt ?? "").trim().replace(/^\./, "").toLowerCase();
  const supportsAudio = ["wav", "mp3", "flac", "sph", "m4a", "ogg", "opus", "aac"].includes(ext);
  const looksLikeImage = ["png", "jpg", "jpeg", "gif", "webp", "bmp", "tiff"].includes(ext);
  const canTryAudio =
    Boolean(onRequestAudioPreview) && (supportsAudio || (preview.isBinary && !looksLikeImage));

  const prepareAndPlayAudio = async () => {
    if (!onRequestAudioPreview || audioPreparing) return;
    if (audioSource) {
      void audioRef.current?.play().catch(() => undefined);
      return;
    }
    setAudioPreparing(true);
    setAudioError(null);
    try {
      const prepared = await onRequestAudioPreview();
      setAudioSource({ src: prepared.src, type: audioMimeFromExt(prepared.ext) });
      setTimeout(() => {
        audioRef.current?.load();
        void audioRef.current?.play().catch(() => undefined);
      }, 0);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error("Audio preview failed:", err);
      setAudioError(message || "Audio preview failed.");
      onAudioError?.(message || "Audio preview failed.");
    } finally {
      setAudioPreparing(false);
    }
  };

  const copyPayload = preview.previewText ? preview.previewText : preview.hexSnippet ? `Hex: ${preview.hexSnippet}` : "";
  const showAudioPanel = Boolean(audioSource || audioError || canTryAudio);
  const hasTextPreview = Boolean(preview.previewText && preview.previewText.trim());
  return (
    <div className="flex h-full min-h-0 flex-col gap-2">
      <div className="flex items-center justify-between gap-2 shrink-0">
        <div className="text-xs font-semibold text-slate-700">Preview</div>
        <div className="flex items-center gap-1">
          <Badge variant="secondary" className="bg-white/70 text-slate-600">
            {preview.guessedExt ? `.${preview.guessedExt}` : "unknown"} · {formatBytes(preview.size)} ·{" "}
            {preview.isBinary ? "binary" : "text"}
          </Badge>
          {audioPreparing ? <Loader2 className="h-4 w-4 animate-spin text-slate-500" /> : null}
          {canTryAudio ? (
            <Tooltip content="Audio preview" showArrow placement="bottom-end">
              <div>
                <Button
                  type="button"
                  isIconOnly
                  size="sm"
                  variant="ghost"
                  className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
                  onClick={() => void prepareAndPlayAudio()}
                  aria-label="Audio preview"
                >
                  <Play className="h-4 w-4" />
                </Button>
              </div>
            </Tooltip>
          ) : null}
          {onOpen ? (
            <Tooltip content={openTooltip ?? "Open in default app"} showArrow placement="bottom-end">
              <div>
                <Button
                  type="button"
                  isIconOnly
                  size="sm"
                  variant="ghost"
                  className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
                  onClick={onOpen}
                  aria-label="Open in default app"
                  disabled={openDisabled}
                >
                  <ArrowUpRightFromSquare className="h-4 w-4" />
                </Button>
              </div>
            </Tooltip>
          ) : null}
          <Button
            type="button"
            isIconOnly
            size="sm"
            variant="ghost"
            className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
            onClick={() => onCopy(copyPayload)}
            aria-label="Copy preview"
          >
            <Copy className="h-4 w-4" />
          </Button>
        </div>
      </div>

      {showAudioPanel ? (
        <div className="shrink-0 space-y-2">
          {audioLabel ? <div className="text-xs text-slate-600">{audioLabel}</div> : null}
          <div
            className={cn(
              "relative rounded-lg bg-white/60 p-2 ring-1 ring-black/[0.05]",
              canTryAudio && !audioSource ? "cursor-pointer" : "",
            )}
            onClick={() => {
              if (!audioSource) void prepareAndPlayAudio();
            }}
          >
            <audio
              ref={audioRef}
              controls
              preload="none"
              className={cn("h-10 w-full", !audioSource ? "pointer-events-none opacity-70" : "")}
            >
              {audioSource ? <source src={audioSource.src} type={audioSource.type} /> : null}
            </audio>
          </div>
          {audioError ? <div className="text-xs text-amber-700">{audioError}</div> : null}
        </div>
      ) : null}

      <ScrollArea className="flex-1 min-h-0 rounded-lg bg-white/60 ring-1 ring-black/[0.05]">
        <pre className="whitespace-pre-wrap break-all px-2 py-2 text-[11px] text-slate-800 font-mono select-text cursor-text">
          {hasTextPreview ? preview.previewText : `Hex: ${preview.hexSnippet}`}
        </pre>
      </ScrollArea>
    </div>
  );
}

function JsonPreviewPanel({
  title,
  value,
  onCopy,
  onOpen,
  openDisabled,
  openTooltip,
}: {
  title: string;
  value: unknown;
  onCopy: () => void;
  onOpen?: null | (() => void);
  openDisabled?: boolean;
  openTooltip?: string;
}) {
  const [imageFailedSrc, setImageFailedSrc] = useState<string | null>(null);
  const [audioFailedSrc, setAudioFailedSrc] = useState<string | null>(null);
  const [videoFailedSrc, setVideoFailedSrc] = useState<string | null>(null);
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

  const videoCandidates = useMemo(() => {
    const guessIsVideo = (src: string, mime?: string) => {
      const t = (mime ?? "").trim().toLowerCase();
      if (t.startsWith("video/")) return true;
      const ext = extFromUrl(src) ?? "";
      return ["mp4", "webm", "mov", "m4v"].includes(ext);
    };

    const normalizeCandidate = (src: string, mime?: string) => {
      const cleaned = src.trim();
      if (!cleaned) return null;
      if (videoFailedSrc === cleaned) return null;
      const isAllowed =
        cleaned.startsWith("https://") || cleaned.startsWith("http://") || cleaned.startsWith("data:video/");
      if (!isAllowed) return null;
      if (!guessIsVideo(cleaned, mime)) return null;
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
      return out.length ? out.slice(0, 2) : null;
    }

    if (typeof value === "object") {
      const obj = value as Record<string, unknown>;
      const src = typeof obj.src === "string" ? obj.src : "";
      const type = typeof obj.type === "string" ? obj.type : undefined;
      return normalizeCandidate(src, type) ? [normalizeCandidate(src, type)!] : null;
    }

    return null;
  }, [videoFailedSrc, value]);

  return (
    <div className="flex h-full min-h-0 flex-col gap-2">
      <div className="flex items-center justify-between gap-2 shrink-0">
        <div className="min-w-0 truncate text-xs font-semibold text-slate-700">{title}</div>
        <div className="flex items-center gap-1">
          {onOpen ? (
            <Tooltip content={openTooltip ?? "Open in default app"} showArrow placement="bottom-end">
              <div>
                <Button
                  type="button"
                  isIconOnly
                  size="sm"
                  variant="ghost"
                  className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
                  onClick={onOpen}
                  aria-label="Open in default app"
                  disabled={openDisabled}
                >
                  <ArrowUpRightFromSquare className="h-4 w-4" />
                </Button>
              </div>
            </Tooltip>
          ) : null}
          <Button
            type="button"
            isIconOnly
            size="sm"
            variant="ghost"
            className="h-8 w-8 rounded-full bg-transparent text-slate-600 hover:bg-black/[0.05]"
            onClick={onCopy}
            aria-label="Copy JSON value"
          >
            <Copy className="h-4 w-4" />
          </Button>
        </div>
      </div>
      {audioCandidates ? (
        <div className="shrink-0 space-y-2">
          {audioCandidates.map((candidate) => (
            <div key={candidate.src} className="rounded-lg bg-white/60 p-2 ring-1 ring-black/[0.05]">
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
      {videoCandidates ? (
        <div className="shrink-0 space-y-2">
          {videoCandidates.map((candidate) => (
            <div key={candidate.src} className="rounded-lg bg-white/60 p-2 ring-1 ring-black/[0.05]">
              <video
                controls
                preload="metadata"
                className="w-full max-h-[360px] rounded-lg bg-slate-50"
                onError={() => setVideoFailedSrc(candidate.src)}
              >
                <source src={candidate.src} type={candidate.type} />
              </video>
            </div>
          ))}
        </div>
      ) : null}
      {imageCandidate ? (
        <div className="shrink-0 rounded-lg bg-white/60 p-2 ring-1 ring-black/[0.05]">
          <img
            src={imageCandidate.src}
            alt={title}
            width={imageCandidate.width}
            height={imageCandidate.height}
            loading="lazy"
            decoding="async"
            className="max-h-[360px] w-full rounded-lg object-contain bg-slate-50"
            onError={() => setImageFailedSrc(imageCandidate.src)}
          />
        </div>
      ) : null}
      <ScrollArea className="flex-1 min-h-0 rounded-lg bg-white/60 ring-1 ring-black/[0.05]">
        <pre className="whitespace-pre-wrap break-all px-2 py-2 text-[11px] text-slate-800 font-mono select-text cursor-text">
          {previewText}
        </pre>
      </ScrollArea>
    </div>
  );
}

function EmptyState({ hint }: { hint: string }) {
  return (
    <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
      <TriangleAlert className="h-4 w-4 text-slate-400" />
      <div className="max-w-[520px] leading-relaxed">{hint}</div>
    </div>
  );
}
