import { useEffect, useMemo, useRef, useState, type ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { motion } from "framer-motion";
import {
  ArrowUpRightFromSquare,
  BadgeInfo,
  ChevronLeft,
  ChevronRight,
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
import { CopyButton } from "@/components/ui/copy-button";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";
import { Tooltip, TooltipContent, TooltipTrigger } from "@/components/ui/tooltip";
import { cn } from "@/lib/utils";
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
  readLastIndex,
  readPreferredOpenerForExt,
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
  zenodoZipInlineEntryMedia,
  zenodoZipListEntries,
  zenodoZipOpenEntry,
  zenodoZipPeekEntry,
  type FieldMeta,
  type FieldPreview,
  type HfConfigSummary,
  type HfDatasetPreview,
  type IndexSummary,
  type InlineMediaResponse,
  type ItemMeta,
  type OpenLeafResponse,
  type WdsDirSummary,
  type WdsSampleListResponse,
  type ZenodoRecordSummary,
  type ZenodoTarEntryListResponse,
  type ZenodoTarEntrySummary,
  type ZenodoZipEntrySummary,
} from "@/lib/tauri-api";
import { useViewerStore } from "@/store/viewer";

const HF_PAGE_SIZE = 25;
const WDS_PAGE_SIZE = 50;
const ZENODO_TAR_PAGE_SIZE = 25;

const EMPTY_ROWS: unknown[] = [];

type SourceKind = "auto" | "litdata" | "mds" | "wds" | "hf" | "zenodo";
type EffectiveKind = Exclude<SourceKind, "auto">;
type ZenodoEntry = ZenodoTarEntrySummary | ZenodoZipEntrySummary;

function formatBytes(value: number) {
  if (!Number.isFinite(value)) return "0 B";
  const units = ["B", "KB", "MB", "GB", "TB"];
  let v = value;
  let idx = 0;
  while (v >= 1024 && idx < units.length - 1) {
    v /= 1024;
    idx += 1;
  }
  return `${v.toFixed(v >= 10 || v < 1 ? 0 : 1)} ${units[idx]}`;
}

function audioMimeFromExt(value: string) {
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
}

function videoMimeFromExt(value: string) {
  switch (value) {
    case "mp4":
      return "video/mp4";
    case "webm":
      return "video/webm";
    case "mov":
      return "video/quicktime";
    default:
      return undefined;
  }
}

function isInlineMediaExt(value: string) {
  const ext = value.trim().replace(/^\\./, "").toLowerCase();
  if (!ext) return false;
  if (audioMimeFromExt(ext)) return true;
  if (videoMimeFromExt(ext)) return true;
  return ["png", "jpg", "jpeg", "gif", "webp", "bmp", "svg"].includes(ext);
}

function buildPreviewMeta(preview: FieldPreview | null) {
  if (!preview) return [];
  const ext = (preview.guessedExt ?? "").trim().replace(/^\\./, "");
  const typeLabel = ext ? `.${ext}` : "unknown";
  return [typeLabel, formatBytes(preview.size), preview.isBinary ? "binary" : "text"];
}

function buildPreviewBodyText(preview: FieldPreview) {
  if (preview.isBinary) return `Hex: ${preview.hexSnippet}`;
  return preview.previewText ?? "";
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

function normalizeFilter(value: string) {
  return value.trim().toLowerCase();
}

function matchesFilter(haystack: string, needle: string) {
  if (!needle) return true;
  return haystack.toLowerCase().includes(needle);
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

function looksLikeHfInput(value: string) {
  const v = value.trim();
  if (!v) return false;
  if (v.startsWith("hf://datasets/")) return true;
  if (v.startsWith("https://huggingface.co/datasets/") || v.startsWith("http://huggingface.co/datasets/")) return true;
  if (v.startsWith("https://hf.co/datasets/") || v.startsWith("http://hf.co/datasets/")) return true;
  return false;
}

function looksLikeZenodoInput(value: string) {
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
}

function displayHfDatasetId(value: string) {
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
}

type HfMediaInfo = { src: string; type: string } | null;

function inferMediaTypeFromFieldName(fieldName: string): string | null {
  const lower = fieldName.toLowerCase();
  if (
    lower === "mp4" ||
    lower === "video" ||
    lower.includes("video") ||
    lower.endsWith(".mp4") ||
    lower.endsWith(".webm") ||
    lower.endsWith(".mov")
  ) {
    return "video/mp4";
  }
  if (lower === "mp3" || lower === "wav" || lower === "audio" || lower === "flac" || lower.includes("audio")) {
    if (lower.includes("wav")) return "audio/wav";
    if (lower.includes("flac")) return "audio/flac";
    if (lower.includes("mp3")) return "audio/mpeg";
    return "audio/mpeg";
  }
  if (lower === "png" || lower === "jpg" || lower === "jpeg" || lower === "image" || lower.includes("image")) {
    if (lower.includes("png")) return "image/png";
    return "image/jpeg";
  }
  return null;
}

function looksLikeBase64(value: string) {
  if (value.length < 100) return false;
  return /^[A-Za-z0-9+/=]+$/.test(value.slice(0, 1000));
}

function extractHfMedia(value: unknown, fieldName?: string): HfMediaInfo {
  if (!value) return null;

  if (Array.isArray(value) && value.length > 0) {
    const first = value[0];
    if (first && typeof first === "object" && "src" in first && typeof first.src === "string") {
      const type = "type" in first && typeof first.type === "string" ? first.type : "";
      return { src: first.src, type };
    }
  }

  if (typeof value === "object" && value !== null && "src" in value) {
    const obj = value as Record<string, unknown>;
    if (typeof obj.src === "string") {
      let type = typeof obj.type === "string" ? obj.type : "";
      if (!type) {
        if (typeof obj.width === "number" || typeof obj.height === "number") {
          type = "image";
        } else {
          const lower = obj.src.toLowerCase();
          if (
            lower.includes(".jpg") ||
            lower.includes(".jpeg") ||
            lower.includes(".png") ||
            lower.includes(".gif") ||
            lower.includes(".webp") ||
            lower.includes("/image/")
          ) {
            type = "image";
          } else if (
            lower.includes(".wav") ||
            lower.includes(".mp3") ||
            lower.includes(".flac") ||
            lower.includes(".ogg") ||
            lower.includes("/audio/")
          ) {
            type = "audio";
          } else if (
            lower.includes(".mp4") ||
            lower.includes(".webm") ||
            lower.includes(".mov") ||
            lower.includes("/video/")
          ) {
            type = "video";
          }
        }
      }
      return { src: obj.src, type };
    }
  }

  if (typeof value === "string" && (value.startsWith("http://") || value.startsWith("https://"))) {
    const lower = value.toLowerCase();
    if (lower.includes(".wav") || lower.includes(".mp3") || lower.includes(".flac") || lower.includes(".ogg")) {
      return { src: value, type: "audio" };
    }
    if (
      lower.includes(".png") ||
      lower.includes(".jpg") ||
      lower.includes(".jpeg") ||
      lower.includes(".gif") ||
      lower.includes(".webp")
    ) {
      return { src: value, type: "image" };
    }
    if (lower.includes(".mp4") || lower.includes(".webm") || lower.includes(".mov")) {
      return { src: value, type: "video" };
    }
  }

  if (typeof value === "string" && fieldName && looksLikeBase64(value)) {
    const inferredType = inferMediaTypeFromFieldName(fieldName);
    if (inferredType) {
      return { src: `data:${inferredType};base64,${value}`, type: inferredType };
    }
  }

  return null;
}

function isHfAudioMedia(media: HfMediaInfo) {
  if (!media) return false;
  return media.type.startsWith("audio") || media.type === "audio";
}

function isHfImageMedia(media: HfMediaInfo) {
  if (!media) return false;
  return media.type.startsWith("image") || media.type === "image";
}

function isHfVideoMedia(media: HfMediaInfo) {
  if (!media) return false;
  return media.type.startsWith("video") || media.type === "video";
}

function StatBlockLarge({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="flex flex-col items-center justify-center px-4 py-1 min-w-0">
      <span className="text-[10px] font-medium uppercase tracking-wider text-slate-400">{label}</span>
      <span className="text-lg font-bold tabular-nums text-slate-800 truncate max-w-[160px]" title={String(value)}>
        {String(value)}
      </span>
    </div>
  );
}

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
      className="bg-white/80 rounded-full h-9"
      placeholder={placeholder}
      value={value}
      onChange={(e) => onValueChange(e.target.value)}
      isClearable
      onClear={() => onValueChange("")}
      startContent={<Search className="h-4 w-4 text-slate-500" />}
      aria-label={ariaLabel}
    />
  );
}

function SelectableRowButton({
  isSelected,
  onClick,
  className,
  ariaLabel,
  children,
}: {
  isSelected: boolean;
  onClick: () => void;
  className: string;
  ariaLabel: string;
  children: ReactNode;
}) {
  return (
    <Button
      type="button"
      variant="ghost"
      className={cn(
        "grid h-auto min-w-0 w-full rounded-none border-b border-black/[0.04] px-3 py-2 text-left text-[13px] font-normal transition-all duration-150",
        isSelected ? "bg-emerald-50/60 hover:bg-emerald-50/60" : "hover:bg-black/[0.03]",
        className,
      )}
      onClick={onClick}
      aria-label={ariaLabel}
    >
      {children}
    </Button>
  );
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
    wdsSelectedSampleKey,
    selectWdsSample,
    wdsSelectedMemberPath,
    wdsSelectedMemberName,
    selectWdsMember,
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

  const requestId = mode?.requestId ?? 0;
  const tauri = useMemo(() => isTauri(), []);

  const [hfToken, setHfToken] = useState<string | null>(null);
  const hfTokenMasked = hfToken ? `…${hfToken.slice(-6)}` : null;
  const [hfTokenDialogOpen, setHfTokenDialogOpen] = useState(false);
  const [hfTokenDraft, setHfTokenDraft] = useState("");
  const [logDockOpen, setLogDockOpen] = useState(false);
  const [sourceKind, setSourceKind] = useState<SourceKind>("auto");

  const handleSaveHfToken = async () => {
    const trimmed = hfTokenDraft.trim();
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
  };

  const handleClearSavedHfToken = () => {
    void (async () => {
      try {
        await clearHfToken();
        setHfToken(null);
        setHfTokenDialogOpen(false);
      } catch (err) {
        setStatusMessage(err instanceof Error ? err.message : "Unable to clear token.");
      }
    })();
  };

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

  // Reset sourceKind to auto when user changes input
  useEffect(() => {
    if (sourceKind !== "auto" && chunkSelection.length === 0) {
      setSourceKind("auto");
    }
  }, [sourceInput, chunkSelection.length, sourceKind]);

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
    if (hfTokenDialogOpen) {
      setHfTokenDraft(hfToken ?? "");
    }
  }, [hfToken, hfTokenDialogOpen]);

  useEffect(() => {
    setLogDockOpen(false);
  }, [requestId]);

  const indexQuery = useQuery<IndexSummary>({
    queryKey: ["index-summary", requestId],
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
    queryKey: ["wds-dir", requestId],
    enabled: Boolean(isWdsMode),
    queryFn: () => {
      if (!mode || mode.kind !== "webdataset-dir") throw new Error("No WebDataset selected.");
      return wdsLoadDir(mode.dirPath);
    },
  });

  // HuggingFace cache state - must be declared before hfQuery to allow use in queryFn
  const hfDatasetInput = isHfMode ? mode.input : null;
  const [hfSplitsCache, setHfSplitsCache] = useState<{ input: string; configs: HfConfigSummary[] } | null>(null);
  const [hfSelectedCache, setHfSelectedCache] = useState<{ input: string; config: string; split: string } | null>(null);

  const hfQuery = useQuery<HfDatasetPreview>({
    queryKey: [
      "hf-preview",
      isHfMode ? mode.input : null,
      isHfMode ? requestId : 0,
      hfConfigOverride,
      hfSplitOverride,
      hfOffset,
      HF_PAGE_SIZE,
      hfTokenMasked,
    ],
    enabled: Boolean(isHfMode && tauri),
    queryFn: () => {
      if (!mode || mode.kind !== "huggingface") throw new Error("No dataset selected.");
      // Use override if set, otherwise use cached selection (for faster pagination).
      // Only pass config/split if we have both (backend optimization skips splits API).
      const cachedSelection = hfSelectedCache?.input === mode.input ? hfSelectedCache : null;
      const effectiveConfig = hfConfigOverride ?? cachedSelection?.config;
      const effectiveSplit = hfSplitOverride ?? cachedSelection?.split;
      return hfDatasetPreview({
        input: mode.input,
        config: effectiveConfig ?? undefined,
        split: effectiveSplit ?? undefined,
        offset: hfOffset,
        length: HF_PAGE_SIZE,
        token: hfToken,
      });
    },
    staleTime: 60 * 1000,
  });

  const zenodoQuery = useQuery<ZenodoRecordSummary>({
    queryKey: ["zenodo-record", isZenodoMode ? mode.input : null, isZenodoMode ? requestId : 0],
    enabled: Boolean(isZenodoMode && tauri),
    queryFn: () => {
      if (!mode || mode.kind !== "zenodo") throw new Error("No Zenodo record selected.");
      return zenodoRecordSummary({ input: mode.input });
    },
    staleTime: 5 * 60 * 1000,
  });

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
    // Only update splits cache if configs is non-empty (initial load).
    // Pagination responses have empty configs to skip the splits API call.
    if (hfQuery.data.configs.length > 0) {
      setHfSplitsCache({ input: hfDatasetInput, configs: hfQuery.data.configs });
    }
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
      setStatusMessage(
        `Loaded ${indexQuery.data.chunks.length} ${noun}${indexQuery.data.chunks.length === 1 ? "" : "s"}.`,
      );
    }
  }, [indexQuery.data, isMdsMode, setStatusMessage]);

  useEffect(() => {
    if (wdsDirQuery.data) {
      setStatusMessage(
        `Loaded ${wdsDirQuery.data.shards.length} shard${wdsDirQuery.data.shards.length === 1 ? "" : "s"}.`,
      );
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
      setStatusMessage(
        `Loaded Zenodo record ${zenodoQuery.data.recordId} · ${count} file${count === 1 ? "" : "s"}.`,
      );
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

  const selectedZenodoFileIsZip = Boolean(
    isZenodoMode && selectedZenodoFile && selectedZenodoFile.key.toLowerCase().endsWith(".zip"),
  );
  const selectedZenodoFileIsTar = Boolean(
    isZenodoMode && selectedZenodoFile && looksLikeTarFilename(selectedZenodoFile.key),
  );

  // Query for items in selected chunk (LitData / MDS)
  const itemsQuery = useQuery<ItemMeta[]>({
    queryKey: ["chunk-items", selectedChunk?.filename ?? null, requestId],
    enabled: Boolean(isLitdataMode && selectedChunk && indexQuery.data),
    queryFn: () => {
      if (!selectedChunk || !indexQuery.data) throw new Error("No chunk selected.");
      return listChunkItems({ indexPath: indexQuery.data.indexPath, chunkFilename: selectedChunk.filename });
    },
  });

  const mdsItemsQuery = useQuery<ItemMeta[]>({
    queryKey: ["mds-samples", selectedChunk?.filename ?? null, requestId],
    enabled: Boolean(isMdsMode && selectedChunk && indexQuery.data),
    queryFn: () => {
      if (!selectedChunk || !indexQuery.data) throw new Error("No shard selected.");
      return mosaicmlListSamples({ indexPath: indexQuery.data.indexPath, shardFilename: selectedChunk.filename });
    },
  });

  // Query for WDS samples
  const wdsSamplesQuery = useQuery<WdsSampleListResponse>({
    queryKey: ["wds-samples", selectedShard?.filename ?? null, wdsOffset, requestId],
    enabled: Boolean(isWdsMode && selectedShard && wdsDirQuery.data),
    queryFn: () => {
      if (!selectedShard || !wdsDirQuery.data) throw new Error("No shard selected.");
      return wdsListSamples({
        dirPath: wdsDirQuery.data.dirPath,
        shardFilename: selectedShard.filename,
        offset: wdsOffset,
        length: WDS_PAGE_SIZE,
      });
    },
  });

  // Query for Zenodo tar/zip entries
  const zenodoTarEntriesQuery = useQuery<ZenodoTarEntryListResponse>({
    queryKey: ["zenodo-tar-entries", selectedZenodoFile?.contentUrl, zenodoEntriesOffset],
    enabled: Boolean(isZenodoMode && selectedZenodoFile && looksLikeTarFilename(selectedZenodoFile.key)),
    queryFn: () => {
      if (!selectedZenodoFile) throw new Error("No file selected.");
      return zenodoTarListEntries({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        offset: zenodoEntriesOffset,
        length: ZENODO_TAR_PAGE_SIZE,
      });
    },
    staleTime: 60 * 1000,
  });

  const zenodoZipEntriesQuery = useQuery<ZenodoZipEntrySummary[]>({
    queryKey: ["zenodo-zip-entries", selectedZenodoFile?.contentUrl],
    enabled: Boolean(isZenodoMode && selectedZenodoFileIsZip && selectedZenodoFile),
    queryFn: () => {
      if (!selectedZenodoFile) throw new Error("No file selected.");
      return zenodoZipListEntries({ contentUrl: selectedZenodoFile.contentUrl, filename: selectedZenodoFile.key });
    },
    staleTime: 60 * 1000,
  });

  // Selected item
  const selectedItem = useMemo(() => {
    if (selectedItemIndex === null) return null;
    if (isLitdataMode && itemsQuery.data) {
      return itemsQuery.data.find((item) => item.itemIndex === selectedItemIndex) ?? null;
    }
    return null;
  }, [isLitdataMode, itemsQuery.data, selectedItemIndex]);

  // Query for field preview
  const fieldPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["field-preview", selectedChunk?.filename ?? null, selectedItemIndex, selectedFieldIndex, requestId],
    enabled: Boolean(
      isLitdataMode && selectedChunk && selectedItemIndex !== null && selectedFieldIndex !== null && indexQuery.data,
    ),
    queryFn: () => {
      if (!selectedChunk || selectedItemIndex === null || selectedFieldIndex === null || !indexQuery.data)
        throw new Error("No field selected.");
      return peekField({
        indexPath: indexQuery.data.indexPath,
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItemIndex,
        fieldIndex: selectedFieldIndex,
      });
    },
  });

  const mdsFieldPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["mds-field-preview", selectedChunk?.filename ?? null, selectedItemIndex, selectedFieldIndex, requestId],
    enabled: Boolean(
      isMdsMode && selectedChunk && selectedItemIndex !== null && selectedFieldIndex !== null && indexQuery.data,
    ),
    queryFn: () => {
      if (!selectedChunk || selectedItemIndex === null || selectedFieldIndex === null || !indexQuery.data)
        throw new Error("No field selected.");
      return mosaicmlPeekField({
        indexPath: indexQuery.data.indexPath,
        shardFilename: selectedChunk.filename,
        itemIndex: selectedItemIndex,
        fieldIndex: selectedFieldIndex,
      });
    },
  });

  const wdsMemberPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["wds-member-preview", selectedShard?.filename ?? null, wdsSelectedMemberPath ?? null, requestId],
    enabled: Boolean(isWdsMode && selectedShard && wdsSelectedMemberPath && wdsDirQuery.data),
    queryFn: () => {
      if (!selectedShard || !wdsSelectedMemberPath || !wdsDirQuery.data) throw new Error("No member selected.");
      return wdsPeekMember({
        dirPath: wdsDirQuery.data.dirPath,
        shardFilename: selectedShard.filename,
        memberPath: wdsSelectedMemberPath,
      });
    },
  });

  // Build items list for column 2
  const items = useMemo(() => (isLitdataMode ? itemsQuery.data ?? [] : []), [isLitdataMode, itemsQuery.data]);
  const mdsItems = useMemo(() => (isMdsMode ? mdsItemsQuery.data ?? [] : []), [isMdsMode, mdsItemsQuery.data]);
  const wdsSamples = useMemo(() => (isWdsMode ? wdsSamplesQuery.data?.samples ?? [] : []), [isWdsMode, wdsSamplesQuery.data]);

  const wdsSelectedSample = useMemo(() => {
    if (!isWdsMode || !wdsSelectedSampleKey) return null;
    return wdsSamples.find((sample) => sample.key === wdsSelectedSampleKey) ?? null;
  }, [isWdsMode, wdsSamples, wdsSelectedSampleKey]);

  // Auto-select field by name when switching samples (preserve field selection)
  useEffect(() => {
    if (!isWdsMode || !wdsSelectedSample || !wdsSelectedMemberName) return;
    // Find a field with the same name in the new sample
    const matchingField = wdsSelectedSample.fields.find((f) => f.name === wdsSelectedMemberName);
    if (matchingField && matchingField.memberPath !== wdsSelectedMemberPath) {
      selectWdsMember(matchingField.memberPath, matchingField.name);
    }
  }, [isWdsMode, wdsSelectedSample, wdsSelectedMemberName, wdsSelectedMemberPath, selectWdsMember]);

  // HF rows
  const hfRows = useMemo(() => {
    if (!isHfMode || !hfQuery.data) return EMPTY_ROWS;
    return hfQuery.data.rows ?? EMPTY_ROWS;
  }, [hfQuery.data, isHfMode]);

  const hfSelectedRow = useMemo(() => {
    if (hfSelectedRowIndex === null || !hfRows.length) return null;
    return (hfRows[hfSelectedRowIndex] as Record<string, unknown>) ?? null;
  }, [hfRows, hfSelectedRowIndex]);

  // Zenodo entries
  const zenodoEntries = useMemo(() => {
    if (zenodoTarEntriesQuery.data) return zenodoTarEntriesQuery.data.entries;
    if (zenodoZipEntriesQuery.data) return zenodoZipEntriesQuery.data;
    return [];
  }, [zenodoTarEntriesQuery.data, zenodoZipEntriesQuery.data]);

  const selectedZenodoEntry = useMemo(() => {
    if (!zenodoSelectedEntryName) return null;
    return zenodoEntries.find((e: ZenodoEntry) => e.name === zenodoSelectedEntryName) ?? null;
  }, [zenodoEntries, zenodoSelectedEntryName]);

  const zenodoFilePreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-file-preview", selectedZenodoFile?.contentUrl ?? null, requestId],
    enabled: Boolean(isZenodoMode && tauri && selectedZenodoFile && !selectedZenodoFileIsTar && !selectedZenodoFileIsZip),
    queryFn: () => {
      if (!selectedZenodoFile) throw new Error("No Zenodo file selected.");
      return zenodoPeekFile({ contentUrl: selectedZenodoFile.contentUrl });
    },
  });

  const zenodoTarEntryPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-tar-entry-preview", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null, requestId],
    enabled: Boolean(isZenodoMode && tauri && selectedZenodoFileIsTar && selectedZenodoFile && selectedZenodoEntry && !selectedZenodoEntry.isDir),
    queryFn: () => {
      if (!selectedZenodoFile || !selectedZenodoEntry) throw new Error("No Zenodo entry selected.");
      return zenodoTarPeekEntry({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const zenodoZipEntryPreviewQuery = useQuery<FieldPreview>({
    queryKey: ["zenodo-zip-entry-preview", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null, requestId],
    enabled: Boolean(isZenodoMode && tauri && selectedZenodoFileIsZip && selectedZenodoFile && selectedZenodoEntry && !selectedZenodoEntry.isDir),
    queryFn: () => {
      if (!selectedZenodoFile || !selectedZenodoEntry) throw new Error("No Zenodo entry selected.");
      return zenodoZipPeekEntry({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const zenodoTarInlineMediaQuery = useQuery<InlineMediaResponse>({
    queryKey: ["zenodo-tar-inline-media", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null, zenodoTarEntryPreviewQuery.data?.guessedExt ?? null, requestId],
    enabled: Boolean(
      isZenodoMode &&
        tauri &&
        selectedZenodoFileIsTar &&
        selectedZenodoFile &&
        selectedZenodoEntry &&
        !selectedZenodoEntry.isDir &&
        zenodoTarEntryPreviewQuery.data?.guessedExt &&
        isInlineMediaExt(zenodoTarEntryPreviewQuery.data.guessedExt),
    ),
    queryFn: () => {
      if (!selectedZenodoFile || !selectedZenodoEntry) throw new Error("No Zenodo entry selected.");
      return zenodoTarInlineEntryMedia({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const zenodoZipInlineMediaQuery = useQuery<InlineMediaResponse>({
    queryKey: ["zenodo-zip-inline-media", selectedZenodoFile?.contentUrl ?? null, selectedZenodoEntry?.name ?? null, zenodoZipEntryPreviewQuery.data?.guessedExt ?? null, requestId],
    enabled: Boolean(
      isZenodoMode &&
        tauri &&
        selectedZenodoFileIsZip &&
        selectedZenodoFile &&
        selectedZenodoEntry &&
        !selectedZenodoEntry.isDir &&
        zenodoZipEntryPreviewQuery.data?.guessedExt &&
        isInlineMediaExt(zenodoZipEntryPreviewQuery.data.guessedExt),
    ),
    queryFn: () => {
      if (!selectedZenodoFile || !selectedZenodoEntry) throw new Error("No Zenodo entry selected.");
      return zenodoZipInlineEntryMedia({
        contentUrl: selectedZenodoFile.contentUrl,
        filename: selectedZenodoFile.key,
        entryName: selectedZenodoEntry.name,
      });
    },
  });

  const zenodoPreviewData =
    zenodoFilePreviewQuery.data ?? zenodoTarEntryPreviewQuery.data ?? zenodoZipEntryPreviewQuery.data ?? null;
  const zenodoInlineMediaData = zenodoTarInlineMediaQuery.data ?? zenodoZipInlineMediaQuery.data ?? null;
  const zenodoPreviewLoading =
    zenodoFilePreviewQuery.isLoading ||
    zenodoTarEntryPreviewQuery.isLoading ||
    zenodoZipEntryPreviewQuery.isLoading ||
    zenodoTarInlineMediaQuery.isLoading ||
    zenodoZipInlineMediaQuery.isLoading;

  // Field names for selected item
  const fieldNames = useMemo((): FieldMeta[] => {
    if (selectedItem) return selectedItem.fields;
    if (isMdsMode && selectedItemIndex !== null) {
      const mdsItem = mdsItems.find((item) => item.itemIndex === selectedItemIndex);
      if (mdsItem) return mdsItem.fields;
    }
    return [];
  }, [isMdsMode, mdsItems, selectedItem, selectedItemIndex]);

  // Audio preview
  const litdataAudioPreviewQuery = useQuery<{ path: string }>({
    queryKey: ["audio-preview-litdata", selectedChunk?.filename ?? null, selectedItemIndex, selectedFieldIndex, requestId],
    enabled: Boolean(isLitdataMode && selectedChunk && selectedItemIndex !== null && selectedFieldIndex !== null && indexQuery.data && fieldPreviewQuery.data?.guessedExt && audioMimeFromExt(fieldPreviewQuery.data.guessedExt)),
    queryFn: () => {
      if (!selectedChunk || selectedItemIndex === null || selectedFieldIndex === null || !indexQuery.data)
        throw new Error("No field selected.");
      return prepareAudioPreview({
        indexPath: indexQuery.data.indexPath,
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItemIndex,
        fieldIndex: selectedFieldIndex,
      });
    },
  });

  const mdsAudioPreviewQuery = useQuery<{ path: string }>({
    queryKey: ["audio-preview-mds", selectedChunk?.filename ?? null, selectedItemIndex, selectedFieldIndex, requestId],
    enabled: Boolean(isMdsMode && selectedChunk && selectedItemIndex !== null && selectedFieldIndex !== null && indexQuery.data && mdsFieldPreviewQuery.data?.guessedExt && audioMimeFromExt(mdsFieldPreviewQuery.data.guessedExt)),
    queryFn: () => {
      if (!selectedChunk || selectedItemIndex === null || selectedFieldIndex === null || !indexQuery.data)
        throw new Error("No field selected.");
      return mosaicmlPrepareAudioPreview({
        indexPath: indexQuery.data.indexPath,
        shardFilename: selectedChunk.filename,
        itemIndex: selectedItemIndex,
        fieldIndex: selectedFieldIndex,
      });
    },
  });

  const wdsAudioPreviewQuery = useQuery<{ path: string }>({
    queryKey: ["audio-preview-wds", selectedShard?.filename ?? null, wdsSelectedMemberPath ?? null, requestId],
    enabled: Boolean(isWdsMode && selectedShard && wdsSelectedMemberPath && wdsDirQuery.data && wdsMemberPreviewQuery.data?.guessedExt && audioMimeFromExt(wdsMemberPreviewQuery.data.guessedExt)),
    queryFn: () => {
      if (!selectedShard || !wdsSelectedMemberPath || !wdsDirQuery.data) throw new Error("No member selected.");
      return wdsPrepareAudioPreview({
        dirPath: wdsDirQuery.data.dirPath,
        shardFilename: selectedShard.filename,
        memberPath: wdsSelectedMemberPath,
      });
    },
  });

  const audioPreviewPath =
    litdataAudioPreviewQuery.data?.path ?? mdsAudioPreviewQuery.data?.path ?? wdsAudioPreviewQuery.data?.path ?? null;

  const localPreviewData = fieldPreviewQuery.data ?? mdsFieldPreviewQuery.data ?? wdsMemberPreviewQuery.data ?? null;
  const localPreviewLoading =
    fieldPreviewQuery.isLoading || mdsFieldPreviewQuery.isLoading || wdsMemberPreviewQuery.isLoading;
  const previewMetaSource = localPreviewData ?? zenodoPreviewData;

  const handleOpenResult = async (response: OpenLeafResponse) => {
    setStatusMessage(response.message);
    if (!response.needsOpener) return;

    const ext = response.ext.trim();
    try {
      const preferred = await readPreferredOpenerForExt(ext);
      if (preferred) {
        const message = await openPathWithApp({ path: response.path, appPath: preferred });
        setStatusMessage(message);
        return;
      }
    } catch (err) {
      setStatusMessage(err instanceof Error ? err.message : "Unable to open with the preferred app.");
    }

    try {
      const picked = await chooseOpenerApp();
      if (!picked) return;
      await savePreferredOpenerForExt(ext, picked);
      const message = await openPathWithApp({ path: response.path, appPath: picked });
      setStatusMessage(message);
    } catch (err) {
      setStatusMessage(err instanceof Error ? err.message : "Unable to open with the selected app.");
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
    indexQuery.data?.chunks?.reduce((acc, chunk) => acc + (Number.isFinite(chunk.chunkBytes) ? chunk.chunkBytes : 0), 0) ??
    0;

  const totalItems =
    indexQuery.data?.chunks?.reduce((acc, chunk) => acc + (Number.isFinite(chunk.chunkSize) ? chunk.chunkSize : 0), 0) ??
    0;

  const wdsTotalBytes =
    wdsDirQuery.data?.shards?.reduce((acc, shard) => acc + (Number.isFinite(shard.bytes) ? shard.bytes : 0), 0) ?? 0;

  const hfSplitPairs = useMemo(() => {
    // Prefer cached configs (always non-empty after initial load).
    // Pagination responses have empty configs array, so we fall back to cache.
    const cachedConfigs = hfSplitsCache?.input === hfDatasetInput ? hfSplitsCache.configs : [];
    const configs = cachedConfigs.length > 0 ? cachedConfigs : (hfQuery.data?.configs ?? []);
    return configs.flatMap((c) => c.splits.map((s) => ({ config: c.config, split: s })));
  }, [hfDatasetInput, hfQuery.data?.configs, hfSplitsCache]);

  const effectiveKind: EffectiveKind =
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

  const busy =
    indexQuery.isFetching ||
    wdsDirQuery.isFetching ||
    zenodoQuery.isFetching ||
    hfQuery.isFetching ||
    zenodoTarEntriesQuery.isFetching ||
    zenodoZipEntriesQuery.isFetching ||
    zenodoFilePreviewQuery.isFetching ||
    zenodoTarEntryPreviewQuery.isFetching ||
    zenodoZipEntryPreviewQuery.isFetching ||
    zenodoTarInlineMediaQuery.isFetching ||
    zenodoZipInlineMediaQuery.isFetching ||
    itemsQuery.isFetching ||
    mdsItemsQuery.isFetching ||
    wdsSamplesQuery.isFetching ||
    fieldPreviewQuery.isFetching ||
    mdsFieldPreviewQuery.isFetching ||
    wdsMemberPreviewQuery.isFetching ||
    litdataAudioPreviewQuery.isFetching ||
    mdsAudioPreviewQuery.isFetching ||
    wdsAudioPreviewQuery.isFetching;

  const latestError =
    indexQuery.error ||
    wdsDirQuery.error ||
    zenodoQuery.error ||
    hfQuery.error ||
    zenodoTarEntriesQuery.error ||
    zenodoZipEntriesQuery.error ||
    zenodoFilePreviewQuery.error ||
    zenodoTarEntryPreviewQuery.error ||
    zenodoZipEntryPreviewQuery.error ||
    zenodoTarInlineMediaQuery.error ||
    zenodoZipInlineMediaQuery.error ||
    itemsQuery.error ||
    mdsItemsQuery.error ||
    wdsSamplesQuery.error ||
    fieldPreviewQuery.error ||
    mdsFieldPreviewQuery.error ||
    wdsMemberPreviewQuery.error ||
    litdataAudioPreviewQuery.error ||
    mdsAudioPreviewQuery.error ||
    wdsAudioPreviewQuery.error ||
    undefined;
  const errorMessage = useMemo(() => {
    if (!latestError) return null;
    if (latestError instanceof Error) return latestError.message;
    if (typeof latestError === "string") return latestError;
    // Handle Tauri AppError objects: { code: "...", message: "..." }
    if (typeof latestError === "object" && latestError !== null) {
      const err = latestError as Record<string, unknown>;
      if (typeof err.message === "string") return err.message;
      try {
        return JSON.stringify(latestError);
      } catch {
        // fall through
      }
    }
    return String(latestError);
  }, [latestError]);

  const logMessage = errorMessage ? errorMessage : statusMessage ?? "Idle";

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

  const hfSelectedSplitLabel = useMemo(() => {
    const selectedConfig = (hfConfigOverride ?? hfQuery.data?.config ?? hfSelectedCache?.config ?? "").trim();
    const selectedSplit = (hfSplitOverride ?? hfQuery.data?.split ?? hfSelectedCache?.split ?? "").trim();
    if (!selectedConfig || !selectedSplit) return "—";
    return `${selectedConfig}/${selectedSplit}`;
  }, [hfConfigOverride, hfQuery.data?.config, hfQuery.data?.split, hfSelectedCache?.config, hfSelectedCache?.split, hfSplitOverride]);

  const headerStatsKind: "hf" | "zenodo" | "wds" | "local" = showHfStats ? "hf" : showZenodoStats ? "zenodo" : isWdsMode ? "wds" : "local";
  const headerStats = {
    hf: {
      datasetLabel: datasetPreviewLabel,
      splitLabel: hfSelectedSplitLabel,
      rowsLabel:
        hfQuery.data && hfQuery.data.numRowsTotal !== null && hfQuery.data.numRowsTotal !== undefined
          ? hfQuery.data.numRowsTotal.toLocaleString()
          : "—",
    },
    zenodo: {
      recordLabel: zenodoRecordLabel,
      filesLabel: String(zenodoQuery.data?.files.length ?? "—"),
      sizeLabel: zenodoTotalBytes ? formatBytes(zenodoTotalBytes) : "—",
    },
    wds: {
      shardsLabel: String(wdsDirQuery.data?.shards.length ?? "—"),
      sizeLabel: wdsTotalBytes ? formatBytes(wdsTotalBytes) : "—",
    },
    local: {
      shardsLabel: String(indexQuery.data?.chunks.length ?? "—"),
      itemsLabel: totalItems ? totalItems.toLocaleString() : "—",
      sizeLabel: totalBytes ? formatBytes(totalBytes) : "—",
    },
  };

  const [filterLevel1, setFilterLevel1] = useState("");
  const [filterLevel2, setFilterLevel2] = useState("");
  const [filterLevel3, setFilterLevel3] = useState("");
  const level1Needle = useMemo(() => normalizeFilter(filterLevel1), [filterLevel1]);
  const level2Needle = useMemo(() => normalizeFilter(filterLevel2), [filterLevel2]);
  const level3Needle = useMemo(() => normalizeFilter(filterLevel3), [filterLevel3]);

  useEffect(() => {
    setFilterLevel1("");
    setFilterLevel2("");
    setFilterLevel3("");
  }, [requestId]);

  const [hfOffsetDraft, setHfOffsetDraft] = useState(String(hfOffset));
  useEffect(() => {
    setHfOffsetDraft(String(hfOffset));
  }, [hfOffset, requestId]);

  const commitHfOffset = () => {
    const trimmed = hfOffsetDraft.trim();
    if (!trimmed) {
      setHfOffsetDraft(String(hfOffset));
      return;
    }
    const parsed = Number.parseInt(trimmed, 10);
    if (!Number.isFinite(parsed)) {
      setHfOffsetDraft(String(hfOffset));
      return;
    }
    setHfOffset(parsed);
  };

  const headerLoadIcon: ReactNode = busy ? (
    <Loader2 className="mr-2 h-4 w-4 animate-spin" />
  ) : effectiveKind === "hf" ? (
    <Database className="mr-2 h-4 w-4" />
  ) : effectiveKind === "zenodo" ? (
    <BadgeInfo className="mr-2 h-4 w-4" />
  ) : (
    <HardDrive className="mr-2 h-4 w-4" />
  );

  const hasDetection = sourceInput.trim() || chunkSelection.length > 0;
  const detectedBadgeLabel =
    effectiveKind === "hf"
      ? "Hugging Face"
      : effectiveKind === "zenodo"
        ? "Zenodo"
        : effectiveKind === "wds"
          ? "WebDataset"
          : effectiveKind === "mds"
            ? "MosaicML MDS"
            : "LitData";

  const detectedDescription =
    effectiveKind === "hf"
      ? displayHfDatasetId(sourceInput) ?? "Streaming dataset from Hugging Face Hub"
      : effectiveKind === "zenodo"
        ? "Remote archive from Zenodo repository"
        : effectiveKind === "wds"
          ? "Local WebDataset shards (.tar)"
          : effectiveKind === "mds"
            ? "Local MosaicML streaming dataset"
            : "Local LitData optimized dataset";

  const level1Pending = isHfMode ? hfQuery.isPending : isZenodoMode ? zenodoQuery.isPending : isWdsMode ? wdsDirQuery.isPending : indexQuery.isPending;
  const selectedHfConfig = hfConfigOverride ?? hfQuery.data?.config ?? hfSelectedCache?.config ?? null;
  const selectedHfSplit = hfSplitOverride ?? hfQuery.data?.split ?? hfSelectedCache?.split ?? null;

  const enablePagination = isHfMode || isWdsMode || (isZenodoMode && selectedZenodoFileIsTar);

  return (
    <main className="h-full overflow-hidden">
      <div className="flex h-full flex-col gap-2">
        <section className="shrink-0 overflow-hidden rounded-2xl bg-white/55 shadow-[var(--shadow-soft)] backdrop-blur ring-1 ring-black/5">
          <div className="flex gap-4 p-3">
            <div className="flex flex-1 min-w-0 flex-col gap-2">
              <div className="flex flex-col gap-1">
                <div className="flex w-full min-w-0 flex-wrap items-center gap-2">
                  <div className="min-w-[50%] flex-1">
                    <Input
                      className="w-full rounded-md"
                      placeholder={sourcePlaceholder}
                      value={sourceInput}
                      onChange={(e) => setSourceInput(e.target.value)}
                      onKeyDown={(e) => {
                        if (e.key === "Enter") void handleLoad();
                      }}
                      aria-label="Source"
                    />
                  </div>

                  <div className="flex w-full flex-wrap items-center justify-end gap-2 sm:w-auto sm:flex-nowrap sm:shrink-0">
                    {canBrowse ? (
                      <Button variant="outline" onClick={() => void handleChoose()} disabled={busy || !tauri}>
                        <FolderOpen className="mr-2 h-4 w-4" />
                        Browse
                      </Button>
                    ) : null}
                    <Tooltip>
                      <TooltipTrigger asChild>
                        <div>
                          <Button
                            onClick={() => void handleLoad()}
                            disabled={busy || (!sourceInput.trim() && chunkSelection.length === 0) || !tauri}
                          >
                            {headerLoadIcon}
                            Load
                          </Button>
                        </div>
                      </TooltipTrigger>
                      {!tauri && <TooltipContent>Loading requires the Tauri runtime.</TooltipContent>}
                    </Tooltip>
                  </div>
                </div>

                <div className="flex flex-wrap items-center gap-2 px-0.5 text-[11px] text-slate-400">
                  {hasDetection ? (
                    <>
                      <span>Detected:</span>
                      <Badge variant="secondary" className="bg-white/70 text-slate-600 text-[11px]">
                        {detectedBadgeLabel}
                      </Badge>
                      <span>·</span>
                      <span>{detectedDescription}</span>
                    </>
                  ) : (
                    <span>Supports: LitData, MosaicML MDS, WebDataset, Hugging Face, Zenodo</span>
                  )}

                  {chunkSelection.length > 0 ? (
                    <>
                      <Badge variant="secondary" className="bg-white/85 text-slate-700">
                        {chunkSelection.length} shard{chunkSelection.length > 1 ? "s" : ""} selected
                      </Badge>
                      <Button
                        type="button"
                        size="sm"
                        variant="ghost"
                        className="h-6 px-2 text-[11px] font-semibold text-slate-600"
                        onClick={() => setChunkSelection([])}
                        disabled={busy}
                      >
                        Clear
                      </Button>
                    </>
                  ) : null}

                  {showHfStats ? (
                    <Button
                      type="button"
                      size="sm"
                      variant="ghost"
                      className="h-6 px-2 text-[11px] font-medium text-slate-600"
                      disabled={!tauri}
                      onClick={() => setHfTokenDialogOpen(true)}
                    >
                      <KeyRound className="mr-1 h-3 w-3" />
                      {hfTokenMasked ? `Token ${hfTokenMasked}` : "Set Token"}
                    </Button>
                  ) : null}
                </div>
              </div>
            </div>

            <div className="hidden xl:flex shrink-0 w-[420px] items-center justify-center gap-3 rounded-xl bg-white/50 px-4 py-2 ring-1 ring-black/[0.05]">
              {headerStatsKind === "hf" ? (
                <>
                  <StatBlockLarge label="Dataset" value={headerStats.hf.datasetLabel} />
                  <StatBlockLarge label="Split" value={headerStats.hf.splitLabel} />
                  <StatBlockLarge label="Rows" value={headerStats.hf.rowsLabel} />
                </>
              ) : headerStatsKind === "zenodo" ? (
                <>
                  <StatBlockLarge label="Record" value={headerStats.zenodo.recordLabel} />
                  <StatBlockLarge label="Files" value={headerStats.zenodo.filesLabel} />
                  <StatBlockLarge label="Size" value={headerStats.zenodo.sizeLabel} />
                </>
              ) : headerStatsKind === "wds" ? (
                <>
                  <StatBlockLarge label="Shards" value={headerStats.wds.shardsLabel} />
                  <StatBlockLarge label="Size" value={headerStats.wds.sizeLabel} />
                </>
              ) : (
                <>
                  <StatBlockLarge label="Shards" value={headerStats.local.shardsLabel} />
                  <StatBlockLarge label="Items" value={headerStats.local.itemsLabel} />
                  <StatBlockLarge label="Size" value={headerStats.local.sizeLabel} />
                </>
              )}
            </div>
          </div>
        </section>

        <Dialog open={hfTokenDialogOpen} onOpenChange={setHfTokenDialogOpen}>
          <DialogContent aria-describedby={undefined}>
            <DialogHeader>
              <DialogTitle>Hugging Face Token</DialogTitle>
              <div className="text-xs text-slate-500">
                Saved locally on this device. Required for private or gated datasets.
              </div>
            </DialogHeader>
            <div className="space-y-3">
              <Input
                type="password"
                className="rounded-lg"
                placeholder={hfTokenMasked ? `Token saved (${hfTokenMasked})` : "Paste token here"}
                value={hfTokenDraft}
                onChange={(e) => setHfTokenDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === "Escape") setHfTokenDialogOpen(false);
                  if (e.key === "Enter") {
                    e.preventDefault();
                    void handleSaveHfToken();
                  }
                }}
                autoFocus
                aria-label="Hugging Face token"
              />

              <div className="flex items-center justify-end gap-2">
                {hfToken ? (
                  <Button size="sm" variant="outline" onClick={handleClearSavedHfToken}>
                    Clear
                  </Button>
                ) : null}
                <Button size="sm" variant="ghost" onClick={() => setHfTokenDialogOpen(false)}>
                  Close
                </Button>
                <Button size="sm" type="button" onClick={() => void handleSaveHfToken()}>
                  Save
                </Button>
              </div>
            </div>
          </DialogContent>
        </Dialog>

        {/* Explorer area */}
        <motion.div initial={{ opacity: 0, y: 12 }} animate={{ opacity: 1, y: 0 }} className="flex-1 min-h-0">
          <div className="h-full min-h-0 flex flex-col overflow-hidden rounded-2xl bg-white/80 shadow-sm backdrop-blur-xl ring-1 ring-black/[0.08]">
            <div className="hidden lg:grid flex-1 min-h-0 grid-cols-3">
              {/* Column 1 */}
              <div className="flex min-w-0 min-h-0 flex-col border-r border-black/[0.06]">
                <div className="flex items-center gap-2 border-b border-black/[0.06] bg-slate-50/50 px-3 py-2">
                  <HardDrive className="h-4 w-4 text-emerald-600" />
                  <span className="text-[12px] font-semibold text-slate-700">
                    {isHfMode ? "Splits" : isZenodoMode ? "Files" : "Shards"}
                  </span>
                  <span className="ml-auto text-[11px] font-medium text-slate-500 tabular-nums">
                    {isHfMode
                      ? hfSplitPairs.length || "—"
                      : isZenodoMode
                        ? zenodoFiles.length || "—"
                        : isWdsMode
                          ? (wdsDirQuery.data?.shards.length ?? 0) || "—"
                          : (indexQuery.data?.chunks.length ?? 0) || "—"}
                  </span>
                </div>
                <div className="flex min-w-0 min-h-0 flex-1 flex-col overflow-hidden px-2 py-1.5">
                  <div className="flex h-full min-h-0 flex-col gap-1.5">
                    <ListFilterInput
                      value={filterLevel1}
                      onValueChange={setFilterLevel1}
                      placeholder={isHfMode ? "Filter splits…" : isZenodoMode ? "Filter files…" : "Filter shards…"}
                      ariaLabel="Filter level 1"
                    />
                    <ScrollArea className="flex-1 min-h-0 rounded-xl bg-white/50 ring-1 ring-black/[0.04]">
                      {isHfMode ? (
                        hfSplitPairs
                          .filter((pair) => matchesFilter(`${pair.config}/${pair.split}`, level1Needle))
                          .map((pair) => {
                            const key = `${pair.config}:${pair.split}`;
                            return (
                              <SelectableRowButton
                                key={key}
                                isSelected={selectedHfConfig === pair.config && selectedHfSplit === pair.split}
                                className="grid-cols-[1fr] items-center gap-2"
                                ariaLabel={`Select split ${pair.config}/${pair.split}`}
                                onClick={() => setHfConfigSplit(pair.config, pair.split)}
                              >
                                <div className="truncate font-medium text-slate-900">{`${pair.config}/${pair.split}`}</div>
                              </SelectableRowButton>
                            );
                          })
                      ) : isZenodoMode ? (
                        zenodoFiles
                          .filter((file) => matchesFilter(file.key, level1Needle))
                          .map((file) => (
                            <SelectableRowButton
                              key={file.key}
                              isSelected={selectedZenodoFile?.key === file.key}
                              className="grid-cols-[1fr_auto] items-center gap-2"
                              ariaLabel={`Select file ${file.key}`}
                              onClick={() => selectZenodoFile(file.key)}
                            >
                              <div className="truncate font-medium text-slate-900">{file.key}</div>
                              <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                {formatBytes(file.size)}
                              </div>
                            </SelectableRowButton>
                          ))
                      ) : isWdsMode ? (
                        (wdsDirQuery.data?.shards ?? [])
                          .filter((shard) => matchesFilter(shard.filename, level1Needle))
                          .map((shard) => (
                            <SelectableRowButton
                              key={shard.filename}
                              isSelected={selectedShard?.filename === shard.filename}
                              className="grid-cols-[1fr_auto] items-center gap-2"
                              ariaLabel={`Select shard ${shard.filename}`}
                              onClick={() => selectChunk(shard.filename)}
                            >
                              <div className="truncate font-medium text-slate-900">{shard.filename}</div>
                              <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                {formatBytes(shard.bytes)}
                              </div>
                            </SelectableRowButton>
                          ))
                      ) : (
                        (indexQuery.data?.chunks ?? [])
                          .filter((chunk) => matchesFilter(chunk.filename, level1Needle))
                          .map((chunk) => (
                            <SelectableRowButton
                              key={chunk.filename}
                              isSelected={selectedChunk?.filename === chunk.filename}
                              className="grid-cols-[1fr_auto] items-center gap-2"
                              ariaLabel={`Select shard ${chunk.filename}`}
                              onClick={() => selectChunk(chunk.filename)}
                            >
                              <div className="truncate font-medium text-slate-900">{chunk.filename}</div>
                              <div className="flex items-center gap-2 whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                <span>{chunk.chunkSize.toLocaleString()} items</span>
                                <span className="text-slate-400">·</span>
                                <span>{formatBytes(chunk.chunkBytes)}</span>
                              </div>
                            </SelectableRowButton>
                          ))
                      )}
                      {level1Pending ? (
                        <div className="p-4">
                          <Skeleton className="h-10 w-full" />
                        </div>
                      ) : null}
                    </ScrollArea>
                  </div>
                </div>
              </div>

              {/* Column 2 */}
              <div className="flex min-w-0 min-h-0 flex-col border-r border-black/[0.06]">
                <div className="flex items-center gap-2 border-b border-black/[0.06] bg-slate-50/50 px-3 py-2">
                  <BadgeInfo className="h-4 w-4 text-sky-600" />
                  <span className="text-[12px] font-semibold text-slate-700">
                    {isHfMode ? "Rows" : isZenodoMode ? "Entries" : isMdsMode ? "Samples" : isWdsMode ? "Samples" : "Items"}
                  </span>
                  <span className="ml-auto text-[11px] font-medium text-slate-500 tabular-nums">
                    {isHfMode
                      ? hfRows.length || "—"
                      : isZenodoMode
                        ? zenodoEntries.length || "—"
                        : isWdsMode
                          ? wdsSamples.length || "—"
                          : isMdsMode
                            ? mdsItems.length || "—"
                            : items.length || "—"}
                  </span>
                </div>
                <div className="flex min-w-0 min-h-0 flex-1 flex-col overflow-hidden px-2 py-1.5">
                  <div className="flex h-full min-h-0 flex-col gap-1.5">
                    <ListFilterInput value={filterLevel2} onValueChange={setFilterLevel2} placeholder="Filter…" ariaLabel="Filter level 2" />
                    <ScrollArea className="flex-1 min-h-0 rounded-xl bg-white/50 ring-1 ring-black/[0.04]">
                      {isHfMode ? (
                        hfQuery.isFetching ? (
                          <div className="p-4">
                            <Skeleton className="h-10 w-full" />
                          </div>
                        ) : hfRows.length > 0 ? (
                          hfRows
                            .map((row, idx) => ({ row: row as Record<string, unknown>, idx }))
                            .filter(({ row }) => {
                              if (!level2Needle) return true;
                              const preview = Object.entries(row)
                                .slice(0, 3)
                                .map(([k, v]) => `${k}: ${formatCell(v)}`)
                                .join(" ");
                              return matchesFilter(preview, level2Needle);
                            })
                            .map(({ row, idx }) => (
                              <SelectableRowButton
                                key={idx}
                                isSelected={hfSelectedRowIndex === idx}
                                className="grid-cols-[1fr_auto] items-center gap-2"
                                ariaLabel={`Select row ${hfOffset + idx}`}
                                onClick={() => selectHfRow(idx)}
                              >
                                <div className="truncate font-medium text-slate-900">Row {hfOffset + idx}</div>
                                <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                  {Object.keys(row).length} fields
                                </div>
                              </SelectableRowButton>
                            ))
                        ) : (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <TriangleAlert className="h-4 w-4 text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">No rows available.</div>
                          </div>
                        )
                      ) : isZenodoMode ? (
                        zenodoEntries.length > 0 ? (
                          zenodoEntries
                            .filter((entry) => matchesFilter(entry.name, level2Needle))
                            .map((entry) => {
                              const entrySize =
                                "size" in entry ? entry.size : (entry as ZenodoZipEntrySummary).uncompressedSize;
                              return (
                                <SelectableRowButton
                                  key={entry.name}
                                  isSelected={selectedZenodoEntry?.name === entry.name}
                                  className="grid-cols-[1fr_auto] items-center gap-2"
                                  ariaLabel={`Select entry ${entry.name}`}
                                  onClick={() => selectZenodoEntry(entry.name)}
                                >
                                  <div className="truncate font-medium text-slate-900">{entry.name}</div>
                                  <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                    {formatBytes(entrySize)}
                                  </div>
                                </SelectableRowButton>
                              );
                            })
                        ) : zenodoTarEntriesQuery.isPending || zenodoZipEntriesQuery.isPending ? (
                          <div className="p-4">
                            <Skeleton className="h-10 w-full" />
                          </div>
                        ) : selectedZenodoFile ? (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <BadgeInfo className="h-4 w-4 text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">
                              {looksLikeTarFilename(selectedZenodoFile.key) || selectedZenodoFile.key.endsWith(".zip")
                                ? "Loading entries…"
                                : "Select a .tar or .zip file to browse entries."}
                            </div>
                          </div>
                        ) : (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <TriangleAlert className="h-4 w-4 text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">Select a file from the first column.</div>
                          </div>
                        )
                      ) : isWdsMode ? (
                        wdsSamples.length > 0 ? (
                          wdsSamples
                            .filter((sample) => matchesFilter(sample.key, level2Needle))
                            .map((sample) => (
                              <SelectableRowButton
                                key={sample.key}
                                isSelected={wdsSelectedSampleKey === sample.key}
                                className="grid-cols-[1fr_auto] items-center gap-2"
                                ariaLabel={`Select sample ${sample.key}`}
                                onClick={() => selectWdsSample(sample.key)}
                              >
                                <div className="truncate font-medium text-slate-900">{sample.key}</div>
                                <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                  {sample.fields.length} field{sample.fields.length !== 1 ? "s" : ""}
                                </div>
                              </SelectableRowButton>
                            ))
                        ) : wdsSamplesQuery.isPending ? (
                          <div className="p-4">
                            <Skeleton className="h-10 w-full" />
                          </div>
                        ) : (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <TriangleAlert className="h-4 w-4 text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">Select a shard from the first column.</div>
                          </div>
                        )
                      ) : isMdsMode ? (
                        mdsItems.length > 0 ? (
                          mdsItems
                            .filter((item) => matchesFilter(`Sample ${item.itemIndex}`, level2Needle))
                            .map((item) => (
                              <SelectableRowButton
                                key={item.itemIndex}
                                isSelected={selectedItemIndex === item.itemIndex}
                                className="grid-cols-[1fr_auto] items-center gap-2"
                                ariaLabel={`Select sample ${item.itemIndex}`}
                                onClick={() => selectItem(item.itemIndex)}
                              >
                                <div className="truncate font-medium text-slate-900">Sample {item.itemIndex}</div>
                                <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                  {item.fields.length} field{item.fields.length !== 1 ? "s" : ""} · {formatBytes(item.totalBytes)}
                                </div>
                              </SelectableRowButton>
                            ))
                        ) : mdsItemsQuery.isPending ? (
                          <div className="p-4">
                            <Skeleton className="h-10 w-full" />
                          </div>
                        ) : selectedChunk ? (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">Loading samples…</div>
                          </div>
                        ) : (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <TriangleAlert className="h-4 w-4 text-slate-400" />
                            <div className="max-w-[520px] leading-relaxed">Select a shard from the first column.</div>
                          </div>
                        )
                      ) : items.length > 0 ? (
                        items
                          .filter((item) => matchesFilter(`Item ${item.itemIndex}`, level2Needle))
                          .map((item) => (
                            <SelectableRowButton
                              key={item.itemIndex}
                              isSelected={selectedItemIndex === item.itemIndex}
                              className="grid-cols-[1fr_auto] items-center gap-2"
                              ariaLabel={`Select item ${item.itemIndex}`}
                              onClick={() => selectItem(item.itemIndex)}
                            >
                              <div className="truncate font-medium text-slate-900">Item {item.itemIndex}</div>
                              <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                {item.fields.length} field{item.fields.length !== 1 ? "s" : ""}
                              </div>
                            </SelectableRowButton>
                          ))
                      ) : itemsQuery.isPending ? (
                        <div className="p-4">
                          <Skeleton className="h-10 w-full" />
                        </div>
                      ) : selectedChunk ? (
                        <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                          <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
                          <div className="max-w-[520px] leading-relaxed">Loading items…</div>
                        </div>
                      ) : (
                        <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                          <TriangleAlert className="h-4 w-4 text-slate-400" />
                          <div className="max-w-[520px] leading-relaxed">Select a shard from the first column.</div>
                        </div>
                      )}
                    </ScrollArea>
                  </div>
                </div>

                {enablePagination ? (
                  <div className="shrink-0 border-t border-black/[0.06] bg-slate-50/50 px-2 py-2">
                    <div className="flex w-full items-center justify-center gap-2">
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={
                          busy ||
                          (isHfMode && hfOffset <= 0) ||
                          (isWdsMode && wdsOffset <= 0) ||
                          (isZenodoMode && zenodoEntriesOffset <= 0)
                        }
                        onClick={() => {
                          if (isHfMode) setHfOffset(hfOffset - HF_PAGE_SIZE);
                          else if (isWdsMode) setWdsOffset(wdsOffset - WDS_PAGE_SIZE);
                          else if (isZenodoMode) setZenodoEntriesOffset(zenodoEntriesOffset - ZENODO_TAR_PAGE_SIZE);
                        }}
                      >
                        <ChevronLeft className="h-4 w-4" />
                        Prev
                      </Button>
                      <div className="flex flex-1 flex-col items-center justify-center text-center min-w-0">
                        {isHfMode ? (
                          <div className="flex items-center gap-1.5">
                            <Input
                              className="h-7 w-20 rounded-md bg-white text-xs tabular-nums text-center"
                              value={hfOffsetDraft}
                              onChange={(e) => setHfOffsetDraft(e.target.value)}
                              onBlur={commitHfOffset}
                              onKeyDown={(e) => {
                                if (e.key === "Enter") {
                                  e.preventDefault();
                                  commitHfOffset();
                                }
                              }}
                              aria-label="Row offset"
                            />
                            <span className="text-[11px] text-slate-500 tabular-nums">
                              {hfQuery.data?.numRowsTotal !== null && hfQuery.data?.numRowsTotal !== undefined
                                ? `/ ${hfQuery.data.numRowsTotal.toLocaleString()}`
                                : ""}
                            </span>
                          </div>
                        ) : isWdsMode ? (
                          <span className="text-[11px] font-medium text-slate-600 tabular-nums">
                            Offset {wdsOffset.toLocaleString()}
                            {wdsSamplesQuery.data?.numSamplesTotal !== null && wdsSamplesQuery.data?.numSamplesTotal !== undefined
                              ? ` / ${wdsSamplesQuery.data.numSamplesTotal.toLocaleString()}`
                              : ""}
                          </span>
                        ) : (
                          <span className="text-[11px] font-medium text-slate-600 tabular-nums">
                            Offset {zenodoEntriesOffset.toLocaleString()}
                            {zenodoTarEntriesQuery.data?.numEntriesTotal !== null && zenodoTarEntriesQuery.data?.numEntriesTotal !== undefined
                              ? ` / ${zenodoTarEntriesQuery.data.numEntriesTotal.toLocaleString()}`
                              : ""}
                          </span>
                        )}
                      </div>
                      <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={
                          busy ||
                          (isHfMode &&
                            hfQuery.data?.numRowsTotal !== null &&
                            hfQuery.data?.numRowsTotal !== undefined &&
                            hfOffset + HF_PAGE_SIZE >= hfQuery.data.numRowsTotal) ||
                          (isWdsMode &&
                            wdsSamplesQuery.data?.numSamplesTotal !== null &&
                            wdsSamplesQuery.data?.numSamplesTotal !== undefined &&
                            wdsOffset + WDS_PAGE_SIZE >= wdsSamplesQuery.data.numSamplesTotal) ||
                          (isZenodoMode &&
                            zenodoTarEntriesQuery.data?.numEntriesTotal !== null &&
                            zenodoTarEntriesQuery.data?.numEntriesTotal !== undefined &&
                            zenodoEntriesOffset + ZENODO_TAR_PAGE_SIZE >= zenodoTarEntriesQuery.data.numEntriesTotal)
                        }
                        onClick={() => {
                          if (isHfMode) setHfOffset(hfOffset + HF_PAGE_SIZE);
                          else if (isWdsMode) setWdsOffset(wdsOffset + WDS_PAGE_SIZE);
                          else if (isZenodoMode) setZenodoEntriesOffset(zenodoEntriesOffset + ZENODO_TAR_PAGE_SIZE);
                        }}
                      >
                        Next
                        <ChevronRight className="h-4 w-4" />
                      </Button>
                    </div>
                  </div>
                ) : null}
              </div>

              {/* Column 3 */}
              <div className="flex min-w-0 min-h-0 flex-col">
                <div className="flex items-center gap-2 border-b border-black/[0.06] bg-slate-50/50 px-3 py-2">
                  <Play className="h-4 w-4 text-cyan-600" />
                  <span className="text-[12px] font-semibold text-slate-700">Fields</span>
                  <span className="ml-auto text-[11px] font-medium text-slate-500 tabular-nums">
                    {isHfMode
                      ? hfSelectedRow
                        ? Object.keys(hfSelectedRow).length
                        : "—"
                      : isWdsMode
                        ? wdsSelectedSample?.fields.length ?? "—"
                        : fieldNames.length || "—"}
                  </span>
                </div>
                <div className="flex flex-1 min-h-0 flex-col">
                  <div className="flex flex-col min-h-0 flex-1 overflow-hidden">
                    <div className="flex min-w-0 min-h-0 flex-1 flex-col overflow-hidden px-2 py-1.5">
                      <div className="flex h-full min-h-0 flex-col gap-1.5">
                        <ListFilterInput
                          value={filterLevel3}
                          onValueChange={setFilterLevel3}
                          placeholder="Filter fields…"
                          ariaLabel="Filter level 3"
                        />
                        <ScrollArea className="flex-1 min-h-0 rounded-xl bg-white/50 ring-1 ring-black/[0.04]">
                          {isHfMode && hfSelectedRow ? (
                            Object.keys(hfSelectedRow)
                              .filter((key) => matchesFilter(key, level3Needle))
                              .sort((a, b) => a.localeCompare(b))
                              .map((key) => {
                                const fieldValue = hfSelectedRow[key];
                                const typeHint =
                                  fieldValue === null
                                    ? "null"
                                    : Array.isArray(fieldValue)
                                      ? `array(${fieldValue.length})`
                                      : typeof fieldValue === "number"
                                        ? "number"
                                        : typeof fieldValue === "boolean"
                                          ? "bool"
                                          : typeof fieldValue === "string"
                                            ? "text"
                                            : "json";
                                return (
                                  <SelectableRowButton
                                    key={key}
                                    isSelected={hfSelectedFieldName === key}
                                    className="grid-cols-[1fr_auto] items-center gap-2"
                                    ariaLabel={`Select field ${key}`}
                                    onClick={() => selectHfField(key)}
                                  >
                                    <div className="truncate font-medium text-slate-900">{key}</div>
                                    <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                      {typeHint}
                                    </div>
                                  </SelectableRowButton>
                                );
                              })
                          ) : isWdsMode && wdsSelectedSample ? (
                            wdsSelectedSample.fields
                              .filter((field) => matchesFilter(field.name, level3Needle))
                              .map((field) => (
                                <SelectableRowButton
                                  key={field.memberPath}
                                  isSelected={wdsSelectedMemberPath === field.memberPath}
                                  className="grid-cols-[1fr_auto] items-center gap-2"
                                  ariaLabel={`Select field ${field.name}`}
                                  onClick={() => selectWdsMember(field.memberPath, field.name)}
                                >
                                  <div className="truncate font-medium text-slate-900">{field.name}</div>
                                  <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                    {formatBytes(field.size)}
                                  </div>
                                </SelectableRowButton>
                              ))
                          ) : fieldNames.length > 0 ? (
                            fieldNames
                              .filter((field) => matchesFilter(`Field ${field.fieldIndex}`, level3Needle))
                              .map((field) => (
                                <SelectableRowButton
                                  key={field.fieldIndex}
                                  isSelected={selectedFieldIndex === field.fieldIndex}
                                  className="grid-cols-[1fr_auto] items-center gap-2"
                                  ariaLabel={`Select field ${field.fieldIndex}`}
                                  onClick={() => selectField(field.fieldIndex)}
                                >
                                  <div className="truncate font-medium text-slate-900">Field {field.fieldIndex}</div>
                                  <div className="whitespace-nowrap text-[11px] text-slate-500 tabular-nums">
                                    {formatBytes(field.size)}
                                  </div>
                                </SelectableRowButton>
                              ))
                          ) : selectedItemIndex !== null ? (
                            <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                              <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
                              <div className="max-w-[520px] leading-relaxed">Loading fields…</div>
                            </div>
                          ) : (
                            <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                              <TriangleAlert className="h-4 w-4 text-slate-400" />
                              <div className="max-w-[520px] leading-relaxed">Select an item to see its fields.</div>
                            </div>
                          )}
                        </ScrollArea>
                      </div>
                    </div>
                  </div>

                  <div className="shrink min-h-0 max-h-[50%] flex flex-col border-t border-black/[0.06]">
                    <div className="flex items-center gap-2 border-b border-black/[0.06] bg-slate-50/50 px-3 py-1.5">
                      <Sparkles className="h-3.5 w-3.5 text-slate-400" />
                      <span className="text-[11px] font-semibold text-slate-600">Preview</span>
                      {previewMetaSource && (
                        <div className="ml-auto flex items-center gap-1.5">
                          {buildPreviewMeta(previewMetaSource).map((tag, i) => (
                            <Badge key={i} variant="secondary" className="text-[10px]">
                              {tag}
                            </Badge>
                          ))}
                        </div>
                      )}
                    </div>
                    <div className="flex flex-1 min-h-0 flex-col overflow-auto px-2 py-1.5">
                      {localPreviewLoading ? (
                        <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                          <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
                          <div>Loading preview…</div>
                        </div>
                      ) : localPreviewData ? (
                        <div className="space-y-2">
                          {audioPreviewPath ? (
                            <audio controls className="w-full" src={toFileSrc(audioPreviewPath)} />
                          ) : null}
                          <ScrollArea className="max-h-32 rounded-none bg-slate-50/70 px-3 py-2 text-xs select-text cursor-text">
                            <pre
                              className={cn(
                                "font-mono text-slate-700",
                                localPreviewData.isBinary ? "whitespace-pre" : "whitespace-pre-wrap break-words",
                              )}
                            >
                              {buildPreviewBodyText(localPreviewData)}
                            </pre>
                          </ScrollArea>
                          <div className="flex items-center gap-2">
                            <CopyButton text={buildPreviewBodyText(localPreviewData)} />
                            <Button
                              size="sm"
                              variant="ghost"
                              onClick={() => {
                                void (async () => {
                                  try {
                                    if (isLitdataMode) {
                                      if (
                                        !selectedChunk ||
                                        selectedItemIndex === null ||
                                        selectedFieldIndex === null ||
                                        !indexQuery.data?.indexPath
                                      )
                                        return;
                                      const response = await openLeaf({
                                        indexPath: indexQuery.data.indexPath,
                                        chunkFilename: selectedChunk.filename,
                                        itemIndex: selectedItemIndex,
                                        fieldIndex: selectedFieldIndex,
                                      });
                                      await handleOpenResult(response);
                                      return;
                                    }

                                    if (isMdsMode) {
                                      if (
                                        !selectedChunk ||
                                        selectedItemIndex === null ||
                                        selectedFieldIndex === null ||
                                        !indexQuery.data?.indexPath
                                      )
                                        return;
                                      const response = await mosaicmlOpenLeaf({
                                        indexPath: indexQuery.data.indexPath,
                                        shardFilename: selectedChunk.filename,
                                        itemIndex: selectedItemIndex,
                                        fieldIndex: selectedFieldIndex,
                                      });
                                      await handleOpenResult(response);
                                      return;
                                    }

                                    if (isWdsMode) {
                                      if (!selectedShard || !wdsSelectedMemberPath || !wdsDirQuery.data?.dirPath) return;
                                      const response = await wdsOpenMember({
                                        dirPath: wdsDirQuery.data.dirPath,
                                        shardFilename: selectedShard.filename,
                                        memberPath: wdsSelectedMemberPath,
                                      });
                                      await handleOpenResult(response);
                                    }
                                  } catch (err) {
                                    setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected field.");
                                  }
                                })();
                              }}
                            >
                              <ArrowUpRightFromSquare className="mr-1 h-3.5 w-3.5" />
                              Open
                            </Button>
                          </div>
                        </div>
                      ) : isZenodoMode ? (
                        zenodoPreviewLoading ? (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <Loader2 className="h-4 w-4 animate-spin text-slate-400" />
                            <div>Loading preview…</div>
                          </div>
                        ) : zenodoPreviewData ? (
                          <div className="space-y-2">
                            {!zenodoInlineMediaData &&
                            selectedZenodoFile &&
                            !selectedZenodoFileIsTar &&
                            !selectedZenodoFileIsZip &&
                            zenodoPreviewData.guessedExt &&
                            videoMimeFromExt(zenodoPreviewData.guessedExt) ? (
                              <div className="rounded-lg bg-slate-900 p-2 overflow-hidden">
                                <video controls preload="metadata" className="w-full max-h-[40vh]" src={selectedZenodoFile.contentUrl}>
                                  Your browser does not support the video tag.
                                </video>
                              </div>
                            ) : null}
                            {zenodoInlineMediaData ? (
                              zenodoInlineMediaData.mime.startsWith("image/") ? (
                                <div className="rounded-lg bg-slate-50/70 px-3 py-2 overflow-hidden">
                                  <img
                                    alt={selectedZenodoEntry?.name ?? selectedZenodoFile?.key ?? "Zenodo preview"}
                                    src={`data:${zenodoInlineMediaData.mime};base64,${zenodoInlineMediaData.base64}`}
                                    className="w-full max-h-[40vh] object-contain"
                                  />
                                </div>
                              ) : zenodoInlineMediaData.mime.startsWith("video/") ? (
                                <div className="rounded-lg bg-slate-900 p-2 overflow-hidden">
                                  <video
                                    controls
                                    className="w-full max-h-[40vh]"
                                    src={`data:${zenodoInlineMediaData.mime};base64,${zenodoInlineMediaData.base64}`}
                                  >
                                    Your browser does not support the video tag.
                                  </video>
                                </div>
                              ) : zenodoInlineMediaData.mime.startsWith("audio/") ? (
                                <audio controls className="w-full" src={`data:${zenodoInlineMediaData.mime};base64,${zenodoInlineMediaData.base64}`} />
                              ) : null
                            ) : null}
                            <ScrollArea className="max-h-32 rounded-none bg-slate-50/70 px-3 py-2 text-xs select-text cursor-text">
                              <pre
                                className={cn(
                                  "font-mono text-slate-700",
                                  zenodoPreviewData.isBinary ? "whitespace-pre" : "whitespace-pre-wrap break-words",
                                )}
                              >
                                {buildPreviewBodyText(zenodoPreviewData)}
                              </pre>
                            </ScrollArea>
                            <div className="flex items-center gap-2">
                              <CopyButton text={buildPreviewBodyText(zenodoPreviewData)} />
                              <Button
                                size="sm"
                                variant="ghost"
                                onClick={() => {
                                  void (async () => {
                                    try {
                                      if (!selectedZenodoFile) return;
                                      const response = selectedZenodoEntry
                                        ? selectedZenodoFileIsTar
                                          ? await zenodoTarOpenEntry({
                                              contentUrl: selectedZenodoFile.contentUrl,
                                              filename: selectedZenodoFile.key,
                                              entryName: selectedZenodoEntry.name,
                                            })
                                          : selectedZenodoFileIsZip
                                            ? await zenodoZipOpenEntry({
                                                contentUrl: selectedZenodoFile.contentUrl,
                                                filename: selectedZenodoFile.key,
                                                entryName: selectedZenodoEntry.name,
                                              })
                                            : await zenodoOpenFile({ contentUrl: selectedZenodoFile.contentUrl })
                                        : await zenodoOpenFile({ contentUrl: selectedZenodoFile.contentUrl });
                                      await handleOpenResult(response);
                                    } catch (err) {
                                      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected file.");
                                    }
                                  })();
                                }}
                              >
                                <ArrowUpRightFromSquare className="mr-1 h-3.5 w-3.5" />
                                Open
                              </Button>
                            </div>
                          </div>
                        ) : selectedZenodoFile ? (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <Sparkles className="h-5 w-5 text-slate-400" />
                            <div>
                              {selectedZenodoEntry ? "Pick an entry to preview its bytes." : "Pick a file to preview its bytes."}
                            </div>
                          </div>
                        ) : (
                          <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                            <Sparkles className="h-5 w-5 text-slate-400" />
                            <div>Pick a file to preview its bytes.</div>
                          </div>
                        )
                      ) : isHfMode && hfSelectedFieldName && hfSelectedRow ? (
                        (() => {
                          const fieldValue = hfSelectedRow[hfSelectedFieldName];
                          const media = extractHfMedia(fieldValue, hfSelectedFieldName);
                          const isAudio = isHfAudioMedia(media);
                          const isImage = isHfImageMedia(media);
                          const isVideo = isHfVideoMedia(media);

                          const isBase64Media = media && media.src.startsWith("data:");
                          const showRawValue = !isBase64Media;

                          return (
                            <div className="space-y-2">
                              {isVideo && media ? (
                                <div className="rounded-lg bg-slate-900 p-2 overflow-hidden">
                                  <video controls className="w-full max-h-[40vh]" src={media.src}>
                                    Your browser does not support the video tag.
                                  </video>
                                </div>
                              ) : isAudio && media ? (
                                <audio controls className="w-full" src={media.src} />
                              ) : isImage && media ? (
                                <div className="rounded-lg bg-slate-50/70 p-2 overflow-hidden">
                                  <img src={media.src} alt={hfSelectedFieldName} className="w-full max-h-[40vh] object-contain" />
                                </div>
                              ) : null}
                              {showRawValue && (
                                <ScrollArea className="max-h-32 rounded-none bg-slate-50/70 px-3 py-2 text-xs select-text cursor-text">
                                  <pre className="whitespace-pre-wrap break-words font-mono text-slate-700">
                                    {safeJson(fieldValue)}
                                  </pre>
                                </ScrollArea>
                              )}
                              {isBase64Media && (
                                <div className="text-xs text-slate-500 px-1">
                                  Base64-encoded {isVideo ? "video" : isAudio ? "audio" : "media"} (
                                  {typeof fieldValue === "string" ? Math.round(fieldValue.length / 1024) : 0} KB)
                                </div>
                              )}
                              <div className="flex items-center gap-2">
                                <CopyButton text={safeJson(fieldValue)} />
                                <Button
                                  size="sm"
                                  variant="ghost"
                                  onClick={() => {
                                    void (async () => {
                                      try {
                                        const effectiveConfig = hfConfigOverride ?? hfQuery.data?.config;
                                        const effectiveSplit = hfSplitOverride ?? hfQuery.data?.split;
                                        if (!hfDatasetInput || !hfSelectedFieldName || !effectiveConfig || !effectiveSplit) return;
                                        const response = await hfOpenField({
                                          input: hfDatasetInput,
                                          config: effectiveConfig,
                                          split: effectiveSplit,
                                          rowIndex: hfOffset + (hfSelectedRowIndex ?? 0),
                                          fieldName: hfSelectedFieldName,
                                          token: hfToken,
                                        });
                                        await handleOpenResult(response);
                                      } catch (err) {
                                        setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected field.");
                                      }
                                    })();
                                  }}
                                >
                                  <ArrowUpRightFromSquare className="mr-1 h-3.5 w-3.5" />
                                  Open
                                </Button>
                              </div>
                            </div>
                          );
                        })()
                      ) : (
                        <div className="flex h-full min-h-0 flex-col items-center justify-center gap-2 px-3 py-6 text-center text-xs text-slate-500">
                          <Sparkles className="h-5 w-5 text-slate-400" />
                          <div>Pick a field to preview its bytes.</div>
                        </div>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            </div>

            {/* Log dock */}
            <div className="shrink-0 border-t border-black/[0.04] bg-white/40">
              <Button
                type="button"
                variant="ghost"
                className={cn(
                  "flex h-auto w-full items-center gap-3 rounded-none px-4 py-2.5 text-left justify-start",
                  logDockOpen ? "bg-white/50 hover:bg-white/50" : "hover:bg-white/60",
                )}
                onClick={() => setLogDockOpen((prev) => !prev)}
              >
                <div className="flex items-center gap-2">
                  <Terminal className="h-4 w-4 text-slate-500" />
                  <span className="text-[12px] font-medium text-slate-700">Log</span>
                  <Badge variant={errorMessage ? "danger" : busy ? "warning" : "success"} className="text-[10px] font-semibold">
                    {busy ? "Working" : errorMessage ? "Error" : "Ready"}
                  </Badge>
                </div>
                <div className="ml-auto flex min-w-0 items-center gap-2 text-[11px] text-slate-500">
                  <span className="truncate">{logMessage}</span>
                  <ChevronRight className={cn("h-4 w-4 shrink-0 text-slate-400 transition", logDockOpen ? "rotate-90" : "")} />
                </div>
              </Button>

              {logDockOpen ? (
                <div className="space-y-3 border-t border-black/[0.04] bg-white/50 px-4 py-3">
                  <ScrollArea
                    className={cn(
                      "max-h-48 rounded-lg px-3 py-2 text-xs select-text cursor-text",
                      errorMessage ? "bg-rose-50/70 text-rose-700" : "bg-slate-50/70 text-slate-700",
                    )}
                  >
                    <pre className="whitespace-pre-wrap break-words font-mono">{logMessage}</pre>
                  </ScrollArea>
                  <div className="flex flex-wrap items-center justify-between gap-2">
                    <div className="flex items-center gap-2">
                      <CopyButton text={logMessage.trim()} />
                      <Button size="sm" variant="ghost" onClick={() => setStatusMessage(null)}>
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
        </motion.div>
      </div>
    </main>
  );
}
