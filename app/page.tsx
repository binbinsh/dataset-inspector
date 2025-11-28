"use client";

import { useEffect, useMemo, type ReactNode } from "react";
import { useMutation, useQuery } from "@tanstack/react-query";
import {
  ArrowRight,
  BadgeInfo,
  FolderOpen,
  HardDrive,
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
  isTauri,
  listChunkItems,
  loadChunkList,
  loadIndex,
  openLeaf,
  peekField,
  readLastIndex,
  saveLastIndex,
  type FieldPreview,
  type IndexSummary,
  type ItemMeta,
} from "@/lib/tauri-api";
import { cn } from "@/lib/utils";
import { useViewerStore } from "@/store/viewer";

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

export default function Page() {
  const {
    indexPath,
    setIndexPath,
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
    statusMessage,
    setStatusMessage,
  } = useViewerStore();

  useEffect(() => {
    if (!isTauri()) return;
    void readLastIndex().then((last) => {
      if (last) setIndexPath(last);
    });
  }, [setIndexPath]);

  const indexQuery = useQuery<IndexSummary>({
    queryKey: ["index-summary", mode],
    enabled: Boolean(mode),
    queryFn: () => {
      if (!mode) throw new Error("No source selected.");
      return mode.kind === "index" ? loadIndex(mode.indexPath) : loadChunkList(mode.paths);
    },
  });

  useEffect(() => {
    if (indexQuery.data?.indexPath) {
      void saveLastIndex(indexQuery.data.indexPath);
    }
    if (indexQuery.data) {
      setStatusMessage(`Loaded ${indexQuery.data.chunks.length} chunk${indexQuery.data.chunks.length === 1 ? "" : "s"}.`);
    }
  }, [indexQuery.data, setStatusMessage]);

  useEffect(() => {
    if (indexQuery.data && mode?.kind === "index" && chunkSelection.length) {
      setChunkSelection([]);
    }
  }, [chunkSelection.length, indexQuery.data, mode?.kind, setChunkSelection]);

  useEffect(() => {
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
  }, [indexQuery.data, selectChunk, selectedChunkName]);

  const selectedChunk = useMemo(
    () => indexQuery.data?.chunks.find((chunk) => chunk.filename === selectedChunkName) ?? null,
    [indexQuery.data, selectedChunkName],
  );

  const itemsQuery = useQuery<ItemMeta[]>({
    queryKey: ["chunk-items", indexQuery.data?.indexPath, selectedChunk?.filename],
    enabled: Boolean(indexQuery.data && selectedChunk && !indexQuery.isFetching),
    queryFn: () =>
      listChunkItems({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
      }),
    staleTime: 5 * 60 * 1000,
  });

  useEffect(() => {
    const items = itemsQuery.data ?? [];
    if (!items.length) {
      selectItem(null);
      return;
    }
    const exists = items.some((item) => item.itemIndex === selectedItemIndex);
    if (!exists) {
      selectItem(items[0].itemIndex);
    }
  }, [itemsQuery.data, selectItem, selectedItemIndex]);

  const selectedItem = useMemo(
    () => itemsQuery.data?.find((item) => item.itemIndex === selectedItemIndex) ?? null,
    [itemsQuery.data, selectedItemIndex],
  );

  useEffect(() => {
    if (!selectedItem) {
      selectField(null);
      return;
    }
    const exists = selectedItem.fields.some((field) => field.fieldIndex === selectedFieldIndex);
    if (!exists) {
      selectField(selectedItem.fields[0]?.fieldIndex ?? null);
    }
  }, [selectField, selectedFieldIndex, selectedItem]);

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
    enabled: Boolean(indexQuery.data && selectedChunk && selectedItem && selectedField && !itemsQuery.isFetching),
    queryFn: () =>
      peekField({
        indexPath: indexQuery.data?.indexPath ?? "",
        chunkFilename: selectedChunk?.filename ?? "",
        itemIndex: selectedItem?.itemIndex ?? 0,
        fieldIndex: selectedField?.fieldIndex ?? 0,
      }),
    staleTime: 60 * 1000,
  });

  const openFieldMutation = useMutation({
    mutationFn: () => {
      if (!indexQuery.data || !selectedChunk || !selectedItem || !selectedField) {
        throw new Error("Select a field to open.");
      }
      return openLeaf({
        indexPath: indexQuery.data.indexPath,
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItem.itemIndex,
        fieldIndex: selectedField.fieldIndex,
      });
    },
    onSuccess: (message) => setStatusMessage(message),
    onError: (err: unknown) =>
      setStatusMessage(err instanceof Error ? err.message : "Unable to open the selected field."),
  });

  const totalBytes =
    indexQuery.data?.chunks?.reduce(
      (acc, chunk) => acc + (Number.isFinite(chunk.chunkBytes) ? chunk.chunkBytes : 0),
      0,
    ) ?? 0;

  const busy =
    indexQuery.isFetching || itemsQuery.isFetching || previewQuery.isFetching || openFieldMutation.isPending;
  const latestError =
    indexQuery.error || itemsQuery.error || previewQuery.error || openFieldMutation.error || undefined;
  const errorMessage = latestError instanceof Error ? latestError.message : null;
  const formatList = (indexQuery.data?.dataFormat ?? []).join(" · ");

  const handleLoad = () => {
    setStatusMessage(null);
    if (chunkSelection.length > 0) {
      triggerLoad("chunks");
    } else {
      triggerLoad("index");
    }
  };

  const handleChoose = async () => {
    try {
      const pick = await chooseIndexSource(indexPath, indexQuery.data?.rootDir);
      if (!pick) return;
      setStatusMessage(null);
      if (pick.kind === "index") {
        setIndexPath(pick.indexPath);
        triggerLoad("index");
      } else {
        setChunkSelection(pick.paths);
        triggerLoad("chunks", pick.paths);
      }
    } catch (err) {
      setStatusMessage(err instanceof Error ? err.message : String(err));
    }
  };

  return (
    <main className="h-screen w-screen overflow-hidden bg-transparent">
      <div className="mx-auto flex h-full max-w-screen-2xl flex-col gap-4 px-3 pb-3 pt-4">
        <section className="relative overflow-hidden rounded-[24px] border border-white/60 bg-white/70 p-5 shadow-lg backdrop-blur">
          <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(90deg,#d7fce9_0%,#e0f6ff_60%,#f3f7ff_100%)]" />
          <div className="relative grid gap-4 lg:grid-cols-[1.2fr_0.8fr]">
            <div className="space-y-3">
              <h1 className="text-3xl font-semibold uppercase text-slate-900">LITDATA VIEWER</h1>
              <p className="text-sm text-slate-600">
                Load <code className="rounded bg-slate-100 px-1 py-0.5 text-xs">index.json</code> manifests or raw chunk
                files, inspect item leaves, and open them with your native apps.
              </p>
              <div className="flex flex-wrap items-center gap-2">
                <Input
                  className="w-full min-w-[280px] rounded-full border-slate-200 bg-white/70 backdrop-blur sm:w-96"
                  placeholder="/abs/path/to/index.json"
                  value={indexPath}
                  onChange={(e) => setIndexPath(e.target.value)}
                  aria-label="Index path"
                />
                <Button
                  variant="outline"
                  className="border-emerald-200 bg-white/80 text-emerald-700 hover:bg-emerald-50"
                  onClick={handleChoose}
                  disabled={busy || !isTauri()}
                >
                  <FolderOpen className="mr-2 h-4 w-4" />
                  Choose
                </Button>
                <Button onClick={handleLoad} disabled={busy || (!indexPath.trim() && chunkSelection.length === 0) || !isTauri()}>
                  {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <HardDrive className="mr-2 h-4 w-4" />}
                  Load
                </Button>
              </div>
              {chunkSelection.length > 0 ? (
                <div className="flex flex-wrap items-center gap-2 text-xs text-slate-600">
                  <Badge variant="secondary" className="bg-slate-100/80">
                    {chunkSelection.length} selected chunk{chunkSelection.length > 1 ? "s" : ""}
                  </Badge>
                  <span className="truncate">
                    {chunkSelection
                      .slice(0, 3)
                      .map((p) => p.split(/[\\/]/).pop() ?? p)
                      .join(" · ")}
                    {chunkSelection.length > 3 ? " …" : ""}
                  </span>
                  <Button
                    type="button"
                    size="sm"
                    variant="ghost"
                    className="h-7 px-2 text-[11px] font-semibold text-slate-600 hover:bg-slate-100"
                    onClick={() => setChunkSelection([])}
                  >
                    Clear
                  </Button>
                </div>
              ) : null}
            </div>
            <div className="grid grid-cols-3 gap-2">
              <StatPill label="Chunks" value={indexQuery.data?.chunks.length ?? 0} />
              <StatPill label="Total bytes" value={formatBytes(totalBytes)} />
              <StatPill label="Format" value={formatList || "n/a"} />
            </div>
          </div>
        </section>

        <div className="grid flex-1 min-h-0 grid-cols-1 gap-3 lg:grid-cols-3">
          <DataCard
            title="Chunks"
            icon={<HardDrive className="h-4 w-4 text-emerald-600" />}
            footerHint="Load an index.json or chunk list to populate the view."
          >
            <div className="flex h-full flex-col">
              <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                {(indexQuery.data?.chunks ?? []).map((chunk) => (
                  <div
                    key={chunk.filename}
                    className={cn(
                      "grid cursor-pointer grid-cols-1 gap-2 border-b border-slate-100 px-4 py-3 transition",
                      selectedChunk?.filename === chunk.filename
                        ? "border-l-[3px] border-l-emerald-500 bg-emerald-50/70"
                        : "hover:bg-slate-50",
                    )}
                    onClick={() => selectChunk(chunk.filename)}
                  >
                    <div className="flex items-center justify-between gap-3">
                      <div className="font-semibold text-slate-900">{chunk.filename}</div>
                      <Badge variant={chunk.exists ? "accent" : "secondary"}>
                        {chunk.exists ? "On disk" : "Missing"}
                      </Badge>
                    </div>
                    <div className="flex flex-wrap gap-2 text-xs text-slate-600">
                      <Badge variant="secondary">{formatBytes(chunk.chunkBytes)}</Badge>
                      <Badge variant="secondary">{chunk.chunkSize} items</Badge>
                      {chunk.dim ? <Badge variant="secondary">{chunk.dim} dim</Badge> : null}
                    </div>
                  </div>
                ))}
                {!indexQuery.data?.chunks?.length ? <EmptyState hint="Load an index.json to see chunks." /> : null}
              </ScrollArea>
            </div>
          </DataCard>

          <DataCard
            title="Items"
            icon={<BadgeInfo className="h-4 w-4 text-sky-600" />}
            footerHint="Pick a chunk to list its items."
          >
            <div className="flex h-full flex-col">
              <ScrollArea className="flex-1 min-h-0 rounded-[18px] border border-slate-200/70 bg-white/80">
                {(itemsQuery.data ?? []).map((item) => (
                  <div
                    key={item.itemIndex}
                    className={cn(
                      "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-4 py-3 transition",
                      selectedItem?.itemIndex === item.itemIndex
                        ? "border-l-[3px] border-l-sky-500 bg-sky-50/70"
                        : "hover:bg-slate-50",
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

          <DataCard
            title="Fields"
            icon={<Play className="h-4 w-4 text-cyan-600" />}
            footerHint="Double-click a field to open with your default app."
          >
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
                  <PreviewPanel preview={previewQuery.data} />
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
                <span className={cn(errorMessage ? "text-amber-700" : "text-slate-600")}>
                  {errorMessage ?? statusMessage ?? "Idle"}
                </span>
              </div>
            </div>
          </DataCard>
        </div>
      </div>
    </main>
  );
}

function StatPill({ label, value }: { label: string; value: string | number }) {
  return (
    <div className="rounded-[16px] border border-white/70 bg-white/80 px-3 py-2 text-sm shadow-sm">
      <div className="text-[11px] uppercase tracking-[0.08em] text-slate-500">{label}</div>
      <div className="text-lg font-semibold text-slate-900">{value}</div>
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

function PreviewPanel({ preview }: { preview: FieldPreview }) {
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm font-semibold text-slate-900">
          <Sparkles className="h-4 w-4 text-emerald-600" />
          Preview
        </div>
        <Badge variant="secondary">
          {preview.guessedExt ? `.${preview.guessedExt}` : "unknown"} · {formatBytes(preview.size)}
        </Badge>
      </div>
      {preview.previewText ? (
        <pre className="max-h-60 whitespace-pre-wrap break-all rounded-xl bg-slate-50 px-3 py-2 text-xs text-slate-800 shadow-inner">
          {preview.previewText}
        </pre>
      ) : (
        <div className="text-xs text-slate-600 break-all">
          Hex: <span className="font-mono text-slate-800 break-all whitespace-pre-wrap">{preview.hexSnippet}</span>
        </div>
      )}
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
