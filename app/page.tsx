"use client";

import { useEffect } from "react";
import { FolderOpen, Play, HardDrive, Loader2, FileWarning, Wand2 } from "lucide-react";

import { useViewerStore } from "@/store/viewer";
import { Button } from "@/components/ui/button";
import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Input } from "@/components/ui/input";
import { cn } from "@/lib/utils";
import { ScrollArea } from "@/components/ui/scroll-area";
import { Skeleton } from "@/components/ui/skeleton";

const formatBytes = (value: number) => {
  const units = ["B", "KB", "MB", "GB"];
  let v = value;
  let idx = 0;
  while (v >= 1024 && idx < units.length - 1) {
    v /= 1024;
    idx += 1;
  }
  return `${v.toFixed(v >= 10 ? 0 : 1)} ${units[idx]}`;
};

export default function Page() {
  const {
    indexPath,
    indexMeta,
    items,
    selectedChunk,
    selectedItem,
    selectedField,
    fieldPreview,
    status,
    error,
    busy,
    setIndexPath,
    hydrate,
    chooseIndex,
    loadIndex,
    loadChunks,
    selectChunk,
    selectItem,
    selectField,
    openField,
    chunkSelection,
  } = useViewerStore();

  useEffect(() => {
    void hydrate();
  }, [hydrate]);

  const formatList = (indexMeta?.dataFormat ?? []).join(" · ");
  const totalBytes =
    indexMeta?.chunks?.reduce((acc, chunk) => acc + (Number.isFinite(chunk.chunkBytes) ? chunk.chunkBytes : 0), 0) ||
    0;

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-50 via-white to-slate-100 p-4 font-sans">
      <div className="mx-auto flex max-w-screen-2xl flex-col gap-4">
        <div className="relative overflow-hidden rounded-2xl border border-slate-200/60 bg-white/85 p-4 shadow-lg backdrop-blur">
          <div className="absolute inset-y-0 right-0 w-64 bg-[radial-gradient(circle_at_20%_20%,#34d39933,transparent_50%),radial-gradient(circle_at_80%_0%,#0ea5e933,transparent_40%)]" />
          <div className="relative flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
            <div className="space-y-2">
              <div className="flex flex-wrap items-center gap-2">
                <h1 className="text-2xl font-semibold text-slate-900">LitData Viewer</h1>
              </div>
              <p className="text-sm text-slate-600">
                Browse chunked .bin shards like tar files. Double-click a field to open with native apps.
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-2">
              <Input
                className="w-80"
                placeholder="/abs/path/to/index.json"
                value={indexPath}
                onChange={(e) => setIndexPath(e.target.value)}
                aria-label="Index path"
              />
              <Button
                variant="outline"
                className="border-emerald-200 text-emerald-700 hover:bg-emerald-50"
                onClick={chooseIndex}
                disabled={busy}
              >
                <FolderOpen className="mr-2 h-4 w-4" />
                Choose
              </Button>
              <Button
                className="bg-gradient-to-r from-emerald-500 to-cyan-500 shadow-lg shadow-emerald-200"
                onClick={() => {
                  if (chunkSelection.length > 0) {
                    void loadChunks(chunkSelection);
                  } else {
                    void loadIndex();
                  }
                }}
                disabled={busy || !indexPath.trim()}
              >
                {busy ? <Loader2 className="mr-2 h-4 w-4 animate-spin" /> : <HardDrive className="mr-2 h-4 w-4" />}
                Load
              </Button>
            </div>
          </div>
        </div>

        <div className="grid gap-3 xl:grid-cols-3 lg:grid-cols-3 md:grid-cols-2 grid-cols-1">
          <Card className="shadow-sm min-w-0">
            <CardHeader className="pb-2">
              <CardTitle className="flex items-center gap-2">
                <HardDrive className="h-4 w-4 text-emerald-600" />
                Chunks
              </CardTitle>
              <div className="flex items-center gap-2">
                <Badge variant="accent" className="whitespace-nowrap">
                  {indexMeta?.chunks.length ?? 0} files
                </Badge>
                <Badge variant="accent" className="whitespace-nowrap">
                  Total {formatBytes(totalBytes)}
                </Badge>
                <Badge variant="secondary" className="max-w-[180px] truncate" title={formatList || "n/a"}>
                  Format: <span className="ml-1 font-semibold truncate">{formatList || "n/a"}</span>
                </Badge>
              </div>
            </CardHeader>
            <CardContent className="space-y-2">
              <ScrollArea className="h-[calc(100vh-240px)] min-h-[420px] rounded-lg border border-slate-200">
                {(indexMeta?.chunks ?? []).map((chunk) => (
                  <div
                    key={chunk.filename}
                    className={cn(
                      "grid cursor-pointer grid-cols-1 gap-2 border-b border-slate-100 px-3 py-3 transition hover:bg-slate-50",
                      selectedChunk?.filename === chunk.filename &&
                        "bg-emerald-50 border-l-4 border-l-emerald-500 shadow-inner",
                    )}
                    onClick={() => selectChunk(chunk)}
                  >
                    <div className="font-semibold text-slate-900 flex items-center gap-2">
                      {chunk.filename}
                      <Badge variant={chunk.exists ? "accent" : "secondary"}>{chunk.exists ? "On disk" : "Missing"}</Badge>
                    </div>
                    <div className="flex flex-wrap gap-2 text-xs text-slate-600">
                      <Badge variant="secondary">{formatBytes(chunk.chunkBytes)}</Badge>
                      <Badge variant="secondary">{chunk.chunkSize} items</Badge>
                      {chunk.dim ? <Badge variant="secondary">{chunk.dim} dim</Badge> : null}
                    </div>
                  </div>
                ))}
              </ScrollArea>
              {!indexMeta?.chunks?.length && <EmptyState hint="Load an index.json to see chunks." />}
            </CardContent>
          </Card>

          <Card className="shadow-sm min-w-0">
            <CardHeader className="pb-2">
              <CardTitle>Items</CardTitle>
            </CardHeader>
            <CardContent>
              <ScrollArea className="h-[calc(100vh-240px)] min-h-[420px] rounded-lg border border-slate-200">
                {items.map((item) => (
                  <div
                    key={item.itemIndex}
                    className={cn(
                      "grid cursor-pointer grid-cols-[1fr_auto] items-center gap-3 border-b border-slate-100 px-3 py-3 transition hover:bg-slate-50",
                      selectedItem?.itemIndex === item.itemIndex &&
                        "bg-emerald-50 border-l-4 border-l-emerald-500 shadow-inner",
                    )}
                    onClick={() => selectItem(item)}
                  >
                    <div className="font-semibold text-slate-900">Item {item.itemIndex}</div>
                    <div className="flex flex-wrap gap-2 text-xs text-slate-600">
                      <Badge variant="secondary">{formatBytes(item.totalBytes)}</Badge>
                      <Badge variant="secondary">{item.fields.length} leaves</Badge>
                    </div>
                  </div>
                ))}
                {!items.length && <EmptyState hint="Pick a chunk to list its items." />}
              </ScrollArea>
            </CardContent>
          </Card>

          <Card className="shadow-sm min-w-0">
            <CardHeader className="pb-2">
              <CardTitle>Fields</CardTitle>
            </CardHeader>
            <CardContent className="space-y-3">
              {selectedItem ? (
                <ScrollArea className="h-[calc(60vh)] min-h-[260px] rounded-lg border border-slate-200">
                  {selectedItem.fields.map((field) => (
                    <div
                      key={field.fieldIndex}
                      className={cn(
                        "grid cursor-pointer grid-cols-[1fr_auto_auto] items-center gap-2 border-b border-slate-100 px-3 py-3 transition hover:bg-slate-50",
                        selectedField?.fieldIndex === field.fieldIndex &&
                          "bg-emerald-50 border-l-4 border-l-emerald-500 shadow-inner",
                      )}
                      onClick={() => selectField(field)}
                      onDoubleClick={() => openField(field, selectedItem)}
                    >
                      <div className="font-semibold text-slate-900">
                        #{field.fieldIndex} · {indexMeta?.dataFormat[field.fieldIndex] ?? "unknown"}
                      </div>
                      <div className="text-xs text-slate-500">{formatBytes(field.size)}</div>
                      <Button
                        size="sm"
                        variant="ghost"
                        className="hover:bg-emerald-50"
                        disabled={busy}
                        onClick={(e) => {
                          e.stopPropagation();
                          openField(field, selectedItem);
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

              {fieldPreview ? (
                <div className="rounded-xl border border-dashed border-emerald-200 bg-emerald-50/50 p-3 text-sm shadow-inner">
                  <div className="flex items-center justify-between">
                    <div className="flex items-center gap-2">
                      <Wand2 className="h-4 w-4 text-emerald-600" />
                      <strong>Preview</strong>
                    </div>
                    <Badge variant="secondary">
                      {fieldPreview.guessedExt ? `.${fieldPreview.guessedExt}` : "unknown"} ·{" "}
                      {formatBytes(fieldPreview.size)}
                    </Badge>
                  </div>
                  {fieldPreview.previewText ? (
                    <pre className="mt-2 whitespace-pre-wrap break-all rounded-lg bg-white p-3 text-xs text-slate-800 shadow-inner">
                      {fieldPreview.previewText}
                    </pre>
                  ) : (
                    <div className="mt-2 text-xs text-slate-600 break-all">
                      Hex: {fieldPreview.hexSnippet}
                    </div>
                  )}
                </div>
              ) : (
                <Skeleton className="h-16 w-full" />
              )}

              {(busy || error || status) && (
                <div className="flex items-center gap-2 text-sm">
                  {busy ? <Loader2 className="h-4 w-4 animate-spin text-emerald-600" /> : null}
                  {error ? <FileWarning className="h-4 w-4 text-amber-500" /> : null}
                  <span className={cn(error ? "text-amber-700" : "text-slate-600")}>
                    {error ? `⚠️ ${error}` : status}
                  </span>
                </div>
              )}
            </CardContent>
          </Card>
        </div>
      </div>
    </div>
  );
}

function EmptyState({ hint }: { hint: string }) {
  return (
    <div className="flex h-24 flex-col items-center justify-center text-xs text-slate-500">
      <FileWarning className="mb-1 h-4 w-4" />
      {hint}
    </div>
  );
}
