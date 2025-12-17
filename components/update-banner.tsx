"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { AnimatePresence, motion } from "framer-motion";
import { RefreshCw, TriangleAlert, X } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { cn } from "@/lib/utils";
import { checkAppUpdate, downloadAndInstallAppUpdate, isTauri, relaunchApp } from "@/lib/tauri-api";

type DownloadState =
  | { state: "idle" }
  | { state: "started"; contentLength: number | null; downloaded: number }
  | { state: "finished" }
  | { state: "error"; message: string };

export default function UpdateBanner() {
  const tauri = useMemo(() => isTauri(), []);
  const queryClient = useQueryClient();
  const [dismissed, setDismissed] = useState(false);
  const [downloadState, setDownloadState] = useState<DownloadState>({ state: "idle" });
  const [checkDialogOpen, setCheckDialogOpen] = useState(false);

  const updateQuery = useQuery({
    queryKey: ["updater", "check"],
    queryFn: checkAppUpdate,
    enabled: tauri && !dismissed,
    refetchInterval: 6 * 60 * 60 * 1000,
    refetchIntervalInBackground: true,
  });

  const update = updateQuery.data ?? null;
  const hasUpdate = Boolean(update);

  const manualCheckMutation = useMutation({
    mutationFn: checkAppUpdate,
    onSuccess: (result) => {
      queryClient.setQueryData(["updater", "check"], result);
    },
  });

  useEffect(() => {
    if (!tauri) return;
    let unlisten: null | (() => void) = null;

    void listen("app://check-updates", () => {
      setDismissed(false);
      setDownloadState({ state: "idle" });
      setCheckDialogOpen(true);
      manualCheckMutation.mutate();
    }).then((fn) => {
      unlisten = fn;
    });

    return () => {
      unlisten?.();
    };
  }, [tauri, manualCheckMutation]);

  const progressLabel = useMemo(() => {
    if (downloadState.state !== "started") return null;
    const total = downloadState.contentLength;
    const downloaded = downloadState.downloaded;
    if (!total || total <= 0) return `Downloaded ${(downloaded / 1024 / 1024).toFixed(1)} MB`;
    const pct = Math.min(100, Math.max(0, Math.round((downloaded / total) * 100)));
    return `${pct}%`;
  }, [downloadState]);

  const progressPct = useMemo(() => {
    if (downloadState.state !== "started") return null;
    const total = downloadState.contentLength;
    if (!total || total <= 0) return null;
    return Math.min(100, Math.max(0, (downloadState.downloaded / total) * 100));
  }, [downloadState]);

  const installMutation = useMutation({
    mutationFn: async () => {
      if (!update) throw new Error("No update available.");
      setDownloadState({ state: "idle" });

      await downloadAndInstallAppUpdate({
        update,
        onProgress: (event) => {
          if (event.event === "Started") {
            const len = typeof event.data.contentLength === "number" ? event.data.contentLength : null;
            setDownloadState({ state: "started", contentLength: len, downloaded: 0 });
            return;
          }

          if (event.event === "Progress") {
            const chunk = typeof event.data.chunkLength === "number" ? event.data.chunkLength : 0;
            setDownloadState((prev) => {
              if (prev.state !== "started") return { state: "started", contentLength: null, downloaded: chunk };
              return { ...prev, downloaded: prev.downloaded + chunk };
            });
            return;
          }

          if (event.event === "Finished") {
            setDownloadState({ state: "finished" });
          }
        },
      });

      await relaunchApp();
    },
    onError: (error) => {
      const message = error instanceof Error ? error.message : String(error);
      setDownloadState({ state: "error", message });
    },
  });

  const closeCheckDialog = useCallback(() => {
    if (installMutation.isPending) return;
    setCheckDialogOpen(false);
    manualCheckMutation.reset();
  }, [installMutation.isPending, manualCheckMutation]);

  const openAndRunManualCheck = useCallback(() => {
    setDismissed(false);
    setDownloadState({ state: "idle" });
    setCheckDialogOpen(true);
    manualCheckMutation.mutate();
  }, [manualCheckMutation]);

  useEffect(() => {
    if (!checkDialogOpen) return;
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") closeCheckDialog();
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, [checkDialogOpen, closeCheckDialog]);

  return (
    <AnimatePresence>
      {tauri && checkDialogOpen ? (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.15 }}
          className="fixed inset-0 z-50 flex items-center justify-center p-6"
          role="dialog"
          aria-modal="true"
          aria-label="Check for Updates"
        >
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="absolute inset-0 bg-slate-900/20 backdrop-blur-sm"
            onClick={closeCheckDialog}
          />
          <motion.div
            initial={{ opacity: 0, y: 10, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 10, scale: 0.98 }}
            transition={{ duration: 0.18 }}
            className="relative w-full max-w-lg"
          >
            <Card className="border-slate-200 bg-white/90 shadow-[var(--shadow-soft)] backdrop-blur">
              <CardHeader className="flex-row items-start justify-between gap-3">
                <div className="flex min-w-0 flex-1 flex-col gap-1">
                  <CardTitle className="text-slate-700">Updates</CardTitle>
                  <div className="text-sm text-slate-700">
                    {manualCheckMutation.isPending ? (
                      <span className="inline-flex items-center gap-2">
                        <RefreshCw className="h-4 w-4 animate-spin text-slate-500" />
                        Checking for updates…
                      </span>
                    ) : manualCheckMutation.isError ? (
                      <span className="inline-flex items-center gap-2 text-rose-700">
                        <TriangleAlert className="h-4 w-4" />
                        Update check failed
                      </span>
                    ) : update ? (
                      <span className="text-slate-800">Update available</span>
                    ) : (
                      <span className="text-slate-800">You&apos;re up to date</span>
                    )}
                  </div>
                </div>
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={closeCheckDialog}
                  disabled={installMutation.isPending}
                  className="h-9 w-9 rounded-full p-0"
                  aria-label="Close"
                >
                  <X className="h-4 w-4" />
                </Button>
              </CardHeader>

              <CardContent className="flex flex-col gap-4">
                {manualCheckMutation.isError ? (
                  <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
                    {manualCheckMutation.error instanceof Error
                      ? manualCheckMutation.error.message
                      : String(manualCheckMutation.error)}
                  </div>
                ) : update ? (
                  <div className="rounded-xl border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">
                    <div className="flex flex-wrap gap-x-6 gap-y-1">
                      <div>
                        <span className="text-emerald-700">Current</span>{" "}
                        <span className="font-semibold">{update.currentVersion}</span>
                      </div>
                      <div>
                        <span className="text-emerald-700">Latest</span>{" "}
                        <span className="font-semibold">{update.version}</span>
                      </div>
                    </div>
                    {update.date ? (
                      <div className="mt-1 text-xs text-emerald-700">Published: {String(update.date)}</div>
                    ) : null}
                    {update.body ? (
                      <div className="mt-2 line-clamp-5 whitespace-pre-wrap text-xs text-emerald-800">{update.body}</div>
                    ) : null}
                  </div>
                ) : (
                  <div className="rounded-xl border border-slate-200 bg-white p-3 text-sm text-slate-700">
                    No updates found for your current version.
                  </div>
                )}

                {downloadState.state === "error" ? (
                  <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
                    {downloadState.message}
                  </div>
                ) : null}

                {downloadState.state === "started" ? (
                  <div className="flex flex-col gap-2 rounded-xl border border-slate-200 bg-white p-3">
                    <div className="flex items-center justify-between text-sm">
                      <span className="text-slate-700">Downloading update…</span>
                      <span className="tabular-nums text-slate-600">{progressLabel}</span>
                    </div>
                    <div className="h-2 overflow-hidden rounded-full bg-slate-100">
                      <div
                        className={cn(
                          "h-full rounded-full bg-emerald-500 transition-[width] duration-200",
                          progressPct == null ? "w-1/3 animate-pulse" : "",
                        )}
                        style={progressPct == null ? undefined : { width: `${progressPct}%` }}
                      />
                    </div>
                  </div>
                ) : null}

                <div className="flex flex-wrap items-center justify-end gap-2">
                  <Button
                    variant="outline"
                    onClick={() => manualCheckMutation.mutate()}
                    disabled={manualCheckMutation.isPending || installMutation.isPending}
                  >
                    <RefreshCw className={cn("mr-2 h-4 w-4", manualCheckMutation.isPending ? "animate-spin" : "")} />
                    Check again
                  </Button>
                  {update ? (
                    <Button onClick={() => installMutation.mutate()} disabled={installMutation.isPending}>
                      {installMutation.isPending ? "Installing…" : "Download & Restart"}
                    </Button>
                  ) : (
                    <Button onClick={closeCheckDialog} disabled={installMutation.isPending}>
                      Done
                    </Button>
                  )}
                </div>
              </CardContent>
            </Card>
          </motion.div>
        </motion.div>
      ) : null}

      {tauri && hasUpdate && !dismissed ? (
        <motion.div
          initial={{ opacity: 0, y: 8 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: 8 }}
          transition={{ duration: 0.18 }}
          className="pointer-events-none fixed right-4 top-4 z-50 w-[360px]"
        >
          <Card className="pointer-events-auto border-amber-200 bg-amber-50/80 backdrop-blur">
            <CardHeader className="flex-row items-start justify-between gap-3">
              <div className="flex min-w-0 flex-1 flex-col gap-1">
                <CardTitle className="text-amber-700">Update available</CardTitle>
                <div className="flex items-center gap-2 text-sm font-medium text-slate-900">
                  <TriangleAlert className="h-4 w-4 text-amber-600" />
                  <span className="truncate">{update?.version}</span>
                </div>
              </div>

              <Button
                variant="ghost"
                size="sm"
                className="h-8 w-8 px-0 text-slate-600 hover:text-slate-900"
                onClick={() => setDismissed(true)}
                aria-label="Dismiss update"
              >
                <X className="h-4 w-4" />
              </Button>
            </CardHeader>

            <CardContent className="flex flex-col gap-3">
              <div className="text-xs text-slate-600">
                {installMutation.isPending ? (
                  <span className="font-medium text-amber-800">
                    Installing… {progressLabel ? <span className="tabular-nums">{progressLabel}</span> : null}
                  </span>
                ) : downloadState.state === "error" ? (
                  <span className="font-medium text-rose-700">{downloadState.message}</span>
                ) : (
                  <span>Restart is required after installation.</span>
                )}
              </div>

              <div className="flex items-center justify-end gap-2">
                <Button
                  variant="ghost"
                  size="sm"
                  className={cn("gap-2", updateQuery.isFetching ? "opacity-70" : "")}
                  onClick={openAndRunManualCheck}
                  disabled={installMutation.isPending}
                >
                  <RefreshCw className={cn("h-4 w-4", manualCheckMutation.isPending ? "animate-spin" : "")} />
                  Details
                </Button>

                <Button size="sm" onClick={() => installMutation.mutate()} disabled={installMutation.isPending}>
                  Install & Restart
                </Button>
              </div>
            </CardContent>
          </Card>
        </motion.div>
      ) : null}
    </AnimatePresence>
  );
}
