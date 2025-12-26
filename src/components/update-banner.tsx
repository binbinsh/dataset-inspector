import { useCallback, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { listen } from "@tauri-apps/api/event";
import { AnimatePresence, motion } from "framer-motion";
import { Button, Chip, Modal, ModalBody, ModalContent, ModalFooter, ModalHeader, Progress } from "@heroui/react";
import { RefreshCw, TriangleAlert, X } from "lucide-react";

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

  const openAndRunManualCheck = useCallback(() => {
    setDismissed(false);
    setDownloadState({ state: "idle" });
    setCheckDialogOpen(true);
    manualCheckMutation.mutate();
  }, [manualCheckMutation]);

  useEffect(() => {
    if (!tauri) return;
    let unlisten: null | (() => void) = null;

    void listen("app://check-updates", () => {
      openAndRunManualCheck();
    }).then((fn) => {
      unlisten = fn;
    });

    const onCustomCheck = () => openAndRunManualCheck();
    window.addEventListener("dataset-inspector:check-updates", onCustomCheck);

    return () => {
      unlisten?.();
      window.removeEventListener("dataset-inspector:check-updates", onCustomCheck);
    };
  }, [openAndRunManualCheck, tauri]);

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
    const downloaded = downloadState.downloaded;
    if (!total || total <= 0) return null;
    return Math.min(100, Math.max(0, (downloaded / total) * 100));
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

  const isSignatureVerificationError = useMemo(() => {
    if (downloadState.state !== "error") return false;
    return /signature verification failed/i.test(downloadState.message);
  }, [downloadState]);

  return (
    <>
      <AnimatePresence>
        {tauri && hasUpdate && !checkDialogOpen && !dismissed ? (
          <motion.div
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 10 }}
            transition={{ duration: 0.2 }}
            className="fixed bottom-6 right-6 z-50 flex max-w-sm flex-col gap-3 rounded-2xl border border-white/70 bg-white/80 p-4 shadow-[var(--shadow-soft)] backdrop-blur"
          >
            <div className="flex items-start justify-between gap-2">
              <div>
                <div className="text-sm font-semibold text-slate-900">Update available</div>
                <div className="text-xs text-slate-600">
                  {update?.currentVersion} → {update?.version}
                </div>
              </div>
              <Button isIconOnly size="sm" variant="light" onClick={() => setDismissed(true)}>
                <X className="h-4 w-4" />
              </Button>
            </div>
            <div className="flex items-center gap-2">
              <Button size="sm" variant="bordered" onClick={openAndRunManualCheck}>
                Review update
              </Button>
              <Button size="sm" variant="light" onClick={() => setDismissed(true)}>
                Later
              </Button>
            </div>
          </motion.div>
        ) : null}
      </AnimatePresence>

      <Modal
        isOpen={tauri && checkDialogOpen}
        onClose={closeCheckDialog}
        size="lg"
        backdrop="blur"
        hideCloseButton
      >
        <ModalContent>
          <ModalHeader className="flex items-center justify-between gap-3">
            <div className="flex items-center gap-2 text-base font-semibold">
              <Chip variant="flat" color="secondary">
                Updates
              </Chip>
              <span className="text-slate-900">Dataset Inspector</span>
            </div>
            <Button isIconOnly size="sm" variant="light" onClick={closeCheckDialog} isDisabled={installMutation.isPending}>
              <X className="h-4 w-4" />
            </Button>
          </ModalHeader>
          <ModalBody className="gap-4">
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
                <span className="text-slate-900">Update available</span>
              ) : (
                <span className="text-slate-900">You&apos;re up to date.</span>
              )}
            </div>

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
            ) : null}

            {downloadState.state === "error" ? (
              <div className="rounded-xl border border-rose-200 bg-rose-50 p-3 text-sm text-rose-800">
                {isSignatureVerificationError ? (
                  <div className="flex flex-col gap-1">
                    <div className="font-medium">Signature verification failed.</div>
                    <div className="text-rose-800/90">The downloaded update could not be verified.</div>
                  </div>
                ) : (
                  downloadState.message
                )}
              </div>
            ) : null}

            {downloadState.state === "started" ? (
              <div className="space-y-2">
                <Progress value={progressPct ?? 0} />
                <div className="text-xs text-slate-500">{progressLabel}</div>
              </div>
            ) : null}
          </ModalBody>
          <ModalFooter className="gap-2">
            <Button variant="light" onClick={closeCheckDialog} isDisabled={installMutation.isPending}>
              Close
            </Button>
            {update ? (
              <Button
                color="success"
                onClick={() => installMutation.mutate()}
                isLoading={installMutation.isPending}
              >
                Install update
              </Button>
            ) : null}
          </ModalFooter>
        </ModalContent>
      </Modal>
    </>
  );
}
