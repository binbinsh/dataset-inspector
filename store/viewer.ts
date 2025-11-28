import { create } from "zustand";

type LoadMode =
  | { kind: "index"; indexPath: string; requestId: number }
  | { kind: "chunks"; paths: string[]; requestId: number };

type ViewerState = {
  indexPath: string;
  chunkSelection: string[];
  mode: LoadMode | null;
  selectedChunkName: string | null;
  selectedItemIndex: number | null;
  selectedFieldIndex: number | null;
  statusMessage: string | null;
  setIndexPath: (path: string) => void;
  setChunkSelection: (paths: string[]) => void;
  triggerLoad: (mode: "index" | "chunks", payload?: string[]) => void;
  selectChunk: (filename: string | null) => void;
  selectItem: (idx: number | null) => void;
  selectField: (idx: number | null) => void;
  setStatusMessage: (message: string | null) => void;
  clearMode: () => void;
};

export const useViewerStore = create<ViewerState>((set, get) => ({
  indexPath: "",
  chunkSelection: [],
  mode: null,
  selectedChunkName: null,
  selectedItemIndex: null,
  selectedFieldIndex: null,
  statusMessage: null,
  setIndexPath: (path) => set({ indexPath: path }),
  setChunkSelection: (paths) => set({ chunkSelection: paths }),
  triggerLoad: (mode, payload) => {
    const requestId = Date.now();
    if (mode === "index") {
      const indexPath = get().indexPath.trim();
      if (!indexPath) return;
      set({
        mode: { kind: "index", indexPath, requestId },
        selectedChunkName: null,
        selectedItemIndex: null,
        selectedFieldIndex: null,
      });
      return;
    }
    const paths = payload ?? get().chunkSelection;
    if (!paths.length) return;
    set({
      mode: { kind: "chunks", paths, requestId },
      selectedChunkName: null,
      selectedItemIndex: null,
      selectedFieldIndex: null,
    });
  },
  selectChunk: (filename) => set({ selectedChunkName: filename, selectedItemIndex: null, selectedFieldIndex: null }),
  selectItem: (idx) => set({ selectedItemIndex: idx, selectedFieldIndex: null }),
  selectField: (idx) => set({ selectedFieldIndex: idx }),
  setStatusMessage: (message) => set({ statusMessage: message }),
  clearMode: () =>
    set({
      mode: null,
      selectedChunkName: null,
      selectedItemIndex: null,
      selectedFieldIndex: null,
    }),
}));
