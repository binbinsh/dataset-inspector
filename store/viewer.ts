import { create } from "zustand";

type LoadMode =
  | { kind: "litdata-index"; indexPath: string; requestId: number }
  | { kind: "litdata-chunks"; paths: string[]; requestId: number }
  | { kind: "huggingface"; input: string; requestId: number };

type ViewerState = {
  sourceInput: string;
  chunkSelection: string[];
  mode: LoadMode | null;

  selectedChunkName: string | null;
  selectedItemIndex: number | null;
  selectedFieldIndex: number | null;

  hfConfigOverride: string | null;
  hfSplitOverride: string | null;
  hfOffset: number;
  hfSelectedRowIndex: number | null;
  hfSelectedFieldName: string | null;

  statusMessage: string | null;

  setSourceInput: (value: string) => void;
  setChunkSelection: (paths: string[]) => void;
  triggerLoad: (mode: "litdata-index" | "litdata-chunks" | "huggingface", payload?: string[] | string) => void;

  selectChunk: (filename: string | null) => void;
  selectItem: (idx: number | null) => void;
  selectField: (idx: number | null) => void;

  setHfConfigSplit: (config: string, split: string) => void;
  setHfOffset: (offset: number) => void;
  selectHfRow: (rowIndex: number | null) => void;
  selectHfField: (fieldName: string | null) => void;

  setStatusMessage: (message: string | null) => void;
  clearMode: () => void;
};

export const useViewerStore = create<ViewerState>((set, get) => ({
  sourceInput: "",
  chunkSelection: [],
  mode: null,

  selectedChunkName: null,
  selectedItemIndex: null,
  selectedFieldIndex: null,

  hfConfigOverride: null,
  hfSplitOverride: null,
  hfOffset: 0,
  hfSelectedRowIndex: null,
  hfSelectedFieldName: null,

  statusMessage: null,

  setSourceInput: (value) => set({ sourceInput: value }),
  setChunkSelection: (paths) => set({ chunkSelection: paths }),
  triggerLoad: (mode, payload) => {
    const requestId = Date.now();
    if (mode === "litdata-index") {
      const indexPath = get().sourceInput.trim();
      if (!indexPath) return;
      set({
        mode: { kind: "litdata-index", indexPath, requestId },
        selectedChunkName: null,
        selectedItemIndex: null,
        selectedFieldIndex: null,
        hfConfigOverride: null,
        hfSplitOverride: null,
        hfOffset: 0,
        hfSelectedRowIndex: null,
        hfSelectedFieldName: null,
      });
      return;
    }

    if (mode === "litdata-chunks") {
      const paths = (payload as string[] | undefined) ?? get().chunkSelection;
      if (!paths.length) return;
      set({
        mode: { kind: "litdata-chunks", paths, requestId },
        selectedChunkName: null,
        selectedItemIndex: null,
        selectedFieldIndex: null,
        hfConfigOverride: null,
        hfSplitOverride: null,
        hfOffset: 0,
        hfSelectedRowIndex: null,
        hfSelectedFieldName: null,
      });
      return;
    }

    const input = typeof payload === "string" ? payload : get().sourceInput;
    const trimmed = input.trim();
    if (!trimmed) return;
    set({
      mode: { kind: "huggingface", input: trimmed, requestId },
      selectedChunkName: null,
      selectedItemIndex: null,
      selectedFieldIndex: null,
      hfConfigOverride: null,
      hfSplitOverride: null,
      hfOffset: 0,
      hfSelectedRowIndex: null,
      hfSelectedFieldName: null,
    });
  },

  selectChunk: (filename) => set({ selectedChunkName: filename, selectedItemIndex: null, selectedFieldIndex: null }),
  selectItem: (idx) => set({ selectedItemIndex: idx, selectedFieldIndex: null }),
  selectField: (idx) => set({ selectedFieldIndex: idx }),

  setHfConfigSplit: (config, split) =>
    set({
      hfConfigOverride: config,
      hfSplitOverride: split,
      hfOffset: 0,
      hfSelectedRowIndex: null,
      hfSelectedFieldName: null,
    }),
  setHfOffset: (offset) => set({ hfOffset: Math.max(0, offset | 0) }),
  selectHfRow: (rowIndex) => set({ hfSelectedRowIndex: rowIndex }),
  selectHfField: (fieldName) => set({ hfSelectedFieldName: fieldName }),

  setStatusMessage: (message) => set({ statusMessage: message }),
  clearMode: () =>
    set({
      mode: null,
      selectedChunkName: null,
      selectedItemIndex: null,
      selectedFieldIndex: null,
      hfConfigOverride: null,
      hfSplitOverride: null,
      hfOffset: 0,
      hfSelectedRowIndex: null,
      hfSelectedFieldName: null,
    }),
}));

