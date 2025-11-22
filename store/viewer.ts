import { create } from "zustand";
import { invoke } from "@tauri-apps/api/core";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { Store } from "@tauri-apps/plugin-store";

type ChunkSummary = {
  filename: string;
  path: string;
  chunkSize: number;
  chunkBytes: number;
  dim?: number | null;
  exists: boolean;
};

type IndexSummary = {
  indexPath: string;
  rootDir: string;
  dataFormat: string[];
  compression?: string | null;
  chunkSize?: number | null;
  chunkBytes?: number | null;
  configRaw?: Record<string, any> | null;
  chunks: ChunkSummary[];
};

type FieldMeta = {
  fieldIndex: number;
  size: number;
};

type ItemMeta = {
  itemIndex: number;
  totalBytes: number;
  fields: FieldMeta[];
};

type FieldPreview = {
  previewText?: string | null;
  hexSnippet: string;
  guessedExt?: string | null;
  isBinary: boolean;
  size: number;
};

const STORE_NAME = "litdata-viewer.bin";
const STORE_LAST_INDEX = "last_index";
let storeInstance: Store | null = null;

async function getStore(): Promise<Store> {
  if (storeInstance) return storeInstance;
  storeInstance = await Store.load(STORE_NAME);
  return storeInstance;
}

const isTauri = () => typeof window !== "undefined" && Boolean((window as any).__TAURI__);

type State = {
  indexPath: string;
  indexMeta: IndexSummary | null;
  items: ItemMeta[];
  selectedChunk: ChunkSummary | null;
  selectedItem: ItemMeta | null;
  selectedField: FieldMeta | null;
  fieldPreview: FieldPreview | null;
  status: string | null;
  error: string | null;
  busy: boolean;
  chunkSelection: string[];
  setIndexPath: (path: string) => void;
  chooseIndex: () => Promise<void>;
  loadIndex: (path?: string) => Promise<void>;
  loadChunks: (paths: string[]) => Promise<void>;
  selectChunk: (chunk: ChunkSummary) => Promise<void>;
  selectItem: (item: ItemMeta) => void;
  selectField: (field: FieldMeta) => Promise<void>;
  openField: (field: FieldMeta, item: ItemMeta) => Promise<void>;
  hydrate: () => Promise<void>;
};

export const useViewerStore = create<State>((set, get) => ({
  indexPath: "",
  indexMeta: null,
  items: [],
  selectedChunk: null,
  selectedItem: null,
  selectedField: null,
  fieldPreview: null,
  status: null,
  error: null,
  busy: false,
  chunkSelection: [],
  setIndexPath: (path) => set({ indexPath: path }),
  hydrate: async () => {
    if (!isTauri()) return;
    const store = await getStore();
    const last = await store.get<string>(STORE_LAST_INDEX);
    if (last) set({ indexPath: last });
  },
  chooseIndex: async () => {
    if (!isTauri()) {
      set({ error: "Tauri runtime is required for file dialogs." });
      return;
    }
    const picked = await openDialog({
      title: "Select litdata index.json or chunk .bin",
      multiple: true,
      filters: [
        { name: "LitData index", extensions: ["json"] },
        { name: "LitData chunk", extensions: ["bin", "zst"] },
        { name: "All supported", extensions: ["json", "bin", "zst"] },
      ],
    });
    if (Array.isArray(picked) && picked.length > 1) {
      set({ indexPath: picked[0] });
      await get().loadChunks(picked);
    } else {
      const first = Array.isArray(picked) ? picked[0] : picked;
      if (typeof first === "string") {
        if (first.endsWith(".bin") || first.endsWith(".zst") || first.includes(".bin")) {
          set({ indexPath: first });
          await get().loadChunks([first]);
        } else {
          set({ indexPath: first });
          await get().loadIndex(first);
        }
      }
    }
  },
  loadIndex: async (path) => {
    const indexPath = (path ?? get().indexPath).trim();
    if (!indexPath) return;
    if (!isTauri()) {
      set({ error: "Tauri runtime is required to load index." });
      return;
    }
    try {
      set({ busy: true, error: null, status: null });
      const meta = await invoke<IndexSummary>("load_index", { indexPath });
      set({
        indexPath,
        indexMeta: meta,
        items: [],
        selectedChunk: null,
        selectedItem: null,
        selectedField: null,
        fieldPreview: null,
        chunkSelection: [],
        status: `Loaded ${meta.chunks.length} chunk(s)`,
      });
      const store = await getStore();
      await store.set(STORE_LAST_INDEX, indexPath);
      await store.save();
    } catch (err: any) {
      set({ error: String(err) });
    } finally {
      set({ busy: false });
    }
  },
  loadChunks: async (paths: string[]) => {
    if (!paths.length) return;
    if (!isTauri()) {
      set({ error: "Tauri runtime is required to load chunks." });
      return;
    }
    try {
      set({ busy: true, error: null, status: null });
      const meta = await invoke<IndexSummary>("load_chunk_list", { paths });
      set({
        indexPath: meta.indexPath,
        indexMeta: meta,
        items: [],
        selectedChunk: null,
        selectedItem: null,
        selectedField: null,
        fieldPreview: null,
        chunkSelection: paths,
        status: `Loaded ${meta.chunks.length} chunk(s)`,
      });
    } catch (err: any) {
      set({ error: String(err) });
    } finally {
      set({ busy: false });
    }
  },
  selectChunk: async (chunk) => {
    const indexPath = get().indexPath.trim();
    if (!indexPath) return;
    if (!isTauri()) {
      set({ error: "Tauri runtime is required to read chunk." });
      return;
    }
    try {
      set({ busy: true, error: null, status: `Parsing ${chunk.filename}` });
      const items = await invoke<ItemMeta[]>("list_chunk_items", {
        indexPath,
        chunkFilename: chunk.filename,
      });
      set({
        selectedChunk: chunk,
        items,
        selectedItem: items[0] ?? null,
        selectedField: null,
        fieldPreview: null,
        status: `Loaded ${items.length} item(s)`,
      });
    } catch (err: any) {
      set({ error: String(err) });
    } finally {
      set({ busy: false });
    }
  },
  selectItem: (item) => set({ selectedItem: item, selectedField: null, fieldPreview: null }),
  selectField: async (field) => {
    const { selectedItem, selectedChunk, indexPath } = get();
    if (!selectedItem || !selectedChunk || !indexPath.trim()) return;
    if (!isTauri()) {
      set({ error: "Tauri runtime is required to preview." });
      return;
    }
    try {
      set({ busy: true, error: null });
      const preview = await invoke<FieldPreview>("peek_field", {
        indexPath: indexPath.trim(),
        chunkFilename: selectedChunk.filename,
        itemIndex: selectedItem.itemIndex,
        fieldIndex: field.fieldIndex,
      });
      set({ selectedField: field, fieldPreview: preview });
    } catch (err: any) {
      set({ error: String(err) });
    } finally {
      set({ busy: false });
    }
  },
  openField: async (field, item) => {
    const { selectedChunk, indexPath } = get();
    if (!selectedChunk || !indexPath.trim()) return;
    if (!isTauri()) {
      set({ error: "Tauri runtime is required to open files." });
      return;
    }
    try {
      set({ busy: true, error: null });
      const opened = await invoke<string>("open_leaf", {
        indexPath: indexPath.trim(),
        chunkFilename: selectedChunk.filename,
        itemIndex: item.itemIndex,
        fieldIndex: field.fieldIndex,
      });
      set({ status: `Opened: ${opened}` });
    } catch (err: any) {
      set({ error: String(err) });
    } finally {
      set({ busy: false });
    }
  },
}));
