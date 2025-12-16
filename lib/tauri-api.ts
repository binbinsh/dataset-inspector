import { invoke } from "@tauri-apps/api/core";
import { dirname } from "@tauri-apps/api/path";
import { open as openDialog } from "@tauri-apps/plugin-dialog";
import { Store } from "@tauri-apps/plugin-store";

export type ChunkSummary = {
  filename: string;
  path: string;
  chunkSize: number;
  chunkBytes: number;
  dim?: number | null;
  exists: boolean;
};

export type IndexSummary = {
  indexPath: string;
  rootDir: string;
  dataFormat: string[];
  compression?: string | null;
  chunkSize?: number | null;
  chunkBytes?: number | null;
  configRaw?: Record<string, unknown> | null;
  chunks: ChunkSummary[];
};

export type FieldMeta = {
  fieldIndex: number;
  size: number;
};

export type ItemMeta = {
  itemIndex: number;
  totalBytes: number;
  fields: FieldMeta[];
};

export type FieldPreview = {
  previewText?: string | null;
  hexSnippet: string;
  guessedExt?: string | null;
  isBinary: boolean;
  size: number;
};

export type OpenLeafResponse = {
  path: string;
  size: number;
  ext: string;
  opened: boolean;
  needsOpener: boolean;
  message: string;
};

const STORE_NAME = "litdata-viewer.bin";
const STORE_LAST_INDEX = "last_index";
const STORE_OPENERS_BY_EXT = "openers_by_ext";

let storeInstance: Store | null = null;

export const isTauri = () => {
  if (typeof window === "undefined") return false;
  const w = window as any;
  const ua = String(w.navigator?.userAgent || "").toLowerCase();
  return Boolean(
    w.__TAURI_INTERNALS__ ||
      w.__TAURI__ ||
      w.__TAURI_METADATA__ ||
      w.__TAURI_IPC__ ||
      ua.includes("tauri") ||
      w.location?.protocol === "tauri:" ||
      (typeof w.location?.href === "string" && w.location.href.startsWith("tauri://"))
  );
};

async function requireTauri(task: string) {
  if (!isTauri()) {
    throw new Error(`${task} requires the Tauri runtime.`);
  }
}

async function getStore(): Promise<Store> {
  if (storeInstance) return storeInstance;
  await requireTauri("Reading preferences");
  storeInstance = await Store.load(STORE_NAME);
  return storeInstance;
}

export async function saveLastIndex(indexPath: string) {
  if (!isTauri()) return;
  const store = await getStore();
  await store.set(STORE_LAST_INDEX, indexPath);
  await store.save();
}

export async function readLastIndex(): Promise<string | null> {
  if (!isTauri()) return null;
  const store = await getStore();
  return (await store.get<string>(STORE_LAST_INDEX)) ?? null;
}

type OpenersByExt = Record<string, string>;

export async function readPreferredOpenerForExt(ext: string): Promise<string | null> {
  if (!isTauri()) return null;
  const normalized = ext.trim().replace(/^\./, "").toLowerCase();
  if (!normalized) return null;
  const store = await getStore();
  const map = (await store.get<OpenersByExt>(STORE_OPENERS_BY_EXT)) ?? {};
  return map[normalized] ?? null;
}

export async function savePreferredOpenerForExt(ext: string, appPath: string) {
  if (!isTauri()) return;
  const normalized = ext.trim().replace(/^\./, "").toLowerCase();
  const trimmedPath = appPath.trim();
  if (!normalized || !trimmedPath) return;
  const store = await getStore();
  const map = (await store.get<OpenersByExt>(STORE_OPENERS_BY_EXT)) ?? {};
  map[normalized] = trimmedPath;
  await store.set(STORE_OPENERS_BY_EXT, map);
  await store.save();
}

export async function chooseOpenerApp(): Promise<string | null> {
  await requireTauri("Choosing an application");
  const ua = typeof navigator === "undefined" ? "" : String(navigator.userAgent || "");
  const isMac = /Macintosh|Mac OS X/i.test(ua);
  const picked = await openDialog({
    title: isMac ? "Choose an application (.app)" : "Choose an application",
    multiple: false,
    ...(isMac
      ? {
          filters: [{ name: "Applications", extensions: ["app"] }],
          defaultPath: "/Applications",
        }
      : {}),
  });
  if (!picked || Array.isArray(picked)) return null;
  return typeof picked === "string" ? picked : null;
}

async function resolveDefaultDialogPath(path?: string, rootDir?: string): Promise<string | undefined> {
  const trimmed = (path ?? "").trim();
  if (trimmed.endsWith("/") || trimmed.endsWith("\\")) return trimmed;
  const fallbackRoot = (rootDir ?? "").trim();
  const candidate = trimmed || fallbackRoot;
  if (!candidate) return undefined;
  const looksLikeFile = /\.(json|bin|zst)$/i.test(candidate);
  if (!looksLikeFile) return candidate;
  try {
    return await dirname(candidate);
  } catch {
    return candidate;
  }
}

export type PickResult =
  | { kind: "index"; indexPath: string }
  | { kind: "chunks"; paths: string[] };

export async function chooseIndexSource(currentPath: string, lastRoot?: string): Promise<PickResult | null> {
  await requireTauri("Choosing files");
  const defaultPath = (await resolveDefaultDialogPath(currentPath, lastRoot)) ?? undefined;
  const picked = await openDialog({
    title: "Select litdata index.json or chunk .bin/.zst files",
    multiple: true,
    filters: [
      { name: "LitData index", extensions: ["json"] },
      { name: "LitData chunk", extensions: ["bin", "zst"] },
      { name: "All supported", extensions: ["json", "bin", "zst"] },
    ],
    ...(defaultPath ? { defaultPath } : {}),
  });
  if (!picked) return null;
  if (Array.isArray(picked) && picked.length > 1) {
    return { kind: "chunks", paths: picked };
  }
  const first = Array.isArray(picked) ? picked[0] : picked;
  if (typeof first !== "string") return null;
  if (first.endsWith(".bin") || first.endsWith(".zst") || first.includes(".bin")) {
    return { kind: "chunks", paths: [first] };
  }
  return { kind: "index", indexPath: first };
}

export async function loadIndex(indexPath: string): Promise<IndexSummary> {
  await requireTauri("Loading index");
  const trimmed = indexPath.trim();
  if (!trimmed) throw new Error("Provide an index.json path to load.");
  return invoke<IndexSummary>("load_index", { indexPath: trimmed });
}

export async function loadChunkList(paths: string[]): Promise<IndexSummary> {
  await requireTauri("Loading chunks");
  if (!paths.length) throw new Error("Select at least one chunk file to load.");
  return invoke<IndexSummary>("load_chunk_list", { paths });
}

export async function listChunkItems(params: { indexPath: string; chunkFilename: string }): Promise<ItemMeta[]> {
  await requireTauri("Reading chunk");
  return invoke<ItemMeta[]>("list_chunk_items", params);
}

export async function peekField(params: {
  indexPath: string;
  chunkFilename: string;
  itemIndex: number;
  fieldIndex: number;
}): Promise<FieldPreview> {
  await requireTauri("Previewing data");
  return invoke<FieldPreview>("peek_field", params);
}

export async function openLeaf(params: {
  indexPath: string;
  chunkFilename: string;
  itemIndex: number;
  fieldIndex: number;
  openerAppPath?: string | null;
}): Promise<OpenLeafResponse> {
  await requireTauri("Opening field");
  return invoke<OpenLeafResponse>("open_leaf", params);
}

export async function openPathWithApp(params: { path: string; appPath: string }): Promise<string> {
  await requireTauri("Opening with app");
  const path = params.path.trim();
  const appPath = params.appPath.trim();
  if (!path) throw new Error("Missing file path to open.");
  if (!appPath) throw new Error("Missing app path to open with.");
  return invoke<string>("open_path_with_app", { path, appPath });
}
