/**
 * QMD Graph Module - Document relationship graph with link extraction and traversal
 */

import type { Database } from "./db.js";

export type GraphNode = {
  id: string;
  hash: string;
  collection: string;
  path: string;
  title: string;
};

export type GraphEdge = {
  id: number;
  source_hash: string;
  target_hash: string;
  type: string;
  context?: string;
};

export type ExtractedLink = {
  targetPath: string;
  type: "link" | "wiki" | "ref";
  context: string;
};

export type GraphNeighbor = {
  hash: string;
  collection: string;
  path: string;
  title: string;
  edgeType: string;
  direction: "incoming" | "outgoing";
  context?: string;
};

export type TraversalResult = {
  node: GraphNode;
  distance: number;
  path: string[];
};

export function initializeGraphSchema(db: Database): void {
  db.exec(`
    CREATE TABLE IF NOT EXISTS graph_nodes (
      hash_id TEXT PRIMARY KEY,
      hash TEXT NOT NULL,
      collection TEXT NOT NULL,
      path TEXT NOT NULL,
      title TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `);
  db.exec("CREATE INDEX IF NOT EXISTS idx_graph_nodes_collection ON graph_nodes(collection)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_graph_nodes_path ON graph_nodes(path)");

  db.exec(`
    CREATE TABLE IF NOT EXISTS graph_edges (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      source_hash TEXT NOT NULL,
      target_hash TEXT NOT NULL,
      type TEXT NOT NULL DEFAULT 'link',
      context TEXT,
      created_at TEXT NOT NULL,
      UNIQUE(source_hash, target_hash, type)
    )
  `);
  db.exec("CREATE INDEX IF NOT EXISTS idx_graph_edges_source ON graph_edges(source_hash)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_graph_edges_target ON graph_edges(target_hash)");
  db.exec("CREATE INDEX IF NOT EXISTS idx_graph_edges_type ON graph_edges(type)");

  db.exec(`
    CREATE VIRTUAL TABLE IF NOT EXISTS graph_edges_fts USING fts5(
      context, tokenize='porter unicode61'
    )
  `);
}

export function upsertGraphNode(db: Database, hash: string, collection: string, path: string, title: string): void {
  const hashId = hash.slice(0, 16);
  const now = new Date().toISOString();
  db.prepare(`INSERT INTO graph_nodes (hash_id, hash, collection, path, title, created_at) VALUES (?, ?, ?, ?, ?, ?) ON CONFLICT(hash_id) DO UPDATE SET collection = excluded.collection, path = excluded.path, title = excluded.title`).run(hashId, hash, collection, path, title, now);
}

export function deleteGraphNode(db: Database, hash: string): void {
  db.prepare("DELETE FROM graph_nodes WHERE hash_id = ?").run(hash.slice(0, 16));
}

export function getGraphNodeByHash(db: Database, hash: string): GraphNode | null {
  return db.prepare("SELECT hash_id as id, hash, collection, path, title FROM graph_nodes WHERE hash_id = ?").get(hash.slice(0, 16)) as GraphNode | null;
}

export function getGraphNodeByPath(db: Database, collection: string, path: string): GraphNode | null {
  return db.prepare("SELECT hash_id as id, hash, collection, path, title FROM graph_nodes WHERE collection = ? AND path = ?").get(collection, path) as GraphNode | null;
}

export function upsertGraphEdge(db: Database, sourceHash: string, targetHash: string, type: string = "link", context?: string): void {
  if (sourceHash === targetHash) return;
  const now = new Date().toISOString();
  db.prepare(`INSERT INTO graph_edges (source_hash, target_hash, type, context, created_at) VALUES (?, ?, ?, ?, ?) ON CONFLICT(source_hash, target_hash, type) DO UPDATE SET context = excluded.context`).run(sourceHash, targetHash, type, context || null, now);
}

export function deleteGraphEdgesForSource(db: Database, sourceHash: string): number {
  return db.prepare("DELETE FROM graph_edges WHERE source_hash = ?").run(sourceHash).changes;
}

export function getOutgoingEdges(db: Database, hash: string): GraphEdge[] {
  return db.prepare("SELECT * FROM graph_edges WHERE source_hash = ?").all(hash) as GraphEdge[];
}

export function getIncomingEdges(db: Database, hash: string): GraphEdge[] {
  return db.prepare("SELECT * FROM graph_edges WHERE target_hash = ?").all(hash) as GraphEdge[];
}

export function getGraphNeighbors(db: Database, hash: string): GraphNeighbor[] {
  const outgoing = db.prepare(`SELECT gn.hash, gn.collection, gn.path, gn.title, ge.type, ge.context, 'outgoing' as direction FROM graph_edges ge JOIN graph_nodes gn ON ge.target_hash = gn.hash WHERE ge.source_hash = ?`).all(hash) as GraphNeighbor[];
  const incoming = db.prepare(`SELECT gn.hash, gn.collection, gn.path, gn.title, ge.type, ge.context, 'incoming' as direction FROM graph_edges ge JOIN graph_nodes gn ON ge.source_hash = gn.hash WHERE ge.target_hash = ?`).all(hash) as GraphNeighbor[];
  return [...outgoing, ...incoming];
}

export function extractLinks(content: string): ExtractedLink[] {
  const links: ExtractedLink[] = [];
  const mdRegex = /\[([^\]]+)\]\(([^)]+)\)/g;
  let match;
  while ((match = mdRegex.exec(content)) !== null) {
    const targetPath = match[2];
    if (targetPath.startsWith('http')) continue;
    if (targetPath.startsWith('#')) continue;
    const start = Math.max(0, match.index - 100);
    const end = Math.min(content.length, match.index + match[0].length + 100);
    links.push({ targetPath: targetPath.split('#')[0], type: "link", context: content.slice(start, end).replace(/\s+/g, ' ').slice(0, 200) });
  }
  const wikiRegex = /\[\[([^\]|]+)/g;
  while ((match = wikiRegex.exec(content)) !== null) {
    const start = Math.max(0, match.index - 100);
    const end = Math.min(content.length, match.index + match[0].length + 100);
    links.push({ targetPath: match[1].trim(), type: "wiki", context: content.slice(start, end).replace(/\s+/g, ' ').slice(0, 200) });
  }
  return links;
}

export function resolveLinkPath(sourcePath: string, targetPath: string): string | null {
  if (targetPath.startsWith('/')) return targetPath.slice(1);
  const sourceDir = sourcePath.split('/').slice(0, -1).join('/');
  return sourceDir ? `${sourceDir}/${targetPath}` : targetPath;
}

export function indexDocumentLinks(db: Database, sourceHash: string, content: string, sourcePath: string, collectionName: string): { added: number; errors: string[] } {
  const links = extractLinks(content);
  let added = 0;
  const errors: string[] = [];
  for (const link of links) {
    const resolvedPath = resolveLinkPath(sourcePath, link.targetPath);
    if (!resolvedPath) continue;
    const normalizedTarget = resolvedPath.replace(/\.md$/, '');
    const targetNode = db.prepare("SELECT hash FROM graph_nodes WHERE collection = ? AND (path = ? OR path = ? || '.md') LIMIT 1").get(collectionName, resolvedPath, normalizedTarget) as { hash: string } | null;
    if (targetNode) {
      upsertGraphEdge(db, sourceHash, targetNode.hash, link.type, link.context);
      added++;
    }
  }
  return { added, errors };
}

export function traverseGraph(db: Database, startHash: string, maxDepth: number = 2): TraversalResult[] {
  const results: TraversalResult[] = [];
  const visited = new Set<string>();
  const queue = [{ hash: startHash, distance: 0, path: [startHash] }];
  while (queue.length > 0) {
    const { hash, distance, path } = queue.shift()!;
    if (visited.has(hash)) continue;
    visited.add(hash);
    if (hash !== startHash) {
      const node = getGraphNodeByHash(db, hash);
      if (node) results.push({ node, distance, path: [...path] });
    }
    if (distance >= maxDepth) continue;
    const neighbors = getGraphNeighbors(db, hash);
    for (const n of neighbors) {
      if (!visited.has(n.hash)) queue.push({ hash: n.hash, distance: distance + 1, path: [...path, n.hash] });
    }
  }
  return results;
}

export function getNeighborhoodContext(db: Database, hashes: string[], maxDepth: number = 1, maxNodes: number = 20): Map<string, TraversalResult[]> {
  const context = new Map<string, TraversalResult[]>();
  for (const hash of hashes) {
    const neighbors = traverseGraph(db, hash, maxDepth).sort((a, b) => a.distance - b.distance).slice(0, maxNodes);
    context.set(hash, neighbors);
  }
  return context;
}

export function getGraphStats(db: Database): { nodeCount: number; edgeCount: number; avgDegree: number; orphanNodes: number } {
  const nodeCount = (db.prepare("SELECT COUNT(*) as c FROM graph_nodes").get() as { c: number }).c;
  const edgeCount = (db.prepare("SELECT COUNT(*) as c FROM graph_edges").get() as { c: number }).c;
  const degreeResult = db.prepare("SELECT AVG(cnt) as avg_degree FROM (SELECT COUNT(*) as cnt FROM graph_edges GROUP BY source_hash)").get() as { avg_degree: number } | null;
  const orphanResult = db.prepare("SELECT COUNT(*) as c FROM graph_nodes gn WHERE NOT EXISTS (SELECT 1 FROM graph_edges ge WHERE ge.source_hash = gn.hash OR ge.target_hash = gn.hash)").get() as { c: number };
  return { nodeCount, edgeCount, avgDegree: degreeResult?.avg_degree || 0, orphanNodes: orphanResult.c };
}

export interface GraphBoostConfig { enabled: boolean; neighborDepth: number; neighborBoost: number; maxNeighbors: number; }
export const DEFAULT_GRAPH_BOOST: GraphBoostConfig = { enabled: true, neighborDepth: 1, neighborBoost: 0.05, maxNeighbors: 10 };

export function applyGraphBoost(db: Database, results: Array<{ hash: string; score: number }>, config: GraphBoostConfig = DEFAULT_GRAPH_BOOST): Array<{ hash: string; score: number; graphBoost: number }> {
  if (!config.enabled || results.length === 0) return results.map(r => ({ ...r, graphBoost: 0 }));
  const resultHashes = new Set(results.map(r => r.hash));
  const neighborCounts = new Map<string, number>();
  for (const result of results) {
    const neighbors = traverseGraph(db, result.hash, config.neighborDepth).slice(0, config.maxNeighbors);
    let count = 0;
    for (const n of neighbors) if (resultHashes.has(n.node.hash)) count++;
    neighborCounts.set(result.hash, count);
  }
  return results.map(r => {
    const count = neighborCounts.get(r.hash) || 0;
    const boost = Math.min(count * config.neighborBoost, 0.5);
    return { hash: r.hash, score: r.score + boost, graphBoost: boost };
  });
}

export function cleanupOrphanGraphNodes(db: Database): number {
  return db.prepare("DELETE FROM graph_nodes WHERE hash NOT IN (SELECT DISTINCT hash FROM documents WHERE active = 1)").run().changes;
}

export function cleanupOrphanGraphEdges(db: Database): number {
  return db.prepare("DELETE FROM graph_edges WHERE source_hash NOT IN (SELECT DISTINCT hash FROM documents WHERE active = 1) OR target_hash NOT IN (SELECT DISTINCT hash FROM documents WHERE active = 1)").run().changes;
}

export function rebuildGraphFromDocuments(db: Database): { nodes: number; edges: number } {
  db.exec("DELETE FROM graph_edges");
  db.exec("DELETE FROM graph_nodes");
  const docs = db.prepare("SELECT d.hash, d.collection, d.path, d.title FROM documents d WHERE d.active = 1").all() as Array<{ hash: string; collection: string; path: string; title: string }>;
  for (const doc of docs) upsertGraphNode(db, doc.hash, doc.collection, doc.path, doc.title);
  let totalEdges = 0;
  for (const doc of docs) {
    const content = db.prepare("SELECT doc FROM content WHERE hash = ?").get(doc.hash) as { doc: string } | null;
    if (content) totalEdges += indexDocumentLinks(db, doc.hash, content.doc, doc.path, doc.collection).added;
  }
  return { nodes: docs.length, edges: totalEdges };
}
