/**
 * Graph Module Tests
 */

import { describe, it, expect, beforeEach, beforeAll } from "vitest";
import { openDatabase } from "../src/db.js";
import type { Database } from "../src/db.js";
import {
  initializeGraphSchema,
  upsertGraphNode,
  getGraphNodeByHash,
  getGraphNodeByPath,
  upsertGraphEdge,
  getOutgoingEdges,
  getIncomingEdges,
  getGraphNeighbors,
  extractLinks,
  resolveLinkPath,
  traverseGraph,
  getGraphStats,
  applyGraphBoost,
  rebuildGraphFromDocuments,
  type GraphNode,
} from "../src/graph.js";

describe("Graph Module", () => {
  let db: Database;

  beforeEach(() => {
    db = openDatabase(":memory:");
    // Minimal schema for testing
    db.exec(`
      CREATE TABLE IF NOT EXISTS content (
        hash TEXT PRIMARY KEY,
        doc TEXT NOT NULL,
        created_at TEXT NOT NULL
      )
    `);
    db.exec(`
      CREATE TABLE IF NOT EXISTS documents (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        collection TEXT NOT NULL,
        path TEXT NOT NULL,
        title TEXT NOT NULL,
        hash TEXT NOT NULL,
        created_at TEXT NOT NULL,
        modified_at TEXT NOT NULL,
        active INTEGER NOT NULL DEFAULT 1
      )
    `);
    initializeGraphSchema(db);
  });

  describe("Schema Initialization", () => {
    it("should create graph_nodes table", () => {
      const result = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='graph_nodes'"
      ).get();
      expect(result).toBeDefined();
    });

    it("should create graph_edges table", () => {
      const result = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='graph_edges'"
      ).get();
      expect(result).toBeDefined();
    });

    it("should create indexes", () => {
      const indexes = db.prepare(
        "SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_graph_%'"
      ).all() as { name: string }[];
      expect(indexes.length).toBeGreaterThanOrEqual(3);
    });
  });

  describe("Node Operations", () => {
    it("should create a graph node", () => {
      upsertGraphNode(db, "abc123", "test", "path/to/file.md", "Test File");
      const node = getGraphNodeByHash(db, "abc123");
      expect(node).toBeDefined();
      expect(node?.collection).toBe("test");
      expect(node?.path).toBe("path/to/file.md");
      expect(node?.title).toBe("Test File");
    });

    it("should find node by path", () => {
      upsertGraphNode(db, "abc123", "test", "path/to/file.md", "Test File");
      const node = getGraphNodeByPath(db, "test", "path/to/file.md");
      expect(node).toBeDefined();
      expect(node?.hash).toBe("abc123");
    });

    it("should update existing node", () => {
      upsertGraphNode(db, "abc123", "test", "path/to/file.md", "Old Title");
      upsertGraphNode(db, "abc123", "test", "path/to/file.md", "New Title");
      const node = getGraphNodeByHash(db, "abc123");
      expect(node?.title).toBe("New Title");
    });
  });

  describe("Edge Operations", () => {
    beforeEach(() => {
      upsertGraphNode(db, "hash1", "test", "file1.md", "File 1");
      upsertGraphNode(db, "hash2", "test", "file2.md", "File 2");
      upsertGraphNode(db, "hash3", "test", "file3.md", "File 3");
    });

    it("should create an edge", () => {
      upsertGraphEdge(db, "hash1", "hash2", "link");
      const edges = getOutgoingEdges(db, "hash1");
      expect(edges.length).toBe(1);
      expect(edges[0]?.target_hash).toBe("hash2");
    });

    it("should get incoming edges", () => {
      upsertGraphEdge(db, "hash1", "hash2", "link");
      const edges = getIncomingEdges(db, "hash2");
      expect(edges.length).toBe(1);
      expect(edges[0]?.source_hash).toBe("hash1");
    });

    it("should get all neighbors", () => {
      upsertGraphEdge(db, "hash1", "hash2", "link");
      upsertGraphEdge(db, "hash3", "hash1", "wiki");
      const neighbors = getGraphNeighbors(db, "hash1");
      expect(neighbors.length).toBe(2);
    });

    it("should skip self-loops", () => {
      upsertGraphEdge(db, "hash1", "hash1", "link");
      const edges = getOutgoingEdges(db, "hash1");
      expect(edges.length).toBe(0);
    });
  });

  describe("Link Extraction", () => {
    it("should extract markdown links", () => {
      const content = "See [file2](./file2.md) for details";
      const links = extractLinks(content, "file1.md");
      expect(links.length).toBe(1);
      expect(links[0]?.targetPath).toBe("./file2.md");
      expect(links[0]?.type).toBe("link");
    });

    it("should extract wiki links", () => {
      const content = "See [[file2]] for details";
      const links = extractLinks(content, "file1.md");
      expect(links.length).toBe(1);
      expect(links[0]?.targetPath).toBe("file2");
      expect(links[0]?.type).toBe("wiki");
    });

    it("should skip external URLs", () => {
      const content = "See [example](https://example.com)";
      const links = extractLinks(content, "file1.md");
      expect(links.length).toBe(0);
    });

    it("should skip anchor-only links", () => {
      const content = "See [section](#section)";
      const links = extractLinks(content, "file1.md");
      expect(links.length).toBe(0);
    });
  });

  describe("Link Resolution", () => {
    it("should resolve absolute paths", () => {
      const result = resolveLinkPath("folder/file1.md", "/docs/file2.md", "test");
      expect(result).toBe("docs/file2.md");
    });

    it("should resolve relative paths", () => {
      const result = resolveLinkPath("folder/file1.md", "file2.md", "test");
      expect(result).toBe("folder/file2.md");
    });

    it("should resolve parent directory paths", () => {
      const result = resolveLinkPath("folder/file1.md", "../file2.md", "test");
      expect(result).toBe("folder/../file2.md");
    });
  });

  describe("Graph Traversal", () => {
    beforeEach(() => {
      // Create a chain: hash1 -> hash2 -> hash3
      upsertGraphNode(db, "hash1", "test", "file1.md", "File 1");
      upsertGraphNode(db, "hash2", "test", "file2.md", "File 2");
      upsertGraphNode(db, "hash3", "test", "file3.md", "File 3");
      upsertGraphEdge(db, "hash1", "hash2", "link");
      upsertGraphEdge(db, "hash2", "hash3", "link");
    });

    it("should traverse to direct neighbors", () => {
      const results = traverseGraph(db, "hash1", 1);
      expect(results.length).toBe(1);
      expect(results[0]?.node?.hash).toBe("hash2");
      expect(results[0]?.distance).toBe(1);
    });

    it("should traverse to depth 2", () => {
      const results = traverseGraph(db, "hash1", 2);
      expect(results.length).toBe(2);
      expect(results[1]?.node?.hash).toBe("hash3");
    });

    it("should not visit same node twice", () => {
      upsertGraphEdge(db, "hash1", "hash3", "link");
      const results = traverseGraph(db, "hash1", 2);
      // hash3 should only appear once
      const hash3Results = results.filter(r => r.node.hash === "hash3");
      expect(hash3Results.length).toBe(1);
    });
  });

  describe("Graph Statistics", () => {
    beforeEach(() => {
      upsertGraphNode(db, "hash1", "test", "file1.md", "File 1");
      upsertGraphNode(db, "hash2", "test", "file2.md", "File 2");
      upsertGraphEdge(db, "hash1", "hash2", "link");
    });

    it("should count nodes", () => {
      const stats = getGraphStats(db);
      expect(stats.nodeCount).toBe(2);
    });

    it("should count edges", () => {
      const stats = getGraphStats(db);
      expect(stats.edgeCount).toBe(1);
    });

    it("should calculate average degree", () => {
      const stats = getGraphStats(db);
      expect(stats.avgDegree).toBe(1);
    });

    it("should count orphan nodes", () => {
      upsertGraphNode(db, "hash3", "test", "file3.md", "File 3");
      const stats = getGraphStats(db);
      expect(stats.orphanNodes).toBe(1);
    });
  });

  describe("Graph Boost", () => {
    beforeEach(() => {
      upsertGraphNode(db, "hash1", "test", "file1.md", "File 1");
      upsertGraphNode(db, "hash2", "test", "file2.md", "File 2");
      upsertGraphNode(db, "hash3", "test", "file3.md", "File 3");
      upsertGraphEdge(db, "hash1", "hash2", "link");
    });

    it("should boost results based on connectivity", () => {
      const results = [
        { hash: "hash1", score: 0.5 },
        { hash: "hash2", score: 0.5 },
        { hash: "hash3", score: 0.5 },
      ];
      const boosted = applyGraphBoost(db, results);
      // hash1 should have boost for being connected to hash2
      expect(boosted[0]?.graphBoost).toBeGreaterThan(0);
    });

    it("should cap boost at 0.5", () => {
      const results = [
        { hash: "hash1", score: 0.5 },
        { hash: "hash2", score: 0.5 },
      ];
      const boosted = applyGraphBoost(db, results, { enabled: true, neighborDepth: 1, neighborBoost: 1.0, maxNeighbors: 10 });
      expect(boosted[0]?.graphBoost).toBeLessThanOrEqual(0.5);
    });
  });
});
