import "../index.css";

import { describe, expect, it } from "vite-plus/test";
import type { SortingStrategy } from "@dnd-kit/sortable";
import { createSidebarSortingStrategy, measureSidebarPinnedGrid } from "./Sidebar.drag";
import { sidebarListItemId, type SidebarListItem } from "./Sidebar.logic";

function mountPinnedGrid(width: number, count: number) {
  const list = document.createElement("ul");
  list.style.cssText = `display:flex;flex-direction:column;gap:1px;width:${width}px;position:relative`;
  const header = document.createElement("li");
  header.style.cssText = "height:0;margin-inline:0.125rem";
  const grid = document.createElement("ul");
  grid.className = "sidebar-pinned-grid";
  const tiles = Array.from({ length: count }, () => grid.appendChild(document.createElement("li")));
  list.append(header, grid);
  document.body.append(list);
  return { header, grid, tiles, [Symbol.dispose]: () => list.remove() };
}

describe("pinned grid layout contract", () => {
  it.each([
    [152, 1],
    [200, 2],
    [244, 3],
  ])("projects a still drag onto the CSS grid at %ipx (%i columns)", (width, columns) => {
    for (let count = 1; count <= 5; count++) {
      using mounted = mountPinnedGrid(width, count);
      const pinnedGrid = measureSidebarPinnedGrid(mounted.grid, 1);
      expect(pinnedGrid?.columns).toBe(columns);
      const items: SidebarListItem[] = [
        { kind: "marker", marker: "pinned-header" },
        ...mounted.tiles.map((_, index) => ({
          kind: "thread" as const,
          key: `p${index}`,
          section: "pinned" as const,
        })),
        { kind: "marker", marker: "pinned-divider" },
      ];
      const rects = [mounted.header, ...mounted.tiles, mounted.header].map((node) =>
        node.getBoundingClientRect(),
      );
      const strategy = createSidebarSortingStrategy({
        items,
        settledOrder: [],
        settledExpanded: true,
        pinnedGrid,
      });
      const activeIndex = count;
      const args = {
        activeIndex,
        overIndex: activeIndex,
        activeNodeRect: rects[activeIndex]!,
        rects,
        index: 0,
      } satisfies Parameters<SortingStrategy>[0];
      for (const [index, item] of items.entries()) {
        if (item.kind !== "thread") continue;
        const transform = strategy({ ...args, index });
        const tile = `${sidebarListItemId(item)} of ${count}`;
        expect(transform?.x, tile).toBeCloseTo(0, 1);
        expect(transform?.y, tile).toBeCloseTo(0, 1);
        expect(transform?.scaleX, tile).toBeCloseTo(1, 3);
      }
    }
  });
});
