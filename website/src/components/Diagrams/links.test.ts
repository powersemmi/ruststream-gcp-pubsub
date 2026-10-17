import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {describe, expect, it} from 'vitest';
import {LAYOUTS, type DiagramName} from './layouts';
import type {Words} from './words';

// Every word is its own key, so the test reads the layout and never a page's wording.
const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

// The docs are served from the site root, so a route names the page's file under `docs/`.
const DOCS = join(__dirname, '..', '..', '..', '..', 'docs');

const hrefs = (name: DiagramName): Record<string, string> =>
  Object.fromEntries(
    LAYOUTS[name](words)
      .boxes.filter((box) => box.href !== undefined)
      .map((box) => [box.id, box.href!]),
  );

const page = (href: string): boolean => {
  const route = href.replace(/^\/|\/$/g, '');
  return ['.md', '.mdx', '/index.md', '/index.mdx'].some((ending) =>
    existsSync(join(DOCS, route + ending)),
  );
};

describe('a box that links to a page', () => {
  const every = (Object.keys(LAYOUTS) as DiagramName[]).flatMap((name) =>
    Object.entries(hrefs(name)).map(([box, href]) => ({name, box, href})),
  );

  it.each(every)('$name, $box: $href is a page of the site', ({href}) => {
    // A root-relative route: the link prepends the language's base URL, so one route serves
    // every translation, and the site ends every route with a slash.
    expect(href).toMatch(/^\/[a-z0-9/-]+\/$/);
    expect(page(href), `no page under docs/ for ${href}`).toBe(true);
  });
});

describe('a box that names a step explained elsewhere', () => {
  it('links to that page on the path of a message', () => {
    expect(hrefs('path')).toEqual({
      pull: '/architecture/pubsub/leases/',
      publisher: '/architecture/pubsub/ordering/',
      settle: '/architecture/settlement/',
      ack: '/architecture/pubsub/leases/',
    });
  });

  it('links to that page in the design of the crate', () => {
    expect(hrefs('design')).toEqual({
      subscriber: '/architecture/path/',
      message: '/architecture/settlement/',
      broker: '/architecture/lifecycle/',
      pull: '/architecture/pubsub/leases/',
    });
  });

  it('links to that page in the lifecycle and the settlement', () => {
    expect(hrefs('lifecycle')).toEqual({subscriptions: '/architecture/pubsub/'});
    expect(hrefs('settlement')).toEqual({
      nacked: '/architecture/pubsub/redelivery/',
      held: '/architecture/pubsub/leases/',
      deadLettered: '/architecture/pubsub/redelivery/',
    });
  });
});

describe('a diagram drawn on the phone-first drawing area', () => {
  it.each(Object.keys(LAYOUTS) as DiagramName[])('%s keeps every box inside 360 units', (name) => {
    for (const box of LAYOUTS[name](words).boxes) {
      expect(box.x, `${name}, ${box.id}`).toBeGreaterThanOrEqual(0);
      expect(box.x + box.w, `${name}, ${box.id}`).toBeLessThanOrEqual(360);
    }
  });

  it.each(Object.keys(LAYOUTS) as DiagramName[])('%s links only boxes it draws', (name) => {
    const {boxes, links} = LAYOUTS[name](words);
    const ids = new Set(boxes.map((box) => box.id));
    for (const link of links) {
      expect(ids.has(link.from) && ids.has(link.to), `${link.from} -> ${link.to}`).toBe(true);
    }
  });
});
