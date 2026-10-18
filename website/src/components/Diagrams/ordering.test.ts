import {expect, it} from 'vitest';
import {route} from './flow';
import {ordering} from './ordering';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('runs each key straight down its own column from the publisher to the topic', () => {
  const {boxes, links} = ordering(words);
  const byId = new Map(boxes.map((box) => [box.id, box]));
  for (const link of links.filter((link) => link.from === 'publisher' || link.to === 'topic')) {
    // A path with no bend: one vertical stretch, so the two keys never cross.
    expect(route(byId.get(link.from)!, byId.get(link.to)!, link).d).toMatch(/^M \S+ \S+ V \S+$/);
  }
});
