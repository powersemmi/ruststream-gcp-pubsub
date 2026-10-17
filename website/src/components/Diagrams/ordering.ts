import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The two keys of the drawing, each a column of its own from the publisher to the subscription.
const KEYS = [
  {key: 'keyA', delivered: 'deliveredA', x: 12},
  {key: 'keyB', delivered: 'deliveredB', x: 188},
] as const;
const COLUMN = 160;
// The publisher and the topic span both columns, so a link meets them above its own column.
const WIDE = {x: 60, w: 240};

/*
 * Ordering keys: the publisher keeps one batch in flight per key, the topic takes the messages
 * of one region, and a subscription with message ordering delivers each key in publish order.
 */
export function ordering(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('publisher', WIDE.x, 0, WIDE.w, 44, {kind: 'service'}),
    ...KEYS.map(({key, x}) => box(key, x, 76, COLUMN, 44)),
    box('topic', WIDE.x, 152, WIDE.w, 44, {kind: 'broker'}),
    box('subscription', 0, 228, 360, 118, {kind: 'group'}),
    ...KEYS.map(({delivered, x}) => box(delivered, x, 254, COLUMN, 80, {kind: 'core'})),
    box('redelivery', 0, 362, 360, 44, {kind: 'note'}),
  ];
  const over = (x: number): number => (x + COLUMN / 2 - WIDE.x) / WIDE.w;
  const links: Link[] = [
    ...KEYS.map(({key, x}) => ({from: 'publisher', to: key, exitAt: over(x)})),
    ...KEYS.map(({key, x}) => ({from: key, to: 'topic', enterAt: over(x)})),
    {from: 'topic', to: 'subscription', label: word(words, 'inOrder')},
  ];
  return {boxes, links};
}
