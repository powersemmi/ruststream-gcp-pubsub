import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Topics and subscriptions: a publisher publishes to a topic, every subscription attached to the
 * topic receives each message, and the subscribers of one subscription share its messages.
 */
export function topics(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('publisher', 90, 0, 180, 44, {kind: 'service'}),
    box('topic', 90, 76, 180, 44, {kind: 'broker'}),
    box('billing', 0, 160, 172, 128, {kind: 'group'}),
    box('billing1', 12, 186, 148, 40, {kind: 'core'}),
    box('billing2', 12, 236, 148, 40, {kind: 'core'}),
    box('audit', 188, 160, 172, 128, {kind: 'group'}),
    box('audit1', 200, 186, 148, 90, {kind: 'core'}),
    box('retained', 0, 304, 360, 44, {kind: 'note'}),
  ];
  const links: Link[] = [
    {from: 'publisher', to: 'topic', label: word(words, 'publishes')},
    {from: 'topic', to: 'billing', exitAt: 0.2, label: word(words, 'copy')},
    {from: 'topic', to: 'audit', exitAt: 0.8, label: word(words, 'copy')},
  ];
  return {boxes, links};
}
