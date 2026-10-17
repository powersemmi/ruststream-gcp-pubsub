import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

const LEFT = 12;
const RIGHT = 184;
const COLUMN = 140;
// The delivery stops short of the right edge, so the line that brings a message back runs inside
// the drawing.
const DELIVERY = {x: 12, w: 312};

/*
 * What Pub/Sub does with one delivery: an ack removes the message; a nack or an expired deadline
 * brings it back after the retry policy's wait, until a dead-letter policy's cap sends it to the
 * dead-letter topic instead.
 */
export function redelivery(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w: COLUMN, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    {
      id: 'delivery',
      ...DELIVERY,
      y: 0,
      h: 44,
      kind: 'broker',
      lines: lines(words, 'delivery'),
    },
    box('ack', LEFT, 80, 58, {kind: 'good'}),
    box('nack', RIGHT, 80, 58),
    box('policy', RIGHT, 170, 58, {kind: 'broker'}),
    box('deadLetter', LEFT, 262, 74, {kind: 'bad'}),
    box('redeliver', RIGHT, 262, 74),
    {id: 'approximate', x: 0, y: 356, w: 360, h: 44, kind: 'note', lines: lines(words, 'approximate')},
  ];
  const above = (x: number): number => (x + COLUMN / 2 - DELIVERY.x) / DELIVERY.w;
  const links: Link[] = [
    {from: 'delivery', to: 'ack', exitAt: above(LEFT)},
    {from: 'delivery', to: 'nack', exitAt: above(RIGHT)},
    {from: 'nack', to: 'policy'},
    {from: 'policy', to: 'redeliver', label: word(words, 'below')},
    {from: 'policy', to: 'deadLetter', exit: 'left', enter: 'top', label: word(words, 'atCap')},
    {from: 'redeliver', to: 'delivery', exit: 'right', enter: 'right', dashed: true},
  ];
  return {boxes, links};
}
