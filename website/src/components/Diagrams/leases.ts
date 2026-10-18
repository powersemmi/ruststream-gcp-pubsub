import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

// The main column, the column of the batched answers, and the line of the extensions between them.
const MAIN = {x: 12, w: 140};
const SIDE = {x: 250, w: 98};
const EXTEND_X = 200;

/*
 * A streaming pull and its leases: the service sends messages while fewer than max_outstanding
 * are unsettled, the client keeps extending the deadline of every message the application holds,
 * and the application's acks and nacks go back in batches.
 */
export function leases(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  const boxes: Box[] = [
    box('subscription', 12, 0, 336, 44, {kind: 'broker'}),
    box('client', 0, 80, 360, 152, {kind: 'group'}),
    box('pull', MAIN.x, 106, MAIN.w, 52, {kind: 'broker'}),
    box('lease', MAIN.x, 168, MAIN.w, 52, {kind: 'broker'}),
    box('flush', SIDE.x, 106, SIDE.w, 114, {kind: 'broker'}),
    box('app', 0, 256, 360, 140, {kind: 'group'}),
    box('handler', MAIN.x, 282, MAIN.w, 44, {kind: 'service'}),
    box('settle', MAIN.x, 342, MAIN.w, 44, {kind: 'service'}),
    box('expired', 0, 412, 360, 44, {kind: 'note'}),
  ];
  const along = (x: number): number => (x - 12) / 336;
  const links: Link[] = [
    {
      from: 'subscription',
      to: 'pull',
      exitAt: along(MAIN.x + MAIN.w / 2),
      label: word(words, 'messages'),
    },
    {from: 'pull', to: 'lease'},
    {from: 'lease', to: 'handler'},
    {from: 'handler', to: 'settle'},
    {
      from: 'lease',
      to: 'subscription',
      exit: 'right',
      enter: 'bottom',
      enterAt: along(EXTEND_X),
      label: word(words, 'extends'),
    },
    {from: 'settle', to: 'flush', exit: 'right', enter: 'bottom', label: word(words, 'settled')},
    {
      from: 'flush',
      to: 'subscription',
      enterAt: along(SIDE.x + SIDE.w / 2),
      label: word(words, 'answers'),
    },
  ];
  return {boxes, links};
}
