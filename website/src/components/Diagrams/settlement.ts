import type {Box, Lane, Link} from './flow';
import {lines, word, type Words} from './words';

const OUTCOME = {x: 12, w: 124};
const ANSWER = {x: 160, w: 188};
const GAP = 14;

/** Each handler outcome in the order the drawing lists them, with the lines its answer takes. */
export const OUTCOMES: {outcome: string; answer: string; rows: number; href?: string}[] = [
  {outcome: 'ack', answer: 'acked', rows: 2},
  {outcome: 'retry', answer: 'nacked', rows: 3, href: '/architecture/pubsub/redelivery/'},
  {outcome: 'retryAfter', answer: 'held', rows: 4, href: '/architecture/pubsub/leases/'},
  {outcome: 'drop', answer: 'dropped', rows: 2},
  {outcome: 'dropAtCap', answer: 'deadLettered', rows: 3, href: '/architecture/pubsub/redelivery/'},
];

/*
 * What each handler outcome becomes on Pub/Sub: the outcome on the left, the call the crate makes
 * on the right, one row each.
 */
export function settlement(words: Words): {boxes: Box[]; links: Link[]; lanes: Lane[]} {
  const boxes: Box[] = [];
  const links: Link[] = [];
  let y = 28;
  for (const {outcome, answer, rows, href} of OUTCOMES) {
    const h = 14 + 15 * rows;
    boxes.push(
      {id: outcome, ...OUTCOME, y, h, kind: 'service', code: true, lines: lines(words, outcome)},
      {id: answer, ...ANSWER, y, h, kind: 'broker', lines: lines(words, answer), href},
    );
    links.push({from: outcome, to: answer});
    y += h + GAP;
  }
  boxes.push({
    id: 'exactlyOnce',
    x: 0,
    y,
    w: 360,
    h: 44,
    kind: 'note',
    lines: lines(words, 'exactlyOnce'),
  });
  const lanes: Lane[] = [
    {x: OUTCOME.x + OUTCOME.w / 2, label: word(words, 'handler')},
    {x: ANSWER.x + ANSWER.w / 2, label: word(words, 'onPubSub')},
  ];
  return {boxes, links, lanes};
}
