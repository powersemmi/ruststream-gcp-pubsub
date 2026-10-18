import {expect, it} from 'vitest';
import {OUTCOMES, settlement} from './settlement';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('answers every handler outcome with one call on Pub/Sub, on the same row', () => {
  const {boxes, links} = settlement(words);
  const byId = new Map(boxes.map((box) => [box.id, box]));
  expect(links.map((link) => [link.from, link.to])).toEqual(
    OUTCOMES.map(({outcome, answer}) => [outcome, answer]),
  );
  for (const {outcome, answer} of OUTCOMES) {
    // A straight line across: the outcome and its answer share their top and their height.
    expect(byId.get(answer)!.y).toBe(byId.get(outcome)!.y);
    expect(byId.get(answer)!.h).toBe(byId.get(outcome)!.h);
  }
});

it('heads each column with who acts in it', () => {
  const {boxes, lanes} = settlement(words);
  for (const lane of lanes) {
    expect(boxes.filter((box) => box.x + box.w / 2 === lane.x)).toHaveLength(OUTCOMES.length);
  }
});
