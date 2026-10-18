import {expect, it} from 'vitest';
import {redelivery} from './redelivery';
import type {Words} from './words';

const words = new Proxy({}, {get: (_, key) => String(key)}) as Words;

it('brings a nacked message back unless the dead-letter cap sends it on', () => {
  const {links} = redelivery(words);
  const from = (id: string): string[] =>
    links.filter((link) => link.from === id).map((link) => link.to);
  expect(from('delivery')).toEqual(['ack', 'nack']);
  // An ack ends the message; a nack meets the policy, which redelivers or dead-letters.
  expect(from('ack')).toEqual([]);
  expect(from('nack')).toEqual(['policy']);
  expect(from('policy')).toEqual(['redeliver', 'deadLetter']);
  expect(from('redeliver')).toEqual(['delivery']);
  expect(from('deadLetter')).toEqual([]);
});
