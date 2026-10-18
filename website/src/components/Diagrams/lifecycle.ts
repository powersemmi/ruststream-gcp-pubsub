import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * The broker's three states as types on the left, and on the right what each transition does on
 * Pub/Sub: the clients built at connect, each subscription's topology and stream at startup, a
 * topic's publisher on its first publish, and the flush at shutdown.
 */
export function lifecycle(words: Words): {boxes: Box[]; links: Link[]} {
  const left = (id: string, y: number): Box => ({
    id,
    x: 12,
    y,
    w: 156,
    h: 58,
    kind: 'broker',
    code: true,
    lines: lines(words, id),
  });
  const right = (id: string, y: number, {h = 58, href}: {h?: number; href?: string} = {}): Box => ({
    id,
    x: 192,
    y,
    w: 156,
    h,
    kind: 'core',
    lines: lines(words, id),
    href,
  });
  const boxes: Box[] = [
    {id: 'states', x: 0, y: 0, w: 176, h: 424, kind: 'group', lines: lines(words, 'states')},
    {id: 'pubsub', x: 184, y: 0, w: 176, h: 424, kind: 'group', lines: lines(words, 'pubsub')},
    left('unconnected', 28),
    left('connected', 168),
    left('closed', 354),
    right('config', 28),
    right('clients', 98),
    right('subscriptions', 168, {h: 88, href: '/architecture/pubsub/'}),
    right('publisher', 278),
    right('flush', 354),
    {
      id: 'afterShutdown',
      x: 0,
      y: 444,
      w: 360,
      h: 44,
      kind: 'note',
      lines: lines(words, 'afterShutdown'),
    },
  ];
  const links: Link[] = [
    {from: 'unconnected', to: 'connected', label: word(words, 'connect')},
    {from: 'connected', to: 'closed', label: word(words, 'shutdown')},
    {from: 'config', to: 'clients'},
    {from: 'clients', to: 'subscriptions'},
    {from: 'subscriptions', to: 'publisher', dashed: true},
    {from: 'publisher', to: 'flush'},
  ];
  return {boxes, links};
}
