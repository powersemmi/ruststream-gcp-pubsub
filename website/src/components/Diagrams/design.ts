import type {Box, Link} from './flow';
import {lines, word, type Words} from './words';

/*
 * Where the crate sits: the service on top, the core's runtime under it, this crate between the
 * runtime and the google-cloud-pubsub clients, and Pub/Sub at the bottom. Each subscription pumps
 * one streaming pull; the publishers of a connection share one client publisher per topic.
 */
export function design(words: Words): {boxes: Box[]; links: Link[]} {
  const box = (
    id: string,
    x: number,
    y: number,
    w: number,
    h: number,
    fields: Partial<Box> = {},
  ): Box => ({id, x, y, w, h, kind: 'step', lines: lines(words, id), ...fields});
  // The crate's columns leave a gutter at each side, where a link from a box to the client under
  // it runs past the boxes in between.
  const left = 28;
  const right = 182;
  const column = 150;
  const centre = (x: number): number => (x + column / 2 - 12) / 336;
  const boxes: Box[] = [
    box('service', 0, 0, 360, 80, {kind: 'group'}),
    box('handlers', 12, 26, 160, 42, {kind: 'service', code: true}),
    box('app', 188, 26, 160, 42, {kind: 'service', code: true}),
    box('core', 0, 104, 360, 72, {kind: 'group'}),
    box('runtime', 12, 128, 336, 36, {kind: 'core'}),
    box('crate', 0, 200, 360, 204, {kind: 'group'}),
    box('subscriber', left, 226, column, 58, {
      kind: 'broker',
      code: true,
      href: '/architecture/path/',
    }),
    box('publishers', right, 226, column, 58, {kind: 'broker', code: true}),
    box('message', left, 294, column, 44, {
      kind: 'broker',
      code: true,
      href: '/architecture/settlement/',
    }),
    box('broker', right, 294, column, 44, {
      kind: 'broker',
      code: true,
      href: '/architecture/lifecycle/',
    }),
    box('descriptor', left, 348, column, 44, {kind: 'broker', code: true}),
    box('inProcess', right, 348, column, 44, {kind: 'broker'}),
    box('client', 0, 428, 360, 84, {kind: 'group'}),
    box('pull', left, 454, column, 46, {
      kind: 'broker',
      code: true,
      href: '/architecture/pubsub/leases/',
    }),
    box('clients', right, 454, column, 46, {kind: 'broker', code: true}),
    box('pubsub', 12, 536, 336, 36, {kind: 'broker'}),
  ];
  const links: Link[] = [
    {from: 'handlers', to: 'runtime', enterAt: 80 / 336, label: word(words, 'mounted')},
    {from: 'runtime', to: 'subscriber', exitAt: centre(left), label: word(words, 'polls')},
    {from: 'subscriber', to: 'pull', exit: 'left', enter: 'left'},
    {from: 'publishers', to: 'clients', exit: 'right', enter: 'right'},
    {from: 'pull', to: 'pubsub', enterAt: centre(left)},
    {from: 'clients', to: 'pubsub', enterAt: centre(right)},
  ];
  return {boxes, links};
}
