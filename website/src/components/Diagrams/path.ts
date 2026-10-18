import type {Box, Link} from './flow';
import {chain} from './chain';
import type {Words} from './words';

/** The path of one Pub/Sub message that is answered, from the streaming pull to its ack. */
export function path(words: Words): {boxes: Box[]; links: Link[]} {
  return chain(words, [
    {key: 'pull', kind: 'broker', href: '/architecture/pubsub/leases/'},
    {key: 'pump', kind: 'broker'},
    {key: 'message', kind: 'broker'},
    {key: 'dispatch', kind: 'core'},
    {key: 'decode'},
    {key: 'body', kind: 'service'},
    {key: 'encode'},
    {key: 'publisher', kind: 'broker', href: '/architecture/pubsub/ordering/'},
    {key: 'settle', kind: 'core', href: '/architecture/settlement/'},
    {key: 'ack', kind: 'broker', href: '/architecture/pubsub/leases/'},
  ]);
}
