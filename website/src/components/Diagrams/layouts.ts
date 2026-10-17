import {design} from './design';
import type {Box, Lane, Link} from './flow';
import {leases} from './leases';
import {lifecycle} from './lifecycle';
import {ordering} from './ordering';
import {path} from './path';
import {redelivery} from './redelivery';
import {settlement} from './settlement';
import {topics} from './topics';
import type {Words} from './words';

export interface Layout {
  boxes: Box[];
  links: Link[];
  lanes?: Lane[];
}

/** Every diagram a page can name: a layout places the boxes, the page supplies the words. */
export const LAYOUTS = {
  topics,
  leases,
  ordering,
  redelivery,
  design,
  lifecycle,
  path,
  settlement,
} satisfies Record<string, (words: Words) => Layout>;

export type DiagramName = keyof typeof LAYOUTS;
