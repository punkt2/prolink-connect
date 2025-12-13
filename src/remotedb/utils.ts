import {Span} from '@sentry/tracing';

import {Items, ItemType} from './message/item';
import {MessageType} from './message/types';
import {UInt32} from './fields';
import {Message} from './message';
import {Connection, LookupDescriptor} from '.';

/**
 * Specifies the number of items we should request at a time in menu render
 * requests.
 */
const LIMIT = 64;

export const fieldFromDescriptor = ({
  hostDevice,
  menuTarget,
  trackSlot,
  trackType,
}: LookupDescriptor) =>
  new UInt32(Buffer.of(hostDevice.id, menuTarget, trackSlot, trackType));

export const makeRenderMessage = (
  descriptor: LookupDescriptor,
  offset: number,
  count: number,
  total: number
) =>
  new Message({
    type: MessageType.RenderMenu,
    args: [
      fieldFromDescriptor(descriptor),
      new UInt32(offset),
      new UInt32(count),
      new UInt32(0),
      new UInt32(total),
      new UInt32(0x0c),
    ],
  });

/**
 * Async generator to page through menu results after a successful lookup
 * request.
 */
export async function* renderItems<T extends ItemType = ItemType>(
  conn: Connection,
  descriptor: LookupDescriptor,
  total: number,
  span: Span
) {
  console.log(`[METADATA_DEBUG] renderItems START - total=${total}, LIMIT=${LIMIT}`);
  let itemsRead = 0;

  while (itemsRead < total) {
    // Request another page of items
    if (itemsRead % LIMIT === 0) {
      // XXX: itemsRead + count should NOT exceed the total. A larger value
      // will push the offset back to accommodate for the extra items, ensuring
      // we always receive count items.
      const count = Math.min(LIMIT, total - itemsRead);
      const message = makeRenderMessage(descriptor, itemsRead, count, total);

      console.log(`[METADATA_DEBUG] renderItems - requesting page: offset=${itemsRead}, count=${count}, total=${total}`);
      await conn.writeMessage(message, span);
      console.log(`[METADATA_DEBUG] renderItems - reading MenuHeader...`);
      await conn.readMessage(MessageType.MenuHeader, span);
      console.log(`[METADATA_DEBUG] renderItems - MenuHeader received`);
    }

    // Read each item. Ignoring headers and footers, we will determine when to
    // stop by counting the items read until we reach the total items.
    console.log(`[METADATA_DEBUG] renderItems - reading MenuItem ${itemsRead + 1}/${total}...`);
    const resp = await conn.readMessage(MessageType.MenuItem, span);
    console.log(`[METADATA_DEBUG] renderItems - MenuItem received`);

    yield resp.data as Items[T];
    itemsRead++;

    // When we've reached the end of a page we must read the footer
    if (itemsRead % LIMIT === 0 || itemsRead === total) {
      console.log(`[METADATA_DEBUG] renderItems - reading MenuFooter (page complete or end of items)...`);
      await conn.readMessage(MessageType.MenuFooter, span);
      console.log(`[METADATA_DEBUG] renderItems - MenuFooter received`);
    }
  }
  console.log(`[METADATA_DEBUG] renderItems END - total items read: ${itemsRead}`);
}

const colors = [
  ItemType.ColorNone,
  ItemType.ColorPink,
  ItemType.ColorRed,
  ItemType.ColorOrange,
  ItemType.ColorYellow,
  ItemType.ColorGreen,
  ItemType.ColorAqua,
  ItemType.ColorBlue,
  ItemType.ColorPurple,
] as const;

const colorSet = new Set(colors);

type ColorType = (typeof colors)[number];

/**
 * Locate the color item in an item list
 */
export const findColor = (items: Array<Items[ItemType]>) =>
  items.filter(item => colorSet.has(item.type as any)).pop() as Items[ColorType];
