import * as Sentry from '@sentry/node';
import {Span} from '@sentry/tracing';
import {Mutex} from 'async-mutex';
import * as ip from 'ip-address';
import PromiseSocket from 'promise-socket';

import {Socket} from 'net';

import DeviceManager from 'src/devices';
import {Device, DeviceID, MediaSlot, TrackType} from 'src/types';

import {getMessageName, MessageType, Request, Response} from './message/types';
import {REMOTEDB_SERVER_QUERY_PORT} from './constants';
import {readField, UInt32} from './fields';
import {Message} from './message';
import {HandlerArgs, HandlerReturn, queryHandlers} from './queries';

type Await<T> = T extends PromiseLike<infer U> ? U : T;

/**
 * Menu target specifies where a menu should be "rendered" This differs based
 * on the request being made.
 */
export enum MenuTarget {
  Main = 0x01,
}

/**
 * Used to specify where to lookup data when making queries
 */
export interface QueryDescriptor {
  menuTarget: MenuTarget;
  trackSlot: MediaSlot;
  trackType: TrackType;
}

/**
 * Used internally when making queries.
 */
export type LookupDescriptor = QueryDescriptor & {
  targetDevice: Device;
  hostDevice: Device;
};

/**
 * Used to specify the query type that is being made
 */
export type Query = keyof typeof queryHandlers;
export const Query = Request;

const QueryInverse = Object.fromEntries(Object.entries(Query).map(e => [e[1], e[0]]));

/**
 * Returns a string representation of a remote query
 */
export function getQueryName(query: Query) {
  return QueryInverse[query];
}

/**
 * Options used to make a remotedb query
 */
interface QueryOpts<T extends Query> {
  queryDescriptor: QueryDescriptor;
  /**
   * The query type to make
   */
  query: T;
  /**
   * Arguments to pass to the query. These are query specific
   */
  args: HandlerArgs<T>;
  /**
   * The sentry span to associate the query with
   */
  span?: Span;
}

/**
 * Queries the remote device for the port that the remote database server is
 * listening on for requests.
 */
async function getRemoteDBServerPort(deviceIp: ip.Address4) {
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort START - ip=${deviceIp.address}`);
  const conn = new PromiseSocket(new Socket());
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort - connecting to port ${REMOTEDB_SERVER_QUERY_PORT}...`);
  await conn.connect(REMOTEDB_SERVER_QUERY_PORT, deviceIp.address);
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort - connected`);

  // Magic request packet asking the device to report it's remoteDB port
  const data = Buffer.from([
    ...[0x00, 0x00, 0x00, 0x0f],
    ...Buffer.from('RemoteDBServer', 'ascii'),
    0x00,
  ]);

  console.log(`[METADATA_DEBUG] getRemoteDBServerPort - writing query...`);
  await conn.write(data);
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort - reading response...`);
  const resp = await conn.read();
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort - response received`);

  if (typeof resp !== 'object') {
    throw new Error('Invalid response from remotedb');
  }

  if (resp.length !== 2) {
    throw new Error(`Expected 2 bytes, got ${resp.length}`);
  }

  const port = resp.readUInt16BE();
  console.log(`[METADATA_DEBUG] getRemoteDBServerPort END - port=${port}`);
  return port;
}

/**
 * Manages a connection to a single device
 */
export class Connection {
  #socket: PromiseSocket<Socket>;
  #txId = 0;
  #lock = new Mutex();

  device: Device;

  constructor(device: Device, socket: PromiseSocket<Socket>) {
    this.#socket = socket;
    this.device = device;
  }

  async writeMessage(message: Message, span: Span) {
    const tx = span.startChild({
      op: 'writeMessage',
      description: getMessageName(message.type),
    });

    message.transactionId = ++this.#txId;
    console.log(`[METADATA_DEBUG] Connection.writeMessage - type=${getMessageName(message.type)}, txId=${message.transactionId}`);
    await this.#socket.write(message.buffer);
    console.log(`[METADATA_DEBUG] Connection.writeMessage - write complete`);
    tx.finish();
  }

  readMessage<T extends Response>(expect: T, span: Span) {
    console.log(`[METADATA_DEBUG] Connection.readMessage - expecting=${getMessageName(expect)}, acquiring lock...`);
    return this.#lock.runExclusive(async () => {
      console.log(`[METADATA_DEBUG] Connection.readMessage - lock acquired, calling fromStream...`);
      const result = await Message.fromStream(this.#socket, expect, span);
      console.log(`[METADATA_DEBUG] Connection.readMessage - fromStream complete, got type=${getMessageName(result.type)}`);
      return result;
    });
  }

  close() {
    this.#socket.destroy();
  }
}

export class QueryInterface {
  #conn: Connection;
  #hostDevice: Device;
  #lock: Mutex;

  constructor(conn: Connection, lock: Mutex, hostDevice: Device) {
    this.#conn = conn;
    this.#lock = lock;
    this.#hostDevice = hostDevice;
  }

  /**
   * Make a query to the remote database connection.
   */
  async query<T extends Query>(opts: QueryOpts<T>): Promise<Await<HandlerReturn<T>>> {
    const {query, queryDescriptor, args, span} = opts;
    const conn = this.#conn;

    const queryName = getQueryName(opts.query);
    console.log(`[METADATA_DEBUG] QueryInterface.query START - queryName=${queryName}`);

    const tx = span
      ? span.startChild({op: 'remoteQuery', description: queryName})
      : Sentry.startTransaction({name: 'remoteQuery', description: queryName});

    const lookupDescriptor: LookupDescriptor = {
      ...queryDescriptor,
      hostDevice: this.#hostDevice,
      targetDevice: this.#conn.device,
    };

    // TODO: Figure out why typescirpt can't understand our query type discriminate
    // for args here. The interface for this actual query function discrimites just
    // fine.
    const anyArgs = args as any;

    const handler = queryHandlers[query];

    console.log(`[METADATA_DEBUG] QueryInterface.query - acquiring lock...`);
    const releaseLock = await this.#lock.acquire();
    console.log(`[METADATA_DEBUG] QueryInterface.query - lock acquired, executing handler...`);
    try {
      const response = await handler({conn, lookupDescriptor, span: tx, args: anyArgs});
      console.log(`[METADATA_DEBUG] QueryInterface.query END - handler complete`);
      return response as Await<HandlerReturn<T>>;
    } finally {
      console.log(`[METADATA_DEBUG] QueryInterface.query - releasing lock...`);
      releaseLock();
      tx.finish();
    }
  }
}

/**
 * Service that maintains remote database connections with devices on the network.
 */
export default class RemoteDatabase {
  #hostDevice: Device;
  #deviceManager: DeviceManager;

  /**
   * Active device connection map
   */
  #connections = new Map<DeviceID, Connection>();
  /**
   * Locks for each device when locating the connection
   */
  #deviceLocks = new Map<DeviceID, Mutex>();

  constructor(deviceManager: DeviceManager, hostDevice: Device) {
    this.#deviceManager = deviceManager;
    this.#hostDevice = hostDevice;
  }

  /**
   * Open a connection to the specified device for querying
   */
  connectToDevice = async (device: Device) => {
    console.log(`[METADATA_DEBUG] connectToDevice START - deviceId=${device.id}, ip=${device.ip?.address}`);
    const tx = Sentry.startTransaction({name: 'connectRemotedb', data: {device}});

    const {ip} = device;

    console.log(`[METADATA_DEBUG] connectToDevice - getting remote DB server port...`);
    const dbPort = await getRemoteDBServerPort(ip);
    console.log(`[METADATA_DEBUG] connectToDevice - dbPort=${dbPort}`);

    const socket = new PromiseSocket(new Socket());
    console.log(`[METADATA_DEBUG] connectToDevice - connecting to socket...`);
    await socket.connect(dbPort, ip.address);
    console.log(`[METADATA_DEBUG] connectToDevice - socket connected`);

    // Send required preamble to open communications with the device
    const preamble = new UInt32(0x01);
    console.log(`[METADATA_DEBUG] connectToDevice - writing preamble...`);
    await socket.write(preamble.buffer);
    console.log(`[METADATA_DEBUG] connectToDevice - reading preamble response...`);

    // Read the response. It should be a UInt32 field with the value 0x01.
    // There is some kind of problem if not.
    const data = await readField(socket, UInt32.type);

    if (data.value !== 0x01) {
      throw new Error(`Expected 0x01 during preamble handshake. Got ${data.value}`);
    }
    console.log(`[METADATA_DEBUG] connectToDevice - preamble handshake OK`);

    // Send introduction message to set context for querying
    const intro = new Message({
      transactionId: 0xfffffffe,
      type: MessageType.Introduce,
      args: [new UInt32(this.#hostDevice.id)],
    });

    console.log(`[METADATA_DEBUG] connectToDevice - writing intro message...`);
    await socket.write(intro.buffer);
    console.log(`[METADATA_DEBUG] connectToDevice - reading intro response...`);
    const resp = await Message.fromStream(socket, MessageType.Success, tx);

    if (resp.type !== MessageType.Success) {
      throw new Error(`Failed to introduce self to device ID: ${device.id}`);
    }

    this.#connections.set(device.id, new Connection(device, socket));
    console.log(`[METADATA_DEBUG] connectToDevice END - connected to device ${device.id}`);
    tx.finish();
  };

  /**
   * Disconnect from the specified device
   */
  disconnectFromDevice = async (device: Device) => {
    const tx = Sentry.startTransaction({name: 'disconnectFromDevice', data: {device}});

    const conn = this.#connections.get(device.id);

    if (conn === undefined) {
      return;
    }

    const goodbye = new Message({
      transactionId: 0xfffffffe,
      type: MessageType.Disconnect,
      args: [],
    });

    await conn.writeMessage(goodbye, tx);

    conn.close();
    this.#connections.delete(device.id);
    tx.finish();
  };

  /**
   * Remove a connection for a device without sending disconnect message.
   * Used when a connection is in a bad state (e.g., timeout).
   */
  removeConnection(deviceId: DeviceID) {
    const conn = this.#connections.get(deviceId);
    if (conn) {
      try {
        conn.close();
      } catch {
        // Ignore errors when closing dead connection
      }
      this.#connections.delete(deviceId);
    }
  }

  /**
   * Gets the remote database query interface for the given device.
   *
   * If we have not already established a connection with the specified device,
   * we will attempt to first connect.
   *
   * @returns null if the device does not export a remote database service
   */
  async get(deviceId: DeviceID) {
    console.log(`[METADATA_DEBUG] RemoteDatabase.get START - deviceId=${deviceId}`);
    const device = this.#deviceManager.devices.get(deviceId);
    if (device === undefined) {
      console.log(`[METADATA_DEBUG] RemoteDatabase.get - device not found, returning null`);
      return null;
    }

    const lock =
      this.#deviceLocks.get(device.id) ??
      this.#deviceLocks.set(device.id, new Mutex()).get(device.id)!;

    console.log(`[METADATA_DEBUG] RemoteDatabase.get - acquiring device lock...`);
    const releaseLock = await lock.acquire();
    console.log(`[METADATA_DEBUG] RemoteDatabase.get - device lock acquired`);

    try {
      let conn = this.#connections.get(deviceId);
      if (conn === undefined) {
        console.log(`[METADATA_DEBUG] RemoteDatabase.get - no existing connection, connecting...`);
        await this.connectToDevice(device);
      } else {
        console.log(`[METADATA_DEBUG] RemoteDatabase.get - using existing connection`);
      }

      conn = this.#connections.get(deviceId)!;

      // NOTE: We pass the same lock we use for this device to the query
      // interface to ensure all query interfaces use the same lock.

      console.log(`[METADATA_DEBUG] RemoteDatabase.get END - returning QueryInterface`);
      return new QueryInterface(conn, lock, this.#hostDevice);
    } finally {
      console.log(`[METADATA_DEBUG] RemoteDatabase.get - releasing device lock`);
      releaseLock();
    }
  }
}
