import {Mutex} from 'async-mutex';
import promiseRetry from 'promise-retry';
import {timeout, TimeoutError} from 'promise-timeout';
import {OperationOptions} from 'retry';

import dgram, {Socket} from 'dgram';

import {udpClose, udpRead, udpSend} from 'src/utils/udp';

import {rpc} from './xdr';

/**
 * The RPC auth stamp passed by the CDJs. It's unclear if this is actually
 * important, but I'm keeping the rpc calls as close to CDJ calls as I can.
 */
const CDJ_AUTH_STAMP = 0x967b8703;

const rpcAuthMessage = new rpc.UnixAuth({
  stamp: CDJ_AUTH_STAMP,
  name: '',
  uid: 0,
  gid: 0,
  gids: [],
});

interface RpcCall {
  port: number;
  program: number;
  version: number;
  procedure: number;
  data: Buffer;
}

/**
 * Configuration for the retry strategy to use when making RPC calls
 *
 * @see https://www.npmjs.com/package/promise-retry#promiseretryfn-options
 */
export type RetryConfig = OperationOptions & {
  /**
   * Time in milliseconds to wait before a RPC transaction should timeout.
   * @default 1000
   */
  transactionTimeout?: number;
};

/**
 * Generic RPC connection. Can be used to make RPC 2 calls to any program
 * specified in the RpcCall.
 */
export class RpcConnection {
  address: string;
  retryConfig: RetryConfig;
  socket: Socket;
  mutex: Mutex;
  xid = 1;

  constructor(address: string, retryConfig?: RetryConfig) {
    this.address = address;
    this.retryConfig = retryConfig ?? {};
    this.socket = dgram.createSocket('udp4');
    this.mutex = new Mutex();
  }

  // TODO: Turn this into a getter and figure out what logic we can do here
  // to determine if the socket is still open.
  connected = true;

  setupRequest({program, version, procedure, data}: Omit<RpcCall, 'port'>) {
    const auth = new rpc.Auth({
      flavor: 1,
      body: rpcAuthMessage.toXDR(),
    });

    const verifier = new rpc.Auth({
      flavor: 0,
      body: Buffer.alloc(0),
    });

    const request = new rpc.Request({
      rpcVersion: rpc.Version,
      programVersion: version,
      program,
      procedure,
      auth,
      verifier,
      data,
    });

    const packet = new rpc.Packet({
      xid: this.xid,
      message: rpc.Message.request(request),
    });

    return packet.toXDR();
  }

  /**
   * Execute a RPC transaction (call and response).
   *
   * If a transaction does not complete after the configured timeout it will be
   * retried with the retry configuration.
   */
  async call({port, ...call}: RpcCall) {
    this.xid++;

    const callData = this.setupRequest(call);

    const {transactionTimeout, ...retryConfig} = this.retryConfig;

    // Function to execute the transaction, with timeout if the transaction
    // does not resolve after RESPONSE_RETRY_TIMEOUT. Cleans up the listener
    // on timeout to prevent memory leaks.
    const executeWithTimeout = async () => {
      await udpSend(this.socket, callData, 0, callData.length, port, this.address);
      const {promise, cancel} = udpRead(this.socket);
      try {
        return await timeout(promise, transactionTimeout ?? 1000);
      } catch (err) {
        cancel();
        throw err;
      }
    };

    // Function to execute the transaction, with retries if the transaction times out.
    let attemptCount = 0;
    const executeWithRetry = () =>
      promiseRetry(retryConfig, async (retry, attemptNumber) => {
        attemptCount = attemptNumber;
        try {
          return await executeWithTimeout();
        } catch (err) {
          if (err instanceof TimeoutError) {
            console.warn(
              `[RPC_DEBUG] Timeout on attempt ${attemptNumber} - ` +
                `address=${this.address}, port=${port}, ` +
                `program=${call.program}, procedure=${call.procedure}, ` +
                `timeout=${transactionTimeout ?? 1000}ms`
            );
            retry(err);
          } else {
            console.error(
              `[RPC_DEBUG] Non-timeout error - ` +
                `address=${this.address}, port=${port}, ` +
                `program=${call.program}, procedure=${call.procedure}, ` +
                `error=${err instanceof Error ? err.message : String(err)}`
            );
            throw err;
          }
        }
        return undefined;
      }).catch(err => {
        console.error(
          `[RPC_DEBUG] All retries exhausted after ${attemptCount} attempts - ` +
            `address=${this.address}, port=${port}, ` +
            `program=${call.program}, procedure=${call.procedure}, ` +
            `error=${err instanceof Error ? err.message : String(err)}`
        );
        throw err;
      });

    // Execute the transaction exclusively to avoid async call races
    const resp = await this.mutex.runExclusive(executeWithRetry);

    // Decode the XDR response
    const packet = rpc.Packet.fromXDR(resp);

    const message = packet.message().response();
    if (message.arm() !== 'accepted') {
      console.error(
        `[RPC_DEBUG] Request denied - ` +
          `address=${this.address}, port=${port}, ` +
          `program=${call.program}, procedure=${call.procedure}`
      );
      throw new Error('RPC request was denied');
    }

    const body = message.accepted().response();
    if (body.arm() !== 'success') {
      console.error(
        `[RPC_DEBUG] Request failed - ` +
          `address=${this.address}, port=${port}, ` +
          `program=${call.program}, procedure=${call.procedure}, ` +
          `response=${body.arm()}`
      );
      throw new Error('RPC did not successfully return data');
    }

    return body.success() as Buffer;
  }

  async disconnect() {
    await udpClose(this.socket);
  }
}

type RpcProgramCall = Pick<RpcCall, 'procedure' | 'data'>;

/**
 * RpcProgram is constructed with specialization details for a specific RPC
 * program. This should be used to avoid having to repeat yourself for calls
 * made using the RpcConnection.
 */
export class RpcProgram {
  program: number;
  version: number;
  port: number;
  conn: RpcConnection;

  constructor(conn: RpcConnection, program: number, version: number, port: number) {
    this.conn = conn;
    this.program = program;
    this.version = version;
    this.port = port;
  }

  call(data: RpcProgramCall) {
    const {program, version, port} = this;
    return this.conn.call({program, version, port, ...data});
  }

  disconnect() {
    this.conn.disconnect();
  }
}
