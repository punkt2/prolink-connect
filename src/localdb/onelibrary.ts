import * as Sentry from '@sentry/node';
import {Span} from '@sentry/tracing';
import {tmpdir} from 'os';
import {join} from 'path';
import {unlinkSync, writeFileSync} from 'fs';

import {
  Album,
  Artist,
  Artwork,
  Color,
  EntityFK,
  Genre,
  Key,
  Label,
  Playlist,
  PlaylistEntry,
  Track,
} from 'src/entities';
import {MetadataORM, Table} from 'src/localdb/orm';
import {HydrationProgress} from './rekordbox';

/**
 * SQLCipher encryption key for OneLibrary (Device Library Plus) databases.
 *
 * Derived from the pyrekordbox devicelib_plus BLOB constant using the same
 * deobfuscation method as the master.db key (Base85 decode, XOR, zlib).
 */
const SQLCIPHER_KEY =
  'r8gddnr4k847830ar6cqzbkk0el6qytmb3trbbx805jm74vez64i5o8fnrqryqls';

interface OneLibraryOptions {
  orm: MetadataORM;
  dbData: Buffer;
  span?: Span;
  onProgress?: (progress: HydrationProgress) => void;
}

// -- Promisified wrappers around @journeyapps/sqlcipher callback API -----------

function openDatabase(path: string, key: string): Promise<any> {
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const sqlcipher = require('@journeyapps/sqlcipher');
  return new Promise((resolve, reject) => {
    const db = new sqlcipher.Database(path, (err: Error | null) => {
      if (err) return reject(err);
      db.run(`PRAGMA key = '${key}'`, (pragmaErr: Error | null) => {
        if (pragmaErr) return reject(pragmaErr);
        resolve(db);
      });
    });
  });
}

function dbAll(db: any, sql: string): Promise<any[]> {
  return new Promise((resolve, reject) => {
    db.all(sql, (err: Error | null, rows: any[]) => {
      if (err) return reject(err);
      resolve(rows);
    });
  });
}

function dbClose(db: any): Promise<void> {
  return new Promise((resolve, reject) => {
    db.close((err: Error | null) => {
      if (err) return reject(err);
      resolve();
    });
  });
}

// -- Public API ---------------------------------------------------------------

/**
 * Hydrate a MetadataORM instance from a OneLibrary (Device Library Plus)
 * encrypted SQLite database.
 *
 * The dbData buffer is the raw `exportLibrary.db` file fetched via NFS. It
 * will be written to a temporary file so that sqlcipher can open it, then
 * cleaned up after hydration completes.
 */
export async function hydrateFromOneLibrary({dbData, span, ...options}: OneLibraryOptions) {
  console.log(
    `[METADATA_DEBUG] hydrateFromOneLibrary START - dbData size=${dbData.length}`
  );
  const hydrator = new OneLibraryHydrator(options);
  await hydrator.hydrate(dbData, span);
  console.log(`[METADATA_DEBUG] hydrateFromOneLibrary END`);
}

// -- Hydrator -----------------------------------------------------------------

class OneLibraryHydrator {
  #orm: MetadataORM;
  #onProgress: (progress: HydrationProgress) => void;

  constructor({orm, onProgress}: Omit<OneLibraryOptions, 'dbData' | 'span'>) {
    this.#orm = orm;
    this.#onProgress = onProgress ?? (() => null);
  }

  async hydrate(dbData: Buffer, span?: Span) {
    const tx = span
      ? span.startChild({op: 'hydrateFromOneLibrary'})
      : Sentry.startTransaction({name: 'hydrateFromOneLibrary'});

    // sqlcipher requires a file path – write the buffer to a temp file
    const tmpPath = join(tmpdir(), `onelibrary-${Date.now()}.db`);
    writeFileSync(tmpPath, dbData);
    console.log(
      `[METADATA_DEBUG] hydrateFromOneLibrary - wrote temp db to ${tmpPath}`
    );

    let db: any;
    try {
      db = await openDatabase(tmpPath, SQLCIPHER_KEY);
      console.log(`[METADATA_DEBUG] hydrateFromOneLibrary - database opened`);

      // Verify we can read (will fail if key is wrong)
      await dbAll(db, 'SELECT count(*) as n FROM content');
      console.log(`[METADATA_DEBUG] hydrateFromOneLibrary - key verified`);

      await this.#hydrateArtists(db, tx);
      await this.#hydrateAlbums(db, tx);
      await this.#hydrateGenres(db, tx);
      await this.#hydrateColors(db, tx);
      await this.#hydrateLabels(db, tx);
      await this.#hydrateKeys(db, tx);
      await this.#hydrateArtwork(db, tx);
      await this.#hydrateTracks(db, tx);
      await this.#hydratePlaylists(db, tx);
      await this.#hydratePlaylistEntries(db, tx);

      await dbClose(db);
    } catch (err) {
      try {
        if (db) await dbClose(db);
      } catch {
        /* ignore close errors */
      }
      throw err;
    } finally {
      try {
        unlinkSync(tmpPath);
      } catch {
        /* ignore cleanup errors */
      }
    }

    tx.finish();
  }

  // -- Per-table hydration helpers --------------------------------------------

  async #hydrateTable<T>(
    db: any,
    tableName: Table,
    query: string,
    mapRow: (row: any) => T,
    span: Span
  ) {
    const tx = span.startChild({op: 'hydrateTable', description: tableName});
    const rows = await dbAll(db, query);
    tx.setData('items', rows.length);
    console.log(
      `[METADATA_DEBUG] hydrateFromOneLibrary - ${tableName}: ${rows.length} rows`
    );

    let complete = 0;
    for (const row of rows) {
      const entity = mapRow(row);
      this.#orm.insertEntity(tableName, entity as any);
      this.#onProgress({complete: ++complete, table: tableName, total: rows.length});
      // Yield to event loop during large hydrations
      await new Promise(r => setTimeout(r, 0));
    }

    tx.finish();
  }

  async #hydrateArtists(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Artist,
      'SELECT artist_id, name FROM artist',
      row => ({id: row.artist_id, name: row.name ?? ''}) as Artist,
      span
    );
  }

  async #hydrateAlbums(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Album,
      'SELECT album_id, name FROM album',
      row => ({id: row.album_id, name: row.name ?? ''}) as Album,
      span
    );
  }

  async #hydrateGenres(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Genre,
      'SELECT genre_id, name FROM genre',
      row => ({id: row.genre_id, name: row.name ?? ''}) as Genre,
      span
    );
  }

  async #hydrateColors(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Color,
      'SELECT color_id, name FROM color',
      row => ({id: row.color_id, name: row.name ?? ''}) as Color,
      span
    );
  }

  async #hydrateLabels(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Label,
      'SELECT label_id, name FROM label',
      row => ({id: row.label_id, name: row.name ?? ''}) as Label,
      span
    );
  }

  async #hydrateKeys(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Key,
      // "key" is a SQL reserved word – must be quoted
      'SELECT key_id, name FROM "key"',
      row => ({id: row.key_id, name: row.name ?? ''}) as Key,
      span
    );
  }

  async #hydrateArtwork(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Artwork,
      'SELECT image_id, path FROM image',
      row => ({id: row.image_id, path: row.path ?? ''}) as Artwork,
      span
    );
  }

  async #hydrateTracks(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Track,
      `SELECT content_id, title, length, bpmx100, trackNo, discNo,
              samplingRate, bitDepth, bitrate, djPlayCount, releaseYear,
              rating, djComment, path, fileName, fileSize,
              releaseDate, dateAdded, dateCreated,
              isHotCueAutoLoadOn, isKuvoDeliverStatusOn,
              analysisDataFilePath, image_id,
              artist_id_artist, artist_id_originalArtist,
              artist_id_remixer, artist_id_composer,
              album_id, label_id, genre_id, color_id, key_id
       FROM content`,
      row => {
        const analyzePath: string | undefined = row.analysisDataFilePath;

        const track: Track<EntityFK.WithFKs> = {
          id: row.content_id,
          title: row.title ?? '',
          duration: row.length ?? 0,
          tempo: (row.bpmx100 ?? 0) / 100,
          trackNumber: row.trackNo ?? 0,
          discNumber: row.discNo ?? 0,
          sampleRate: row.samplingRate ?? 0,
          sampleDepth: row.bitDepth ?? 0,
          bitrate: row.bitrate ?? 0,
          playCount: row.djPlayCount ?? 0,
          year: row.releaseYear ?? 0,
          rating: row.rating ?? 0,
          comment: row.djComment ?? '',
          mixName: '',
          filePath: row.path ?? '',
          fileName: row.fileName ?? '',
          fileSize: row.fileSize ?? 0,
          releaseDate: row.releaseDate ?? '',
          analyzeDate: row.dateCreated ? new Date(row.dateCreated) : undefined,
          dateAdded: row.dateAdded ? new Date(row.dateAdded) : undefined,
          autoloadHotcues: row.isHotCueAutoLoadOn === 1,
          kuvoPublic: row.isKuvoDeliverStatusOn === 1,
          // Trim the .DAT extension so loadAnlz can append .DAT / .EXT
          analyzePath: analyzePath?.substring(0, analyzePath.length - 4),
          artworkId: row.image_id || null,
          artistId: row.artist_id_artist || null,
          originalArtistId: row.artist_id_originalArtist || null,
          remixerId: row.artist_id_remixer || null,
          composerId: row.artist_id_composer || null,
          albumId: row.album_id || null,
          labelId: row.label_id || null,
          genreId: row.genre_id || null,
          colorId: row.color_id || null,
          keyId: row.key_id || null,
          beatGrid: null,
          cueAndLoops: null,
          waveformHd: null,
        };
        return track;
      },
      span
    );
  }

  async #hydratePlaylists(db: any, span: Span) {
    await this.#hydrateTable(
      db,
      Table.Playlist,
      'SELECT playlist_id, name, attribute, playlist_id_parent FROM playlist',
      row =>
        ({
          id: row.playlist_id,
          name: row.name ?? '',
          isFolder: row.attribute !== 0,
          parentId: row.playlist_id_parent || null,
        }) as Playlist,
      span
    );
  }

  async #hydratePlaylistEntries(db: any, span: Span) {
    // playlist_content has no auto-increment id; generate sequential ids
    const rows = await dbAll(
      db,
      'SELECT playlist_id, content_id, sequenceNo FROM playlist_content'
    );
    const tx = span.startChild({
      op: 'hydrateTable',
      description: Table.PlaylistEntry,
    });
    tx.setData('items', rows.length);
    console.log(
      `[METADATA_DEBUG] hydrateFromOneLibrary - ${Table.PlaylistEntry}: ${rows.length} rows`
    );

    let complete = 0;
    for (const row of rows) {
      const entry: PlaylistEntry<EntityFK.WithFKs> = {
        id: ++complete,
        sortIndex: row.sequenceNo ?? 0,
        playlistId: row.playlist_id,
        trackId: row.content_id,
      };
      this.#orm.insertEntity(Table.PlaylistEntry, entry as any);
      this.#onProgress({
        complete,
        table: Table.PlaylistEntry,
        total: rows.length,
      });
      await new Promise(r => setTimeout(r, 0));
    }

    tx.finish();
  }
}
