import {Span} from '@sentry/tracing';

import LocalDatabase from 'src/localdb';
import {loadAnlz} from 'src/localdb/rekordbox';
import RemoteDatabase, {MenuTarget, Query} from 'src/remotedb';
import {Device, DeviceID, MediaSlot, TrackType} from 'src/types';

import {anlzLoader} from './utils';

export interface Options {
  /**
   * The device to query the track metadata from
   */
  deviceId: DeviceID;
  /**
   * The media slot the track is present in
   */
  trackSlot: MediaSlot;
  /**
   * The type of track we are querying for
   */
  trackType: TrackType;
  /**
   * The track id to retrieve metadata for
   */
  trackId: number;
  /**
   * The Sentry transaction span
   */
  span?: Span;
}

export async function viaRemote(remote: RemoteDatabase, opts: Required<Options>) {
  const {deviceId, trackSlot, trackType, trackId, span} = opts;

  console.log(`[METADATA_DEBUG] viaRemote START - deviceId=${deviceId}, trackId=${trackId}, trackType=${trackType}`);

  console.log(`[METADATA_DEBUG] viaRemote - getting remote connection...`);
  const conn = await remote.get(deviceId);
  if (conn === null) {
    console.log(`[METADATA_DEBUG] viaRemote - connection is null, returning null`);
    return null;
  }
  console.log(`[METADATA_DEBUG] viaRemote - got connection`);

  const queryDescriptor = {
    trackSlot,
    trackType,
    menuTarget: MenuTarget.Main,
  };

  // Use GetGenericMetadata for unanalyzed tracks, GetMetadata for Rekordbox-analyzed
  const metadataQuery =
    trackType === TrackType.Unanalyzed ? Query.GetGenericMetadata : Query.GetMetadata;

  console.log(`[METADATA_DEBUG] viaRemote - querying metadata (query=${metadataQuery})...`);
  const track = await conn.query({
    queryDescriptor,
    query: metadataQuery,
    args: {trackId},
    span,
  });
  console.log(`[METADATA_DEBUG] viaRemote - metadata query complete, got track: ${track?.title}`);

  console.log(`[METADATA_DEBUG] viaRemote - querying track info (file path)...`);
  track.filePath = await conn.query({
    queryDescriptor,
    query: Query.GetTrackInfo,
    args: {trackId},
    span,
  });
  console.log(`[METADATA_DEBUG] viaRemote - track info query complete, filePath=${track.filePath}`);

  console.log(`[METADATA_DEBUG] viaRemote - querying beat grid...`);
  track.beatGrid = await conn.query({
    queryDescriptor,
    query: Query.GetBeatGrid,
    args: {trackId},
    span,
  });
  console.log(`[METADATA_DEBUG] viaRemote - beat grid query complete`);

  console.log(`[METADATA_DEBUG] viaRemote END - returning track`);
  return track;
}

export async function viaLocal(
  local: LocalDatabase,
  device: Device,
  opts: Required<Options>
) {
  const {deviceId, trackSlot, trackId} = opts;

  if (trackSlot !== MediaSlot.USB && trackSlot !== MediaSlot.SD) {
    throw new Error('Expected USB or SD slot for local database query');
  }

  const orm = await local.get(deviceId, trackSlot);
  if (orm === null) {
    return null;
  }

  const track = orm.findTrack(trackId);

  if (track === null) {
    return null;
  }

  const anlz = await loadAnlz(track, 'DAT', anlzLoader({device, slot: trackSlot}));

  track.beatGrid = anlz.beatGrid;
  track.cueAndLoops = anlz.cueAndLoops;

  return track;
}
