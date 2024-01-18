//@ts-check
import { testAdapter } from '@kinshipjs/adapter-tests';
import { adapter, createMySql2Pool } from '../src/index.js';

const pool = createMySql2Pool({
    database: "chinook_ks_test",
    host: "192.168.1.28",
    user: "root",
    password: "root",
    port: 10500
});

const connection = adapter(pool);

await testAdapter(connection, {
    albumsTableName: "Album",
    genresTableName: "Genre",
    playlistTracksTableName: "PlaylistTrack",
    playlistsTableName: "Playlist",
    tracksTableName: "Track",
    precision: 4
});

process.exit(1);