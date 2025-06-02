/*
Copyright (c) 2025 Licensed under the Open Software License version 3.0

OSL-3.0 <https://spdx.org/licenses/OSL-3.0.html>

Attribution Notice:
Skillet (discord: steelskillet)
The Unnamed (https://theunnamedcorp.com/)
*/

import BasePlugin from './base-plugin.js';
import { DataTypes } from 'sequelize';
import path from 'path';
import fs from 'fs';
import { readFile, writeFile } from 'node:fs/promises';

export default class OfficialKothDB extends BasePlugin {
  static get description() {
    return 'Pushes ServerSettings.json from database on startup and syncs player data on join/leave; ServerSettings sync every 90 seconds only when player count is 50 or more';
  }

  static get defaultEnabled() {
    return false;
  }

  static get optionsSpecification() {
    return {
      kothFolderPath: {
        required: false,
        description:
          'Folder path (relative to squadjs index.js) of the koth data folder.',
        default: './SquadGame/Saved/KOTH/',
      },
      database: {
        required: true,
        description: 'Database to use',
        default: false,
        connector: 'sequelize',
      },
      sftp: {
        required: false,
        description: 'SFTP Credentials.',
        default: false,
      },

      syncEnabled: {
        required: false,
        description: 'Whether periodic sync of ServerSettings.json is enabled.',
        default: true,
      },
    };
  }

  constructor(server, options, connectors) {
    super(server, options, connectors);

    //? Setup SFTP Client later
    this.isSFTP = Boolean(options.sftp);
    this.sftpOptions = options.sftp;

    this.models = {};

    this.onPlayerConnected = this.onPlayerConnected.bind(this);
    this.onPlayerDisconnected = this.onPlayerDisconnected.bind(this);
  }

  createModel(name, schema) {
    this.models[name] = this.options.database.define(`KOTH_${name}`, schema, {
      timestamps: false,
    });
    return this.models[name];
  }

  async prepareToMount() {
    const playeridmeta = {
      type: DataTypes.STRING,
      unique: true,
    };
    await this.createModel('PlayerData', {
      id: {
        type: DataTypes.INTEGER,
        primaryKey: true,
        autoIncrement: true,
      },
      player_id: playeridmeta,
      lastsave: {
        type: DataTypes.DATE,
      },
      serversave: {
        type: DataTypes.INTEGER,
      },
      playerdata: {
        type: DataTypes.JSON,
      },
    });

    try {
      await this.models.PlayerData.sync();
      this.verbose(1, 'OfficialKothDB: PlayerData table initialized');
    } catch (err) {
      this.verbose(
        1,
        `OfficialKothDB: Failed to sync PlayerData table: ${err.message}`
      );
      throw err;
    }
    if (this.isSFTP) {
      try {
        const Client = (await import('ssh2-sftp-client')).default;
        this.sftpClient = new Client();
        await this.sftpClient.connect(this.sftpOptions);
        this.verbose(1, 'OfficialKothDB: SFTP initialized');
      } catch (error) {
        this.verbose(
          1,
          `OfficialKothDB: Failed to connect to SFTP: ${error.message}`
        );
        throw error;
      }
    }
  }

  async readPlayerList() {
    const playerListPath = path.join(this.kothpath, 'PlayerList.json');
    this.verbose(
      1,
      `OfficialKothDB: Attempting to read PlayerList.json from ${playerListPath}`
    );

    if (!(await this.fileExists(playerListPath))) {
      this.verbose(
        1,
        `OfficialKothDB: PlayerList.json does not exist at ${playerListPath}`
      );
      return [];
    }

    try {
      const data = await this.fileRead(playerListPath, 'utf8');
      if (!data) {
        this.verbose(
          1,
          `OfficialKothDB: PlayerList.json is empty at ${playerListPath}`
        );
        return [];
      }

      const jsonData = JSON.parse(data);
      const steamIDs = jsonData.players || [];
      if (!Array.isArray(steamIDs)) {
        this.verbose(
          1,
          `OfficialKothDB: Invalid format in PlayerList.json: 'players' is not an array`
        );
        return [];
      }
      this.verbose(
        1,
        `OfficialKothDB: Successfully read ${steamIDs.length} steamIDs from PlayerList.json`
      );
      return steamIDs;
    } catch (err) {
      this.verbose(
        1,
        `OfficialKothDB: Error reading or parsing PlayerList.json at ${playerListPath}: ${err.message}`
      );
      return [];
    }
  }

  async syncServerSettings() {
    try {
      const serverSettings = await this.models.PlayerData.findOne({
        where: { player_id: 'ServerSettings' },
      });
      if (serverSettings) {
        const serverSettingsPath = path.join(
          this.kothpath,
          'ServerSettings.json'
        );
        const serverData =
          typeof serverSettings.playerdata === 'string'
            ? JSON.parse(serverSettings.playerdata)
            : serverSettings.playerdata;
        await this.fileWrite(
          serverSettingsPath,
          JSON.stringify(serverData, null, 2)
        );
        this.verbose(
          1,
          'OfficialKothDB: Pushed ServerSettings.json from database'
        );
      } else {
        this.verbose(
          1,
          'OfficialKothDB: No ServerSettings record found in database, skipping push'
        );
      }
    } catch (err) {
      this.verbose(
        1,
        `OfficialKothDB: Error syncing ServerSettings: ${err.message}`
      );
    }
  }

  async syncKothFiles() {
    try {
      const connectedSteamIDs = await this.readPlayerList();
      if (connectedSteamIDs.length >= 50 && this.options.syncEnabled) {
        await this.syncServerSettings();
      } else {
        this.verbose(
          1,
          `OfficialKothDB: Player count (${connectedSteamIDs.length}) below 50 or sync disabled, skipping ServerSettings sync`
        );
      }

      if (connectedSteamIDs.length === 0) {
        this.verbose(
          1,
          'OfficialKothDB: No connected players found in PlayerList.json'
        );
      }

      for (const steamID of connectedSteamIDs) {
        const playerFilePath = path.join(this.kothpath, `${steamID}.json`);
        this.verbose(
          1,
          `OfficialKothDB: Checking player file at ${playerFilePath}`
        );

        if (await this.fileExists(playerFilePath)) {
          try {
            const playerDataRaw = await this.fileRead(playerFilePath, 'utf8');
            const playerData = JSON.parse(playerDataRaw);

            await this.models.PlayerData.upsert(
              {
                player_id: steamID,
                lastsave: new Date(),
                serversave: this.server.id,
                playerdata: playerData,
              },
              {
                conflictFields: ['player_id'],
              }
            );
            this.verbose(
              1,
              `OfficialKothDB: Synced player data for ${steamID} to DB`
            );
          } catch (err) {
            this.verbose(
              1,
              `OfficialKothDB: Error syncing player ${steamID}: ${err.message}`
            );
          }
        } else {
          this.verbose(
            1,
            `OfficialKothDB: Player file for ${steamID} does not exist at ${playerFilePath}`
          );
        }
      }

      this.verbose(1, 'OfficialKothDB: Periodic sync completed');
    } catch (err) {
      this.verbose(1, `OfficialKothDB: Periodic sync error: ${err.message}`);
    }
  }

  async mount() {
    this.verbose(
      1,
      `OfficialKothDB: Current working directory: ${process.cwd()}`
    );
    this.verbose(
      1,
      `OfficialKothDB: Configured kothFolderPath: ${this.options.kothFolderPath}`
    );

    this.kothpath = path.isAbsolute(this.options.kothFolderPath)
      ? this.options.kothFolderPath
      : path.resolve(process.cwd(), this.options.kothFolderPath);

    this.verbose(1, `OfficialKothDB: Resolved KOTH path: ${this.kothpath}`);

    if (!(await this.fileExists(this.kothpath))) {
      try {
        await this.directoryWrite(this.kothpath, { recursive: true });
        this.verbose(
          1,
          `OfficialKothDB: Created KOTH directory at ${this.kothpath}`
        );
      } catch (err) {
        this.verbose(
          1,
          `OfficialKothDB: Failed to create KOTH directory at ${this.kothpath}: ${err.message}`
        );
        this.verbose(
          1,
          `OfficialKothDB: KOTH DATA PATH "${this.kothpath}" DOES NOT EXIST. Plugin shall remain dormant!`
        );
        return;
      }
    }

    const playerListPath = path.join(this.kothpath, 'PlayerList.json');
    if (!(await this.fileExists(playerListPath))) {
      this.verbose(
        1,
        `OfficialKothDB: PlayerList.json not found at "${playerListPath}". Plugin will not sync player data.`
      );
    }

    await this.syncServerSettings(); // Pull ServerSettings on startup
    await this.syncKothFiles();

    if (this.options.syncEnabled) {
      const intervalMs = 90000;
      this.syncInterval = setInterval(() => this.syncKothFiles(), intervalMs);
      this.verbose(1, `OfficialKothDB: Started periodic sync every 90 seconds`);
    } else {
      this.verbose(1, 'OfficialKothDB: Periodic sync disabled');
    }

    this.server.on('PLAYER_CONNECTED', this.onPlayerConnected);
    this.server.on('PLAYER_DISCONNECTED', this.onPlayerDisconnected);

    this.verbose(1, `OfficialKothDB: Created hooks`);
  }

  async unmount() {
    this.server.removeEventListener('PLAYER_CONNECTED', this.onPlayerConnected);
    this.server.removeEventListener(
      'PLAYER_DISCONNECTED',
      this.onPlayerDisconnected
    );
    if (this.syncInterval) {
      clearInterval(this.syncInterval);
      this.verbose(1, 'OfficialKothDB: Stopped periodic sync');
    }
  }

  getplayerfilename(player) {
    return path.join(this.kothpath, `${player.steamID}.json`);
  }

  async getplayerids(player) {
    return { id: player.steamID };
  }

  async onPlayerConnected(info) {
    this.verbose(1, `OfficialKothDB: koth load`);
    const playerfilename = this.getplayerfilename(info.player);
    this.verbose(
      1,
      `OfficialKothDB: attempting to overwrite local data at ${playerfilename}`
    );

    const playerids = await this.getplayerids(info.player);
    this.verbose(1, `OfficialKothDB: retrieved player id of: ${playerids.id}`);
    if (!playerids) return;
    const playerdb = await this.models.PlayerData.findOne({
      where: { player_id: playerids.id },
    });
    if (!playerdb) return;
    this.verbose(
      1,
      'OfficialKothDB: found playerdata in DB and read into memory'
    );
    const playerdata =
      typeof playerdb.playerdata === 'string'
        ? JSON.parse(playerdb.playerdata)
        : playerdb.playerdata;
    await this.fileWrite(playerfilename, JSON.stringify(playerdata, null, 2));
    this.verbose(1, 'OfficialKothDB: saved file');
  }

  async onPlayerDisconnected(info) {
    this.verbose(1, `OfficialKothDB: koth save`);
    const playerfilename = this.getplayerfilename(info.player);
    this.verbose(
      1,
      `OfficialKothDB: attempting to save file ${playerfilename} to db`
    );
    if (!(await this.fileExists(playerfilename))) return;
    const playerdataRaw = await this.fileRead(playerfilename, 'utf8');
    let playerdata;
    try {
      playerdata = JSON.parse(playerdataRaw);
    } catch (err) {
      this.verbose(
        1,
        `OfficialKothDB: Invalid JSON in file ${playerfilename}, skipping: ${err.message}`
      );
      return;
    }
    this.verbose(1, `OfficialKothDB: read player data into memory`);

    const playerids = await this.getplayerids(info.player);
    this.verbose(1, `OfficialKothDB: retrieved player id of: ${playerids.id}`);
    if (!playerids) return;
    await this.models.PlayerData.upsert(
      {
        player_id: playerids.id,
        lastsave: new Date(),
        serversave: this.server.id,
        playerdata: playerdata,
      },
      {
        conflictFields: ['player_id'],
      }
    );
    this.verbose(1, `OfficialKothDB: saved data to DB`);
  }

  
  async fileExists(filePath) {
    if (this.isSFTP) {
      return this.sftpClient.exists(filePath);
    } else {
      return fs.existsSync(filePath);
    }
  }
  async fileRead(filePath, encoding = 'utf8') {
    if (this.isSFTP) {
      return (await this.sftpClient.get(filePath)).toString();
    } else {
      return readFile(filePath, encoding);
    }
  }
  async fileWrite(filePath, content) {
    if (this.isSFTP) {
      return this.sftpClient.put(content, filePath);
    } else {
      return writeFile(filePath, content);
    }
  }
  async directoryWrite(directoryPath, options = { recursive: true }) {
    if (this.isSFTP) {
      return this.sftpClient.mkdir(directoryPath, options?.recursive);
    } else {
      return fs.mkdirSync(directoryPath, options);
    }
  }
}
