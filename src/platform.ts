import { API, Characteristic, DynamicPlatformPlugin, Logger, PlatformAccessory, PlatformConfig, Service, UnknownContext} from 'homebridge';
import { of, partition, from, fromEventPattern, merge } from 'rxjs';
import { tap, mergeMap, filter, share, map, distinct } from 'rxjs/operators';

import { PLATFORM_NAME, PLUGIN_NAME } from './settings';
import { ModernFormsPlatformAccessory } from './platformAccessory';
import {ResponsePayload, StaticResponsePayload, FanConfig, DeviceContext} from './types';
import axios from 'axios';
import mqtt, {MqttClient} from 'mqtt';
import Bonjour, {Service as mdnsService} from 'bonjour-service';


interface Config extends PlatformConfig {
  pollingInterval?: number
  autoDiscover?: boolean
  fans?: Array<FanConfig>
}

export class ModernFormsPlatform implements DynamicPlatformPlugin {
  public readonly Service: typeof Service = this.api.hap.Service;
  public readonly Characteristic: typeof Characteristic = this.api.hap.Characteristic;
  public readonly accessories: PlatformAccessory<DeviceContext>[] = [];
  public readonly mqtt!: MqttClient;
  public readonly bonjour: Bonjour = new Bonjour({}, () => this.log.error('bonjour error'));

  constructor(
    public readonly log: Logger,
    public readonly config: Config,
    public readonly api: API,
  ) {
    this.log.debug('Finished initializing platform:', this.config.name);

    if (this.config.mqttUrl) {
      this.mqtt = mqtt.connect(this.config.mqttUrl);
    }

    this.api.on('didFinishLaunching', () => {
      this.log.debug('Executed didFinishLaunching callback');
      this.discoverDevices();
    });
  }

  async ping(ip: string) {
    await axios
      .post<ResponsePayload>(`http://${ip}/mf`, {queryDynamicShadowData: 1});
  }

  async staticData(ip: string) {
    this.log.debug(`Fetching static data for ${ip}`);
    const resp = await axios
      .post<StaticResponsePayload>(`http://${ip}/mf`, {queryStaticShadowData: 1});
    return resp.data;
  }

  configureAccessory(accessory: PlatformAccessory) {
    this.log.info('Loading accessory from cache:', accessory.displayName);
    this.accessories.push(accessory as PlatformAccessory<DeviceContext>);
  }

  async discoverDevices() {
    this.log.info('Looking for Modern Forms devices on network');

    const cachedFansAddresses$ = from(this.accessories ?? []).pipe(
      map(accessory => {
        if ((accessory.context as UnknownContext).device) {
          return <FanConfig>(accessory.context as UnknownContext).device;
        }
        return accessory.context;
      }),
      tap(fan => this.log.debug('Found potential IP address from cached devices:', fan.ip)),
    );

    const configFansAddresses$ = from(this.config.fans ?? []).pipe(
      filter((fan) => !!fan.ip),
      tap(fan => this.log.debug('Found potential IP address from config:', fan.ip)),
    );

    // const getActiveInterface = bindNodeCallback(network.get_active_interface);
    // const getMAC = bindNodeCallback(arp.getMAC.bind(arp));

    // const networkFansAddresses$ = of(this.config.autoDiscover).pipe(
    //   mergeMap(autoDiscover => autoDiscover === false ? EMPTY : getActiveInterface()),
    //   tap(() => this.log.debug('Searching network for Modern Forms fans')),
    //   map(int => calculateNetwork(int.ip_address ?? '192.168.0.1', int.netmask ?? '255.255.255.0')),
    //   map(network => network.network + '/' + network.bitmask),
    //   mergeMap(subnet => getIpRange(subnet)),
    //   mergeMap(ip => ping.promise.probe(ip).then(() => ip)),
    //   mergeMap(ip => getMAC(ip).pipe(
    //     map(mac => mac?.toUpperCase() ?? ''),
    //     filter(mac => mac.startsWith('C8:93:46')),
    //     mapTo({ ip: ip, light: true }),
    //   )),
    //   tap(fan => this.log.debug('Found potential IP address from network and filtering by MAC vendor:', fan.ip)),
    // );

    // const filterDiscover = DnsSd.discover({
    //   name: '_easylink._tcp.local',
    //   filter: (device) => /(MF|WAC)_.+(?=\._easylink._tcp.local)_/.test(device.fqdn),
    //   wait: 30,
    // });

    // const networkDNSSD$ = from(filterDiscover).pipe(
    //   mergeMap((devices) => from(devices)),
    //   map((device) => <FanConfig>{ip: device.address, light: true}),
    //   tap(fan => this.log.debug('Found potential IP address from dns-sd', fan.ip)),
    // );

    const find$ = fromEventPattern<mdnsService>(
      (hander) => this.bonjour.find({ type: 'easylink', txt: {} }, hander),
      (handler, browser) => browser.removeListener('up', handler),
    );

    const networkDNSSD$ = find$.pipe(
      map((service) => <FanConfig>{ip: service.host, light: true, name: service.name}),
      tap(fan => this.log.debug('Found potential IP address from dns-sd', fan.ip)),
    );

    const devices$ = merge(cachedFansAddresses$, configFansAddresses$, networkDNSSD$).pipe(
      distinct((fan) => fan.ip),
      mergeMap(fan => of(fan).pipe(
        mergeMap(fan => this.staticData(fan.ip).then(res => {
          fan.name = res.deviceName;
          fan.light = !!res.lightType;
          fan.model = res.fanType;
          return res.clientId;
        }).catch((err) => {
          this.log.error('Error getting static data', err); return null;
        })),
        filter((clientId): clientId is string => clientId !== null),
        tap(clientId => this.log.info(`Found device at ${fan.ip} with client ID of ${clientId}`)),
        map(clientId => {
          const uuid = this.api.hap.uuid.generate(clientId);
          const existingAccessory = this.accessories.find(accessory => accessory.UUID === uuid);
          return { fan, clientId, uuid, existingAccessory };
        }),
      )),
      share(),
    );

    const [newDevices$, existingDevices$] = partition(
      devices$,
      device => !device.existingAccessory,
    );

    newDevices$.subscribe(({ uuid, fan, clientId }) => {
      this.log.info('Adding new accessory:', clientId);
      const accessory = new this.api.platformAccessory<DeviceContext>(clientId, uuid);
      accessory.context = { uuid, ip: fan.ip, light: fan.light, switch: fan.switch, clientId, name: fan.name };
      new ModernFormsPlatformAccessory(this, accessory);
      this.api.registerPlatformAccessories(PLUGIN_NAME, PLATFORM_NAME, [accessory]);
    });

    existingDevices$.subscribe(({ uuid, fan, clientId, existingAccessory }) => {
      this.log.info('Restoring existing accessory from cache:', clientId);
      existingAccessory!.context = { uuid, ip: fan.ip, light: fan.light, switch: fan.switch, clientId, name: fan.name};
      new ModernFormsPlatformAccessory(this, existingAccessory!);
    });
  }
}
