import { Service, PlatformAccessory, CharacteristicValue, Characteristic, WithUUID } from 'homebridge';

import { ModernFormsPlatform } from './platform';
import { DeviceContext, RequestPayload, ResponsePayload } from './types';
import axios from 'axios';
import {
  iif, BehaviorSubject, map, distinctUntilChanged, debounceTime,
  from, merge, switchMap, buffer, skipWhile,
  firstValueFrom, tap, catchError, Subject,
  startWith, interval, throttleTime,
} from 'rxjs';

export class ModernFormsPlatformAccessory {

  private fanService: Service;
  private lightService?: Service;

  private states$ = {
    fanOn: new BehaviorSubject<boolean>(false),
    fanSpeed:new BehaviorSubject<number>(1),
    windSpeed: new BehaviorSubject<number>(1),
    wind: new BehaviorSubject<boolean>(false),
    fanDirection:new BehaviorSubject<'forward' | 'reverse'>('forward'),
    lightOn: new BehaviorSubject<boolean>(false),
    lightBrightness: new BehaviorSubject<number>(1),
  };

  private NUMBER_OF_FAN_SPEEDS = () => this.states$?.wind.getValue() ? 3 : 6;
  private isRemoteSync = true;
  private getRequested$ = new Subject<void>();

  private readonly pollingInterval = (this.platform.config.pollingInterval ?? 5) * 1000;

  constructor(
    private readonly platform: ModernFormsPlatform,
    private readonly accessory: PlatformAccessory<DeviceContext>,
  ) {
      this.accessory.getService(this.platform.Service.AccessoryInformation)!
        .setCharacteristic(this.platform.Characteristic.Manufacturer, 'Modern Forms')
        .setCharacteristic(this.platform.Characteristic.Model, this.device().model ?? 'Unknown')
        .setCharacteristic(this.platform.Characteristic.SerialNumber, this.device().clientId);

      // FAN SERVICE
      this.fanService =
        this.accessory.getService(this.platform.Service.Fanv2) ??
        this.accessory.addService(this.platform.Service.Fanv2);

      this.fanService.setCharacteristic(this.platform.Characteristic.Name, this.device().name ?? this.device().ip);
      this.fanService.getCharacteristic(this.platform.Characteristic.Active)
        .onGet(this.getFanOn.bind(this))
        .onSet(this.setFanOn.bind(this));
      this.fanService.getCharacteristic(this.platform.Characteristic.RotationSpeed)
        .onSet(this.setRotationSpeed.bind(this))
        .setProps({ minStep: 1, minValue: 1, unit: null });
      this.fanService.getCharacteristic(this.platform.Characteristic.RotationDirection)
        .onSet(this.setRotationDirection.bind(this));
      this.fanService.getCharacteristic(this.platform.Characteristic.SwingMode)
        .onGet(this.getSwingMode.bind(this))
        .onSet(this.setSwingMode.bind(this));

      // LIGHT SERVICE
      if (this.device().light) {
        this.lightService =
          this.accessory.getService(this.platform.Service.Lightbulb) ??
          this.accessory.addService(this.platform.Service.Lightbulb);

        this.lightService.setCharacteristic(this.platform.Characteristic.Name, this.device().name ?? this.device().ip);
        this.lightService.getCharacteristic(this.platform.Characteristic.On)
          .onGet(this.getLightOn.bind(this))
          .onSet(this.setLightOn.bind(this));
        this.lightService.getCharacteristic(this.platform.Characteristic.Brightness).onSet( this.setBrightness.bind(this));
      } else {
        const oldLightService = this.accessory.getService(this.platform.Service.Lightbulb);
        if (oldLightService ) {
          this.accessory.removeService(oldLightService);
        }
      }

      if (this.device().switch) {
        const mqttTopic = `stat/${this.device().switch}/RESULT`;
        this.log(`Subscribing to messages on topic ${mqttTopic}...`);
        this.platform.mqtt.subscribe(mqttTopic);
        this.platform.mqtt.on('message', (topic, message) => {
          this.log(`Got message from topic ${topic}`);
          if (topic === mqttTopic) {
            const data = JSON.parse(message.toString());
            this.log(`Message: ${JSON.stringify(data)}`);
            if (data.Button1?.Action === 'SINGLE') {
              this.log('Toggle fan on from MQTT');
              this.states$?.fanOn.next(!this.states$?.fanOn.getValue());
            }
          }
        });
      }

      // initialize
      this.ReactivePipeline();
  }


  device(): DeviceContext {
    return this.accessory.context;
  }

  // HELPERS
  logStateUpdate(characteristic: string, value: CharacteristicValue) {
    this.log(`Updating ${characteristic}`, value);
  }

  updateFanCharacteristic(characteristic: WithUUID<new () => Characteristic>, value: CharacteristicValue) {
    this.fanService.getCharacteristic(characteristic).updateValue(value);
  }

  updateLightCharacteristic(characteristic: WithUUID<new () => Characteristic>, value: CharacteristicValue) {
    this.lightService && this.lightService.getCharacteristic(characteristic).updateValue(value);
  }

  private ReactivePipeline(){

    // Update charateristics
    this.states$?.wind
      .subscribe(() => {
        this.updateFanCharacteristic.bind(this, this.platform.Characteristic.SwingMode);
        this.fanService.getCharacteristic(this.platform.Characteristic.RotationSpeed)
          .setProps({ maxValue: this.NUMBER_OF_FAN_SPEEDS() });
      });
    this.states$?.fanOn
      .subscribe((value) => {
        this.updateLed();
        this.updateFanCharacteristic.bind(value, this.platform.Characteristic.Active);
      });

    iif(() => this.states$?.wind.getValue() ?? false, this.states$.windSpeed, this.states$.fanSpeed)
      .subscribe(this.updateFanCharacteristic.bind(this, this.platform.Characteristic.RotationSpeed));

    this.states$.fanDirection
      .pipe(
        map(dir => dir === 'forward'
          ? this.platform.Characteristic.RotationDirection.CLOCKWISE
          : this.platform.Characteristic.RotationDirection.COUNTER_CLOCKWISE,
        ),
      )
      .subscribe(this.updateFanCharacteristic.bind(this, this.platform.Characteristic.RotationDirection));
    if (this.lightService) {
      this.states$.lightOn
        .subscribe(this.updateLightCharacteristic.bind(this, this.platform.Characteristic.On));

      this.states$.lightBrightness
        .subscribe(this.updateLightCharacteristic.bind(this, this.platform.Characteristic.Brightness));
    }

    // On any observable update, with distinctive changes
    const distinctStateChanges$ = merge(...Object.entries(this.states$).map(([key, sub$]) =>
      from(sub$).pipe(
        distinctUntilChanged(),
        map(val => ({ key, val })),
      ),
    ));

    // build a batched request payload to send all at once from multiple inputs
    const payload$ = distinctStateChanges$.pipe(
      skipWhile(() => this.isRemoteSync),
      tap(({ key, val }) => this.logStateUpdate(key, val)),
      buffer(distinctStateChanges$.pipe(debounceTime(250))),
      map(batch => batch.reduce((acc, { key, val }) => {
        (acc as Record<string, unknown>)[key] = val;
        return acc;
      }, {} as RequestPayload)),
    );

    // Send payload to the device, update the states with response
    const apiUpdates$ = payload$.pipe(
      switchMap(payload => 
        Object.keys(payload).length === 0
          ? from(Promise.resolve(null))
          : from(this.sendDeviceState(payload)),
      ),
      tap(apiResponse => {
        if (apiResponse) {
          this.updateStatesFromRemote(apiResponse);
        }
      }),
      catchError(err => {
        // log error, but don't break the stream
        this.platform.log.error('uncaught error in pipeline', err);
        return from(Promise.resolve(null));
      }),
    );

    apiUpdates$.subscribe();

    // 4. polling for current fan status
    const poll$ = merge(interval(this.pollingInterval), this.getRequested$.pipe(throttleTime(1000))).pipe(
      debounceTime(500),
      startWith(null),
      switchMap(() => from(this.fetchCurrentStateFromDevice())),
      tap(apiState => {
        if (apiState) {
          this.updateStatesFromRemote(apiState);
        }
      }),
      catchError(err => {
        this.platform.log.error('Uncaught Error polling device:', err);
        return from(Promise.resolve(null));
      }),
    );

    poll$.subscribe();
  } 

  private async sendDeviceState(payload: RequestPayload){
    this.platform.log.info(`Sending state of ${this.device().clientId} to ${this.device().ip}.`);
    try{
      const resp = await axios
        .post<ResponsePayload>(`http://${this.device().ip}/mf`, payload);
      return resp.data;
    } catch (error : unknown) {
      this.platform.log.warn(`Failed to send state of ${this.device().clientId} to IP ${this.device().ip}:`, error);
    }
  }

  private async fetchCurrentStateFromDevice() : Promise<ResponsePayload | undefined> {
    this.platform.log.info(`Querying ${this.device().clientId}.`);
    try {
      const response = await axios
        .post<ResponsePayload>(`http://${this.device().ip}/mf`, { queryDynamicShadowData: 1 });
      return response.data;
    } catch(error) {
      this.platform.log.warn(`Failed to get status of ${this.device().clientId} at IP ${this.device().ip}: `, error);
    }
  }


  private updateStatesFromRemote(apiState: ResponsePayload) {
    this.platform.log.info(`Updating states from remote for ${this.device().clientId}: ${JSON.stringify(apiState)}`);
    this.isRemoteSync = true;

    if (apiState.fanOn !== undefined) {
      this.states$.fanOn.next(apiState.fanOn);
    }
    if (apiState.fanSpeed !== undefined) {
      this.states$.fanSpeed.next(apiState.fanSpeed);
    }
    if (apiState.fanDirection !== undefined) {
      this.states$.fanDirection.next(apiState.fanDirection);
    }
    if (apiState.lightOn !== undefined) {
      this.states$.lightOn.next(apiState.lightOn);
    }
    if (apiState.lightBrightness !== undefined) {
      this.states$.lightBrightness.next(apiState.lightBrightness);
    }
    if (apiState.wind !== undefined) {
      this.states$.wind.next(apiState.wind);
    }
    if (apiState.windSpeed !== undefined) {
      this.states$.windSpeed.next(apiState.windSpeed);
    }

    this.isRemoteSync = false;
  }

  updateLed() {
    if (this.device().switch) {
      const mqttTopic = `cmnd/${this.device().switch}/LedPower`;
      this.platform.mqtt.publish(mqttTopic, this.states$.fanOn.getValue() ? 'ON' : 'OFF');
    }
  }

  getStepWithoutGoingOver = (steps: number) => {
    return Math.floor(100 / steps * 1000) / 1000;
  }

  log = (...args: unknown[]) => {
    this.platform.log.info(`[${this.device().ip}]`, ...args);
  }

  // FAN GETTERS / SETTERS

  getFanOn() {
    this.log('Get Fan Characteristic On');
    this.getRequested$.next();
    return firstValueFrom(this.states$.fanOn);
  }

  setFanOn(value: CharacteristicValue) : Promise<void> {
    this.states$.fanOn.next(Boolean(value));
    return Promise.resolve();
  }

  setRotationDirection(value: CharacteristicValue) {
    this.states$.fanDirection.next(value === 0 ? 'forward' : 'reverse');
    return Promise.resolve();
  }

  setRotationSpeed(value: CharacteristicValue) {
    this.states$.fanOn.next((value as number) > 0);
    if (this.states$.wind.getValue()) {
      this.states$.windSpeed.next(value as number);
    } else {
      this.states$.fanSpeed.next(value as number); 
    }
    return Promise.resolve();
  }

  getSwingMode() {
    this.log('Get Fan Characteristic swing mode');
    this.getRequested$.next();
    return firstValueFrom(this.states$.wind);
  }

  setSwingMode(value: CharacteristicValue) {
    this.states$.wind.next(value === this.platform.Characteristic.SwingMode.SWING_ENABLED);
    return Promise.resolve();
  }

  // LIGHT GETTERS / SETTERS

  getLightOn() {
    this.log('Get light Characteristic On');
    this.getRequested$.next();
    return firstValueFrom(this.states$.lightOn);
  }

  setLightOn(value: CharacteristicValue) {
    this.states$.lightOn.next(Boolean(value));
    return Promise.resolve();
  }

  setBrightness(value: CharacteristicValue) {
    this.states$.lightOn.next((value as number) > 0);
    this.states$.lightBrightness.next(value as number);
    return Promise.resolve();
  }
}
