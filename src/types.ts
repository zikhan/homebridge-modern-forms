export declare type ResponsePayload = {
  fanOn: boolean,
  fanSpeed: number,
  fanDirection: 'forward' | 'reverse',
  lightOn: boolean,
  lightBrightness: number,
  clientId: string,
  wind: boolean,
  windSpeed: number
}

export declare type StaticResponsePayload = {
  clientId: string,
  fanType?: string,
  lightType?: string,
  deviceName?: string
}

export interface FanConfig {
  ip: string
  light?: boolean
  switch?: string
  name?: string
  model?: string
}

export interface DeviceContext extends FanConfig {
  uuid: string,
  clientId: string
}

export type RequestPayload = Omit<Partial<ResponsePayload>, 'clientId'>
