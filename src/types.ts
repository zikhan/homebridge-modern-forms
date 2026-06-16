
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
  deviceName: string
}

export type RequestPayload = Omit<Partial<ResponsePayload>, 'clientId'>
