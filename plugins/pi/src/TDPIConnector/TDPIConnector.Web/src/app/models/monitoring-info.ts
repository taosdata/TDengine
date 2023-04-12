import { PIEventReceived } from "./pi-event-received";
import { AFEventReceived } from "./af-event-received";
import { EventsPerPoint } from "./events-per-point";
import { EventsPerAttribute } from "./events-per-attribute";
import { ResponsesPerCode } from "./responses-per-code";
import { PIConnection } from "./pi-connection";
import { TDEngineHttpResponseSummary } from "./td-engine-http-response-summary";
import { TDEngineInfo } from "./td-engine-info";

export interface MonitoringInfo 
{
    piConnection: PIConnection
    eventsPerPoint: EventsPerPoint[] 
    eventsPerAttribute: EventsPerAttribute[] 
    tdEngineHttpResponses:  TDEngineHttpResponseSummary[]
    responsesPerCode: ResponsesPerCode[]
    tdEngineInfo: TDEngineInfo
    lastPIEvents: PIEventReceived[] 
    lastAFEvents: AFEventReceived[] 
}