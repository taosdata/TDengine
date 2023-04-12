// Angular Modules 
import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { environment } from 'src/environments/environment';
@Injectable()
export class ApiHttpService {

    constructor(private http: HttpClient) { }

    public getMonitoringInfo() {
        return this.http.get(environment.webApiUrl + "api/monitoring");
    }
    public getLogFiles() {
        return this.http.get(environment.webApiUrl + "api/logs");
    }
    public getLogContent(fileName: string) {
        return this.http.get(environment.webApiUrl + "api/logs/" + fileName);
    }

    public getExceptions() {
        return this.http.get(environment.webApiUrl + "api/exceptions");
    }
}