import { Component, OnInit } from '@angular/core';
import { MonitoringInfo } from '../models/monitoring-info';
import { ApiHttpService } from '../services/api-http-service';

@Component({
  selector: 'app-monitoring',
  templateUrl: './monitoring.component.html',
  styleUrls: ['./monitoring.component.css']
})
export class MonitoringComponent implements OnInit {
  public monitoringData: MonitoringInfo | null;

  constructor(private apiHttpService: ApiHttpService) {
    this.monitoringData = null; 
  }

  ngOnInit(): void {
    this.apiHttpService.getMonitoringInfo()
    .subscribe((data: any) => {
      this.monitoringData = data
      this.monitoringData?.eventsPerPoint
    });
  }

}
