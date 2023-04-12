import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';
import { ApiHttpService } from '../services/api-http-service';
@Component({
  selector: 'app-log-content',
  templateUrl: './log-content.component.html',
  styleUrls: ['./log-content.component.css']
})
export class LogContentComponent implements OnInit {
  public logContent: string | null;
  public logFileName: string | null;
  constructor(private router: ActivatedRoute, private apiHttpService: ApiHttpService) { 
    this.logContent = null;
    this.logFileName = null;
  }

  ngOnInit() {
    this.logFileName = this.router.snapshot.queryParamMap.get('fileName') as string;
    this.apiHttpService.getLogContent(this.logFileName)
      .subscribe((data: any) => {
        this.logContent = data
        console.log(data);
      });
    }

}
